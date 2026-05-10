package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"github.com/joho/godotenv"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Defaults — overridden by .env file.
var dockerBase = "http://10.0.0.1:2375"
var logFilePath = "./docker-observe.log"

var debugEnabled bool

// --- Logging ---

var fileLogger *log.Logger

func logLine(format string, args ...any) {
	if fileLogger != nil {
		fileLogger.Print(fmt.Sprintf(format, args...))
	}
}

func debugLog(format string, args ...any) {
	if debugEnabled && fileLogger != nil {
		fileLogger.Printf("DEBUG "+format, args...)
	}
}

// --- Docker API types ---

type DockerEvent struct {
	Type   string      `json:"Type"`
	Action string      `json:"Action"`
	Actor  DockerActor `json:"Actor"`
	Time   int64       `json:"time"`
}

type DockerActor struct {
	ID         string            `json:"ID"`
	Attributes map[string]string `json:"Attributes"`
}

type PortBinding struct {
	HostIP   string `json:"HostIp"`
	HostPort string `json:"HostPort"`
}

// ContainerInspect is a minimal slice of docker inspect output.
type ContainerInspect struct {
	ID   string `json:"Id"`
	Name string `json:"Name"`
	NetworkSettings struct {
		Ports map[string][]PortBinding `json:"Ports"`
	} `json:"NetworkSettings"`
}

// --- Container registry ---
//
// Holds port bindings and a cancel func (which tears down all forwarders)
// for each live container.

type ContainerEntry struct {
	ports  map[string][]PortBinding
	cancel context.CancelFunc
}

type ContainerRegistry struct {
	mu   sync.RWMutex
	data map[string]*ContainerEntry // containerID -> entry
}

func newRegistry() *ContainerRegistry {
	return &ContainerRegistry{data: make(map[string]*ContainerEntry)}
}

func (r *ContainerRegistry) add(id string, entry *ContainerEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[id] = entry
}

// remove cancels the container's forwarders and removes it from the registry.
// Returns the stored ports for logging, and ok=false if the container was unknown.
func (r *ContainerRegistry) remove(id string) (map[string][]PortBinding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.data[id]
	if !ok {
		return nil, false
	}
	entry.cancel()
	delete(r.data, id)
	return entry.ports, true
}

// --- Port forwarding ---

// handleConn proxies a single accepted connection to target, and stops when
// ctx is cancelled (which closes both sides).
func handleConn(ctx context.Context, src net.Conn, target string) {
	defer src.Close()

	dst, err := net.Dial("tcp", target)
	if err != nil {
		debugLog("forward: dial %s failed: %v", target, err)
		return
	}
	defer dst.Close()

	// Close both connections when context is cancelled.
	go func() {
		<-ctx.Done()
		src.Close()
		dst.Close()
	}()

	done := make(chan struct{}, 2)
	go func() {
		io.Copy(dst, src)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(src, dst)
		done <- struct{}{}
	}()
	<-done
}

// forwardPort listens on listenAddr and forwards every connection to targetAddr.
// Stops accepting when ctx is cancelled.
func forwardPort(ctx context.Context, listenAddr, targetAddr string) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logLine("[forwarder] ERROR listen %s: %v\n", listenAddr, err)
		return
	}

	// Unblock Accept() when context is cancelled.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	debugLog("forward: listening on %s -> %s", listenAddr, targetAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Context cancelled — normal shutdown.
			if ctx.Err() != nil {
				return
			}
			debugLog("forward: accept error on %s: %v", listenAddr, err)
			return
		}
		go handleConn(ctx, conn, targetAddr)
	}
}

// startForwarders spawns one forwardPort goroutine per host-port binding.
// The remote side is always the Docker host (10.0.0.1) at the same port,
// since that's where Docker published the container port.
func startForwarders(ctx context.Context, name string, ts string, ports map[string][]PortBinding) {
	dockerHost := "10.0.0.1"

	for containerPort, bindings := range ports {
		for _, b := range bindings {
			if b.HostPort == "" {
				continue
			}

			// Skip duplicate bindings (Docker emits one for 0.0.0.0 and one
			// for :: for the same port when both IPv4 and IPv6 are enabled;
			// we only need to listen once locally).
			if b.HostIP != "" && b.HostIP != "0.0.0.0" {
				debugLog("forward: skipping non-default binding %s:%s", b.HostIP, b.HostPort)
				continue
			}

			listenAddr := fmt.Sprintf(":%s", b.HostPort)
			targetAddr := fmt.Sprintf("%s:%s", dockerHost, b.HostPort)

			logLine("[%s] FORWARD %-20s :%s -> %s:%s (%s)\n",
				ts, name, b.HostPort, dockerHost, b.HostPort, containerPort)

			go forwardPort(ctx, listenAddr, targetAddr)
		}
	}
}

// --- Docker API helpers ---

func inspectContainer(id string) (*ContainerInspect, error) {
	url := fmt.Sprintf("%s/containers/%s/json", dockerBase, id)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("inspect request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inspect returned HTTP %d", resp.StatusCode)
	}

	var info ContainerInspect
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}
	return &info, nil
}

// --- Formatting ---

func formatPorts(ports map[string][]PortBinding) string {
	if len(ports) == 0 {
		return "(none)"
	}
	var parts []string
	for containerPort, bindings := range ports {
		if len(bindings) == 0 {
			continue
		}
		for _, b := range bindings {
			hostIP := b.HostIP
			if hostIP == "" || hostIP == "0.0.0.0" {
				hostIP = "*"
			}
			parts = append(parts, fmt.Sprintf("%s:%s -> %s", hostIP, b.HostPort, containerPort))
		}
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ", ")
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func timestamp(unix int64) string {
	return time.Unix(unix, 0).Format("15:04:05")
}

// --- Event handling ---

func handleStart(event DockerEvent, reg *ContainerRegistry) {
	id := event.Actor.ID
	name := event.Actor.Attributes["name"]
	ts := timestamp(event.Time)

	info, err := inspectContainer(id)
	if err != nil {
		logLine("[%s] WARN: could not inspect %s (%s): %v\n", ts, name, shortID(id), err)
		return
	}

	ports := info.NetworkSettings.Ports

	ctx, cancel := context.WithCancel(context.Background())
	reg.add(id, &ContainerEntry{ports: ports, cancel: cancel})

	if len(ports) > 0 {
		logLine("[%s] START   %-20s %s  ports: %s\n",
			ts, name, shortID(id), formatPorts(ports))
		startForwarders(ctx, name, ts, ports)
	} else {
		// No ports — cancel immediately, nothing to forward.
		cancel()
	}
}

func handleDie(event DockerEvent, reg *ContainerRegistry) {
	id := event.Actor.ID
	name := event.Actor.Attributes["name"]
	exitCode := event.Actor.Attributes["exitCode"]
	ts := timestamp(event.Time)

	ports, ok := reg.remove(id) // also calls cancel(), tearing down forwarders
	portStr := "(unknown — not seen at start)"
	if ok {
		portStr = formatPorts(ports)
	}

	logLine("[%s] DIE     %-20s %s  exit=%s  ports released: %s\n",
		ts, name, shortID(id), exitCode, portStr)
}

// syncExisting queries all running containers and registers them as if we had
// seen their start events. This ensures port forwarding is set up for
// containers that were already running when this program started.
func syncExisting(reg *ContainerRegistry) error {
	url := fmt.Sprintf("%s/containers/json", dockerBase)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("list containers returned HTTP %d: %s", resp.StatusCode, body)
	}

	var containers []struct {
		ID    string            `json:"Id"`
		Names []string          `json:"Names"`
		Ports []struct {
			IP          string `json:"IP"`
			PrivatePort uint16 `json:"PrivatePort"`
			PublicPort  uint16 `json:"PublicPort"`
			Type        string `json:"Type"`
		} `json:"Ports"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return fmt.Errorf("decode containers: %w", err)
	}

	ts := timestamp(time.Now().Unix())
	for _, c := range containers {
		name := strings.TrimPrefix(c.Names[0], "/")

		// Use inspect to get the full NetworkSettings.Ports map, same as handleStart.
		info, err := inspectContainer(c.ID)
		if err != nil {
			logLine("[%s] WARN: sync: could not inspect %s (%s): %v\n", ts, name, shortID(c.ID), err)
			continue
		}

		ports := info.NetworkSettings.Ports
		ctx, cancel := context.WithCancel(context.Background())
		reg.add(c.ID, &ContainerEntry{ports: ports, cancel: cancel})

		if len(ports) > 0 {
			logLine("[%s] EXISTS  %-20s %s  ports: %s\n", ts, name, shortID(c.ID), formatPorts(ports))
			startForwarders(ctx, name, ts, ports)
		} else {
			cancel()
		}
	}
	return nil
}

// --- Main event loop ---

func streamEvents(reg *ContainerRegistry) error {
	url := fmt.Sprintf("%s/events", dockerBase)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("events endpoint returned HTTP %d: %s", resp.StatusCode, body)
	}

	logLine("Connected to %s — watching for port binding changes...\n\n", dockerBase)

	decoder := json.NewDecoder(resp.Body)
	for {
		var event DockerEvent
		if err := decoder.Decode(&event); err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		debugLog("DEBUG event: Type=%s Action=%s ID=%s Name=%s",
			event.Type, event.Action, shortID(event.Actor.ID), event.Actor.Attributes["name"])

		if event.Type != "container" {
			continue
		}

		switch event.Action {
		case "start":
			handleStart(event, reg)
		case "die":
			handleDie(event, reg)
		}
	}
}

func main() {
	flag.BoolVar(&debugEnabled, "debug", false, "enable debug-level event logging")
	flag.Parse()

	// Load .env (best-effort — missing file is not fatal).
	if err := godotenv.Load(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("error reading .env: %v", err)
	}
	if v := os.Getenv("DOCKER_BASE"); v != "" {
		dockerBase = v
	}
	if v := os.Getenv("LOG_FILE_PATH"); v != "" {
		logFilePath = v
	}

	// Open log file (append mode).
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("cannot open log file %s: %v", logFilePath, err)
	}
	defer f.Close()
	fileLogger = log.New(f, "", 0) // no prefix — logLine already formats the message

	fileLogger.Printf("started; writing output to %s (debug=%v)", logFilePath, debugEnabled)

	reg := newRegistry()

	if err := syncExisting(reg); err != nil {
		logLine("WARN: could not sync existing containers: %v\n", err)
	}

	for {
		if err := streamEvents(reg); err != nil {
			logLine("ERROR: %v — reconnecting in 5s...\n", err)
			time.Sleep(5 * time.Second)
		}
	}
}
