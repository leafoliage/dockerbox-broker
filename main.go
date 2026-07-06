package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/joho/godotenv"
)

const envFilePath = "/usr/local/etc/dockerbox-broker/dockerbox-broker.env"

// Defaults — overridden by .env file.
var dockerBase = "http://10.0.0.1:2375"
var logFilePath = "/var/log/dockerbox-broker.log"
var socketPath = "/var/run/dockerbox-broker.sock"
var maxRetries = 1

// TCP keepalive settings for the event stream connection.
var keepaliveIdle = 30    // seconds of silence before first probe
var keepaliveInterval = 5 // seconds between probes
var keepaliveCount = 1    // number of unanswered probes before giving up
var connectTimeout = 1

var debugEnabled bool

// Derived from dockerBase in loadConfig; forwarders dial this host.
var dockerHost string

// Built in loadConfig so it picks up CONNECT_TIMEOUT.
var httpClient *http.Client

// --- Logging ---

var fileLogger *log.Logger

const timeFmt = "2006-01-02 15:04:05"

func logf(level, format string, args ...any) {
	if fileLogger == nil {
		return
	}
	fileLogger.Printf("[%s] [%s] %s", time.Now().Format(timeFmt), level, fmt.Sprintf(format, args...))
}

func logDebug(format string, args ...any) {
	if debugEnabled {
		logf("DEBUG", format, args...)
	}
}

func logInfo(format string, args ...any)  { logf("INFO ", format, args...) }
func logWarn(format string, args ...any)  { logf("WARN ", format, args...) }
func logError(format string, args ...any) { logf("ERROR", format, args...) }

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
	ID              string `json:"Id"`
	Name            string `json:"Name"`
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

// add registers a container entry. If the ID is already tracked (e.g. a
// duplicate start event or a reconcile race), the old entry's forwarders are
// cancelled first so they release their ports.
func (r *ContainerRegistry) add(id string, entry *ContainerEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.data[id]; ok {
		old.cancel()
	}
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

// StatusEntry is the JSON-serialisable snapshot of one container's state.
type StatusEntry struct {
	Name  string   `json:"name"`
	ID    string   `json:"id"`
	Ports []string `json:"ports"` // ":<hostPort> -> <dockerHost>:<hostPort> (<containerPort>)"
}

// StatusResponse is the envelope written to the unix socket.
// If Error is non-empty the client should display it and exit non-zero.
type StatusResponse struct {
	Connected bool          `json:"connected"`
	Error     string        `json:"error,omitempty"`
	Entries   []StatusEntry `json:"entries"`
}

// snapshot returns a sorted, read-safe slice of StatusEntry for all live containers.
// Names are fetched on-demand via inspect so they are always current, even after
// a container rename. The registry lock is released before any HTTP call.
func (r *ContainerRegistry) snapshot() []StatusEntry {
	// Copy what we need under the lock, then release before doing HTTP.
	r.mu.RLock()
	type idAndPorts struct {
		id    string
		ports map[string][]PortBinding
	}
	items := make([]idAndPorts, 0, len(r.data))
	for id, e := range r.data {
		items = append(items, idAndPorts{id: id, ports: e.ports})
	}
	r.mu.RUnlock()

	entries := make([]StatusEntry, 0, len(items))
	for _, item := range items {
		info, err := inspectContainer(item.id)
		if err != nil {
			// Container may have just died — skip it.
			continue
		}
		name := strings.TrimPrefix(info.Name, "/")
		var ports []string
		for containerPort, bindings := range item.ports {
			for _, b := range bindings {
				if b.HostIP != "" && b.HostIP != "0.0.0.0" {
					continue // skip duplicate IPv6 bindings
				}
				ports = append(ports, fmt.Sprintf(":%s -> %s:%s (%s)", b.HostPort, dockerHost, b.HostPort, containerPort))
			}
		}
		sort.Strings(ports)
		entries = append(entries, StatusEntry{Name: name, ID: shortID(item.id), Ports: ports})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

// ids returns the set of container IDs currently in the registry.
func (r *ContainerRegistry) ids() map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]bool, len(r.data))
	for id := range r.data {
		out[id] = true
	}
	return out
}

// evict cancels and removes any registry entry whose ID is not in the live set.
func (r *ContainerRegistry) evict(live map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, entry := range r.data {
		if !live[id] {
			entry.cancel()
			delete(r.data, id)
		}
	}
}

// --- Daemon state ---

// daemonState tracks whether the event stream connection to Docker is live.
// serveStatus checks this before calling snapshot() to avoid hanging on
// inspect requests when the Docker host is unreachable.
type daemonState struct {
	mu        sync.RWMutex
	connected bool
}

func (s *daemonState) setConnected(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = v
}

func (s *daemonState) isConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connected
}

// --- Unix socket status server ---

// serveStatus listens on the Unix socket and writes a StatusResponse to each
// connecting client, then closes the connection. It checks daemonState before
// calling snapshot() to avoid hanging when the Docker host is unreachable.
func serveStatus(reg *ContainerRegistry, state *daemonState) {
	// Remove stale socket from a previous run.
	os.Remove(socketPath)

	// Create the socket with root-only permissions (no chmod-after-listen
	// window where other users could connect).
	oldUmask := syscall.Umask(0o177)
	ln, err := net.Listen("unix", socketPath)
	syscall.Umask(oldUmask)
	if err != nil {
		logError("status socket listen %s: %v", socketPath, err)
		return
	}
	defer ln.Close()
	defer os.Remove(socketPath)

	logDebug("status socket listening on %s", socketPath)

	for {
		conn, err := ln.Accept()
		if err != nil {
			logDebug("status socket accept error: %v", err)
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			var resp StatusResponse
			if !state.isConnected() {
				resp = StatusResponse{Connected: false, Error: "docker host unreachable"}
			} else {
				resp = StatusResponse{Connected: true, Entries: reg.snapshot()}
			}
			json.NewEncoder(c).Encode(resp)
		}(conn)
	}
}

// --- Status client ---

// runStatus connects to the daemon's Unix socket and prints a formatted table.
func runStatus() {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to %s: %v\n", socketPath, err)
		fmt.Fprintf(os.Stderr, "is dockerbox-broker running?\n")
		os.Exit(1)
	}
	defer conn.Close()

	var resp StatusResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		fmt.Fprintf(os.Stderr, "error: bad response from daemon: %v\n", err)
		os.Exit(1)
	}

	if !resp.Connected {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		os.Exit(1)
	}

	if len(resp.Entries) == 0 {
		fmt.Println("No active port forwarding entries.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CONTAINER\tID\tFORWARDING")
	for _, e := range resp.Entries {
		if len(e.Ports) == 0 {
			continue
		}
		for i, p := range e.Ports {
			if i == 0 {
				fmt.Fprintf(w, "%s\t%s\t%s\n", e.Name, e.ID, p)
			} else {
				fmt.Fprintf(w, "\t\t%s\n", p)
			}
		}
	}
	w.Flush()
}

// --- Port forwarding ---

// handleConn proxies a single accepted connection to target, and stops when
// ctx is cancelled (which closes both sides).
func handleConn(ctx context.Context, src net.Conn, target string) {
	defer src.Close()

	dst, err := net.Dial("tcp", target)
	if err != nil {
		logDebug("forward: dial %s failed: %v", target, err)
		return
	}
	defer dst.Close()

	done := make(chan struct{}, 2)
	go func() {
		io.Copy(dst, src)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(src, dst)
		done <- struct{}{}
	}()

	// Return when either direction finishes or the context is cancelled;
	// the deferred Closes unblock the remaining io.Copy goroutines.
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// forwardPort listens on listenAddr and forwards every connection to targetAddr.
// Stops accepting when ctx is cancelled.
func forwardPort(ctx context.Context, listenAddr, targetAddr string) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logError("forwarder: listen %s: %v", listenAddr, err)
		return
	}

	// Unblock Accept() when context is cancelled.
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	logDebug("forward: listening on %s -> %s", listenAddr, targetAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logDebug("forward: accept error on %s: %v", listenAddr, err)
			return
		}
		go handleConn(ctx, conn, targetAddr)
	}
}

// startForwarders spawns one forwardPort goroutine per host-port binding.
func startForwarders(ctx context.Context, name string, ports map[string][]PortBinding) {

	for containerPort, bindings := range ports {
		for _, b := range bindings {
			if b.HostPort == "" {
				continue
			}
			if b.HostIP != "" && b.HostIP != "0.0.0.0" {
				logDebug("forward: skipping non-default binding %s:%s", b.HostIP, b.HostPort)
				continue
			}

			listenAddr := fmt.Sprintf(":%s", b.HostPort)
			targetAddr := fmt.Sprintf("%s:%s", dockerHost, b.HostPort)

			logInfo("FORWARD %-20s :%s -> %s:%s (%s)", name, b.HostPort, dockerHost, b.HostPort, containerPort)

			go forwardPort(ctx, listenAddr, targetAddr)
		}
	}
}

// --- Docker API helpers ---

func inspectContainer(id string) (*ContainerInspect, error) {
	url := fmt.Sprintf("%s/containers/%s/json", dockerBase, id)
	resp, err := httpClient.Get(url)
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
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// --- Event handling ---

// trackContainer inspects a container, registers it, and starts forwarders
// for its host-port bindings. logPrefix distinguishes event-driven starts
// ("START") from reconcile discoveries ("EXISTS").
func trackContainer(reg *ContainerRegistry, id, name, logPrefix string) {
	info, err := inspectContainer(id)
	if err != nil {
		logWarn("%s: could not inspect %s (%s): %v", logPrefix, name, shortID(id), err)
		return
	}

	ports := info.NetworkSettings.Ports

	ctx, cancel := context.WithCancel(context.Background())
	reg.add(id, &ContainerEntry{ports: ports, cancel: cancel})

	if len(ports) > 0 {
		logInfo("%-8s%-20s %s  ports: %s", logPrefix, name, shortID(id), formatPorts(ports))
		startForwarders(ctx, name, ports)
	} else {
		cancel()
	}
}

func handleStart(event DockerEvent, reg *ContainerRegistry) {
	trackContainer(reg, event.Actor.ID, event.Actor.Attributes["name"], "START")
}

func handleDie(event DockerEvent, reg *ContainerRegistry) {
	id := event.Actor.ID
	name := event.Actor.Attributes["name"]
	exitCode := event.Actor.Attributes["exitCode"]

	ports, ok := reg.remove(id)
	portStr := "(unknown — not seen at start)"
	if ok {
		portStr = formatPorts(ports)
	}

	logInfo("DIE     %-20s %s  exit=%s  ports released: %s", name, shortID(id), exitCode, portStr)
}

// reconcile fetches the current live container list from Docker and brings the
// registry into sync:
//   - containers in the registry that are no longer running are evicted
//     (their forwarders cancelled and ports released);
//   - containers that are running but not yet in the registry are added and
//     their forwarders started.
//
// Called once at startup and again after every event-stream reconnection.
func reconcile(reg *ContainerRegistry, logPrefix string) error {
	url := fmt.Sprintf("%s/containers/json", dockerBase)
	resp, err := httpClient.Get(url)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("list containers returned HTTP %d: %s", resp.StatusCode, body)
	}

	var containers []struct {
		ID    string   `json:"Id"`
		Names []string `json:"Names"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return fmt.Errorf("decode containers: %w", err)
	}

	// Build the live set and evict anything no longer running.
	live := make(map[string]bool, len(containers))
	for _, c := range containers {
		live[c.ID] = true
	}
	reg.evict(live)

	// Add containers not yet tracked.
	known := reg.ids()
	for _, c := range containers {
		if known[c.ID] {
			continue // already tracked, forwarders already running
		}
		if len(c.Names) == 0 {
			continue
		}
		name := strings.TrimPrefix(c.Names[0], "/")
		trackContainer(reg, c.ID, name, logPrefix)
	}
	return nil
}

// newStreamClient builds an http.Client whose TCP connections have per-socket
// keepalive configured via net.KeepAliveConfig
func newStreamClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: time.Duration(connectTimeout) * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     time.Duration(keepaliveIdle) * time.Second,
			Interval: time.Duration(keepaliveInterval) * time.Second,
			Count:    keepaliveCount,
		},
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			ResponseHeaderTimeout: time.Duration(connectTimeout) * time.Second,
		},
		// No overall Timeout — the stream runs indefinitely.
	}
}

// --- Main event loop ---

// connectAndReconcile opens the event stream first — so events are buffered in
// the TCP socket from this point — then reconciles the registry against the
// current live container list. Any events that arrive during reconcile are
// buffered and will be processed by consumeEvents afterward, closing the gap.
func connectAndReconcile(reg *ContainerRegistry, logPrefix string) (*json.Decoder, func(), error) {
	url := fmt.Sprintf("%s/events", dockerBase)
	resp, err := newStreamClient().Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, nil, fmt.Errorf("events endpoint returned HTTP %d: %s", resp.StatusCode, body)
	}

	logInfo("connected to %s — reconciling...", dockerBase)

	if err := reconcile(reg, logPrefix); err != nil {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("reconcile failed: %w", err)
	}

	logInfo("reconcile done — watching for port binding changes...")

	cleanup := func() { resp.Body.Close() }
	return json.NewDecoder(resp.Body), cleanup, nil
}

// consumeEvents reads Docker events from an already-open decoder and dispatches
// them until the stream returns an error.
func consumeEvents(decoder *json.Decoder, reg *ContainerRegistry) error {
	for {
		var event DockerEvent
		if err := decoder.Decode(&event); err != nil {
			return fmt.Errorf("stream error: %w", err)
		}

		logDebug("event: Type=%s Action=%s ID=%s Name=%s", event.Type, event.Action, shortID(event.Actor.ID), event.Actor.Attributes["name"])

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

func loadConfig() {
	if err := godotenv.Load(envFilePath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("error reading .env: %v", err)
	}
	if v := os.Getenv("DOCKER_BASE"); v != "" {
		dockerBase = v
	}
	if v := os.Getenv("LOG_FILE_PATH"); v != "" {
		logFilePath = v
	}
	if v := os.Getenv("SOCKET_PATH"); v != "" {
		socketPath = v
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxRetries = n
		} else {
			log.Fatalf("invalid MAX_RETRIES value: %q", v)
		}
	}
	if v := os.Getenv("KEEPALIVE_IDLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keepaliveIdle = n
		} else {
			log.Fatalf("invalid KEEPALIVE_IDLE value: %q", v)
		}
	}
	if v := os.Getenv("KEEPALIVE_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keepaliveInterval = n
		} else {
			log.Fatalf("invalid KEEPALIVE_INTERVAL value: %q", v)
		}
	}
	if v := os.Getenv("KEEPALIVE_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keepaliveCount = n
		} else {
			log.Fatalf("invalid KEEPALIVE_COUNT value: %q", v)
		}
	}
	if v := os.Getenv("CONNECT_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			connectTimeout = n
		} else {
			log.Fatalf("invalid CONNECT_TIMEOUT value: %q", v)
		}
	}

	u, err := url.Parse(dockerBase)
	if err != nil || u.Hostname() == "" {
		log.Fatalf("invalid DOCKER_BASE value: %q", dockerBase)
	}
	dockerHost = u.Hostname()

	httpClient = &http.Client{Timeout: time.Duration(connectTimeout) * time.Second}
}

func runDaemon() {
	// Open log file (append mode).
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("cannot open log file %s: %v", logFilePath, err)
	}
	defer f.Close()
	fileLogger = log.New(f, "", 0)

	logInfo("started; writing output to %s (debug=%v)", logFilePath, debugEnabled)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		logInfo("shutting down")
		os.Remove(socketPath)
		os.Exit(0)
	}()

	reg := newRegistry()
	state := &daemonState{}

	go serveStatus(reg, state)

	retries := 0
	for {
		decoder, cleanup, err := connectAndReconcile(reg, "EXISTS")
		if err != nil {
			state.setConnected(false)
			retries++
			if retries >= maxRetries {
				logError("connection failed %d times consecutively, giving up", maxRetries)
				os.Exit(1)
			}
			logError("%v — retrying in 5s... (attempt %d/%d)", err, retries, maxRetries)
			time.Sleep(5 * time.Second)
			continue
		}
		retries = 0
		state.setConnected(true)
		if err := consumeEvents(decoder, reg); err != nil {
			state.setConnected(false)
			logError("%v — reconnecting in 5s...", err)
		}
		cleanup()
		time.Sleep(5 * time.Second)
	}
}

func main() {
	flag.BoolVar(&debugEnabled, "debug", false, "enable debug-level event logging")
	flag.Parse()

	loadConfig()

	switch flag.Arg(0) {
	case "status":
		runStatus()
	default:
		runDaemon()
	}
}
