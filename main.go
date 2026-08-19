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

// Host that published container ports are forwarded to.
var dockerHost = "10.0.0.1"

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

// Seconds a UDP session may sit idle before its socket is reclaimed.
var udpIdleTimeout = 90

var debugEnabled bool

var httpClient = &http.Client{Timeout: time.Duration(connectTimeout) * time.Second}

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
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
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
	binds  bindMap
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

// StatusEntry is the JSON-serialisable snapshot of one container's state.
type StatusEntry struct {
	Name  string   `json:"name"`
	ID    string   `json:"id"`
	Ports []string `json:"ports"` // "<listenIP>:<hostPort> -> <dockerHost>:<hostPort> (<containerPort>)"
}

// StatusResponse is the envelope written to the unix socket.
// If Error is non-empty the client should display it and exit non-zero.
type StatusResponse struct {
	Connected bool          `json:"connected"`
	Error     string        `json:"error,omitempty"`
	Entries   []StatusEntry `json:"entries"`
}

// snapshot returns a sorted, read-safe slice of StatusEntry for every live
// container that has at least one active forwarder.
// Names are fetched on-demand via inspect so they are always current, even after
// a container rename. The registry lock is released before any HTTP call.
func (r *ContainerRegistry) snapshot() []StatusEntry {
	// Copy what we need under the lock, then release before doing HTTP.
	r.mu.RLock()
	type idAndPorts struct {
		id    string
		ports map[string][]PortBinding
		binds bindMap
	}
	items := make([]idAndPorts, 0, len(r.data))
	for id, e := range r.data {
		items = append(items, idAndPorts{id: id, ports: e.ports, binds: e.binds})
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
			// Mirrors the filtering in startForwarders, so status only ever
			// lists bindings that really have a forwarder behind them.
			proto, supported := protoOf(containerPort)
			if !supported {
				continue
			}
			for _, b := range bindings {
				if b.HostPort == "" {
					continue
				}
				listenIP, forwardable := listenIPOf(b.HostIP)
				if !forwardable {
					continue
				}
				if ip, ok := item.binds.listenIP(b.HostPort, proto); ok {
					listenIP = ip
				}
				ports = append(ports, fmt.Sprintf("%s -> %s (%s)",
					net.JoinHostPort(listenIP, b.HostPort),
					net.JoinHostPort(dockerHost, b.HostPort),
					containerPort))
			}
		}
		if len(ports) == 0 {
			continue
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

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		logError("status socket listen %s: %v", socketPath, err)
		return
	}
	defer ln.Close()
	defer os.Remove(socketPath)

	// Restrict access to root only.
	if err := os.Chmod(socketPath, 0600); err != nil {
		logError("status socket chmod %s: %v", socketPath, err)
		ln.Close()
		return
	}

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

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// forwardTCP listens on listenAddr and forwards every connection to targetAddr.
// Stops accepting when ctx is cancelled.
func forwardTCP(ctx context.Context, listenAddr, targetAddr string) {
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

// Largest possible UDP payload, so a datagram is never silently truncated.
const udpBufSize = 65507

// udpSession is one client's leg of a UDP forward: a socket dialed to the
// target, carrying traffic for exactly one client address.
type udpSession struct {
	conn   net.Conn
	client net.Addr
}

// forwardUDP listens on listenAddr and relays datagrams to targetAddr.
//
// UDP offers neither Accept() nor end-of-stream, so both jobs the kernel does
// for TCP have to be done here. Demultiplexing: every client address gets its
// own socket dialed to the target, so a reply arriving on that socket can be
// traced back to the client that earned it. Lifetime: a session is closed once
// it has gone udpIdleTimeout seconds without traffic, since silence is the only
// hint UDP gives that an exchange is over.
func forwardUDP(ctx context.Context, listenAddr, targetAddr string) {
	ln, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		logError("forwarder: listen udp %s: %v", listenAddr, err)
		return
	}
	logDebug("forward: listening on %s/udp -> %s", listenAddr, targetAddr)
	relayUDP(ctx, ln, targetAddr)
}

// relayUDP is the datagram loop behind forwardUDP, split out so it can be
// driven with a caller-supplied listener.
func relayUDP(ctx context.Context, ln net.PacketConn, targetAddr string) {
	var mu sync.Mutex
	sessions := make(map[string]*udpSession)

	// Unblock ReadFrom() and drop every session when context is cancelled.
	go func() {
		<-ctx.Done()
		ln.Close()
		mu.Lock()
		for _, s := range sessions {
			s.conn.Close()
		}
		mu.Unlock()
	}()

	idle := time.Duration(udpIdleTimeout) * time.Second
	buf := make([]byte, udpBufSize)

	for {
		n, client, err := ln.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logDebug("forward: read error on %s/udp: %v", ln.LocalAddr(), err)
			return
		}

		key := client.String()

		mu.Lock()
		sess, ok := sessions[key]
		if !ok {
			conn, derr := net.Dial("udp", targetAddr)
			if derr != nil {
				mu.Unlock()
				logDebug("forward: dial udp %s failed: %v", targetAddr, derr)
				continue
			}
			sess = &udpSession{conn: conn, client: client}
			sessions[key] = sess
			logDebug("forward: udp session %s -> %s opened", key, targetAddr)

			// Carry replies back to this client until the session goes idle,
			// the target refuses us, or the forwarder shuts down. Whichever
			// happens, the session reaps itself.
			go func(s *udpSession, key string) {
				defer func() {
					mu.Lock()
					delete(sessions, key)
					mu.Unlock()
					s.conn.Close()
					logDebug("forward: udp session %s closed", key)
				}()

				rbuf := make([]byte, udpBufSize)
				for {
					n, err := s.conn.Read(rbuf)
					if err != nil {
						return
					}
					if _, err := ln.WriteTo(rbuf[:n], s.client); err != nil {
						return
					}
				}
			}(sess, key)
		}
		mu.Unlock()

		// Traffic in either direction keeps the session alive; the reply
		// goroutine is blocked on Read, so its deadline is the idle clock.
		sess.conn.SetReadDeadline(time.Now().Add(idle))

		if _, err := sess.conn.Write(buf[:n]); err != nil {
			logDebug("forward: write to %s failed: %v", targetAddr, err)
		}
	}
}

// protoOf extracts the protocol from a Docker port key such as "53/udp", and
// reports whether the broker can forward it. A key with no protocol means tcp,
// which is Docker's own default.
func protoOf(containerPort string) (string, bool) {
	proto := "tcp"
	if i := strings.Index(containerPort, "/"); i >= 0 {
		proto = containerPort[i+1:]
	}
	return proto, proto == "tcp" || proto == "udp"
}

// listenIPOf maps a binding's HostIp onto the address the broker listens on
// locally, and reports whether the binding can be forwarded at all. An empty
// string means the wildcard, i.e. every local interface.
//
// Only "::" is refused, and only because Docker reports it alongside "0.0.0.0"
// for a wildcard publish; honouring both would have the second forwarder
// collide with the dual-stack listener the first one opened.
func listenIPOf(hostIP string) (string, bool) {
	switch hostIP {
	case "", "0.0.0.0":
		return "", true
	case "::":
		return "", false
	}
	if net.ParseIP(hostIP) == nil {
		return "", false
	}
	return hostIP, true
}

// --- Bind label ---

// bindLabel is the container label that says which local address each published
// port should be listened on.
const bindLabel = "dockerbox.bind"

// bindMap is a parsed dockerbox.bind label: a default address for every
// published port, plus per-host-port overrides keyed by "8080" or "8080/udp".
type bindMap struct {
	def   string
	ports map[string]string
}

// listenIP returns the address a published host port should be listened on, and
// whether the label had anything to say about it at all. The most specific
// entry wins: a port with a protocol, then the bare port, then the default.
func (m bindMap) listenIP(hostPort, proto string) (string, bool) {
	if ip, ok := m.ports[hostPort+"/"+proto]; ok {
		return ip, true
	}
	if ip, ok := m.ports[hostPort]; ok {
		return ip, true
	}
	if m.def != "" {
		return m.def, true
	}
	return "", false
}

// parseBindLabel reads a dockerbox.bind label. Entries are comma-separated and
// take one of three forms:
//
//	192.168.88.220           every published port is listened for here
//	192.168.88.220:8080      host port 8080, either protocol
//	10.0.0.5:53/udp          host port 53, udp only
//
// An IPv6 address takes brackets when a port follows it: [2001:db8::5]:8080.
//
// Addresses name interfaces of the machine the broker runs on, not of the
// dockerbox — the dial target is always dockerHost.
//
// A malformed entry is reported and skipped rather than failing the whole
// label, so one typo cannot take a container's other ports down with it.
func parseBindLabel(value string) (bindMap, []string) {
	m := bindMap{ports: make(map[string]string)}
	var problems []string

	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		// Take a trailing /proto off first, so the slash can never be read as
		// part of the address.
		spec, proto := entry, ""
		if i := strings.LastIndex(entry, "/"); i >= 0 {
			spec, proto = entry[:i], entry[i+1:]
			if proto != "tcp" && proto != "udp" {
				problems = append(problems, fmt.Sprintf("%q: protocol must be tcp or udp", entry))
				continue
			}
		}

		// A bare address sets the default for every port. Brackets are
		// accepted here too, so [2001:db8::5] and 2001:db8::5 both work.
		bare := strings.TrimSuffix(strings.TrimPrefix(spec, "["), "]")
		if net.ParseIP(bare) != nil {
			if proto != "" {
				problems = append(problems, fmt.Sprintf("%q: a default applies to every port, so it takes no protocol", entry))
				continue
			}
			if m.def != "" {
				problems = append(problems, fmt.Sprintf("%q: default address is already %s", entry, m.def))
				continue
			}
			m.def = bare
			continue
		}

		host, port, err := net.SplitHostPort(spec)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%q: want an address, address:port, or address:port/proto", entry))
			continue
		}
		if net.ParseIP(host) == nil {
			problems = append(problems, fmt.Sprintf("%q: %q is not an IP address", entry, host))
			continue
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			problems = append(problems, fmt.Sprintf("%q: %q is not a port number", entry, port))
			continue
		}

		key := port
		if proto != "" {
			key += "/" + proto
		}
		if prev, dup := m.ports[key]; dup {
			problems = append(problems, fmt.Sprintf("%q: host port %s is already bound to %s", entry, key, prev))
			continue
		}
		m.ports[key] = host
	}
	return m, problems
}

// bindsFor parses a container's bind label, logging anything wrong with it
// against the container's name.
func bindsFor(name string, info *ContainerInspect) bindMap {
	binds, problems := parseBindLabel(info.Config.Labels[bindLabel])
	for _, p := range problems {
		logWarn("%s: bad %s entry %s", name, bindLabel, p)
	}
	return binds
}

// startForwarders spawns one forwarder goroutine per host-port binding, picking
// the transport from the binding's protocol.
func startForwarders(ctx context.Context, name string, ports map[string][]PortBinding, binds bindMap) {

	for containerPort, bindings := range ports {
		proto, supported := protoOf(containerPort)

		for _, b := range bindings {
			if b.HostPort == "" {
				continue
			}
			listenIP, forwardable := listenIPOf(b.HostIP)
			if !forwardable {
				logDebug("forward: skipping binding %s:%s (%s)", b.HostIP, b.HostPort, containerPort)
				continue
			}
			if !supported {
				logWarn("forward: %s binding %s (host port %s) uses unsupported protocol %q — not forwarded",
					name, containerPort, b.HostPort, proto)
				continue
			}

			// The label names an address on this machine and so overrides the
			// one carried by the binding, which names an address inside the
			// dockerbox.
			if ip, ok := binds.listenIP(b.HostPort, proto); ok {
				listenIP = ip
			} else if b.HostIP != "" && b.HostIP != "0.0.0.0" {
				logWarn("forward: %s binding %s is published on %s inside the dockerbox, but the dial goes to %s — "+
					"publish it on the wildcard and set %s instead",
					name, containerPort, b.HostIP, dockerHost, bindLabel)
			}

			forward := forwardTCP
			if proto == "udp" {
				forward = forwardUDP
			}

			listenAddr := net.JoinHostPort(listenIP, b.HostPort)
			targetAddr := net.JoinHostPort(dockerHost, b.HostPort)

			logInfo("FORWARD %-20s %s -> %s (%s)", name, listenAddr, targetAddr, containerPort)

			go forward(ctx, listenAddr, targetAddr)
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
			if b.HostIP == "" || b.HostIP == "0.0.0.0" {
				parts = append(parts, fmt.Sprintf("*:%s -> %s", b.HostPort, containerPort))
				continue
			}
			parts = append(parts, fmt.Sprintf("%s -> %s", net.JoinHostPort(b.HostIP, b.HostPort), containerPort))
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

func handleStart(event DockerEvent, reg *ContainerRegistry) {
	id := event.Actor.ID
	name := event.Actor.Attributes["name"]

	info, err := inspectContainer(id)
	if err != nil {
		logWarn("could not inspect %s (%s): %v", name, shortID(id), err)
		return
	}

	ports := info.NetworkSettings.Ports
	binds := bindsFor(name, info)

	ctx, cancel := context.WithCancel(context.Background())
	reg.add(id, &ContainerEntry{ports: ports, binds: binds, cancel: cancel})

	if len(ports) > 0 {
		logInfo("START   %-20s %s  ports: %s", name, shortID(id), formatPorts(ports))
		startForwarders(ctx, name, ports, binds)
	} else {
		cancel()
	}
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

		info, err := inspectContainer(c.ID)
		if err != nil {
			logWarn("%s: could not inspect %s (%s): %v", logPrefix, name, shortID(c.ID), err)
			continue
		}

		ports := info.NetworkSettings.Ports
		binds := bindsFor(name, info)

		ctx, cancel := context.WithCancel(context.Background())
		reg.add(c.ID, &ContainerEntry{ports: ports, binds: binds, cancel: cancel})

		if len(ports) > 0 {
			logInfo("%-8s %-20s %s  ports: %s", logPrefix, name, shortID(c.ID), formatPorts(ports))
			startForwarders(ctx, name, ports, binds)
		} else {
			cancel()
		}
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
	if v := os.Getenv("UDP_IDLE_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			udpIdleTimeout = n
		} else {
			log.Fatalf("invalid UDP_IDLE_TIMEOUT value: %q", v)
		}
	}
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
