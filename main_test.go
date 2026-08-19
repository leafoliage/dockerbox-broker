package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// echoServer stands in for a published container port: it echoes every datagram
// back to its sender and records which source addresses it saw, which is how
// these tests observe the broker's session behaviour from the far side.
type echoServer struct {
	conn net.PacketConn
	mu   sync.Mutex
	srcs map[string]int
}

func newEchoServer(t *testing.T) *echoServer {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo server listen: %v", err)
	}
	s := &echoServer{conn: conn, srcs: make(map[string]int)}
	go func() {
		buf := make([]byte, 2048)
		for {
			n, src, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			s.mu.Lock()
			s.srcs[src.String()]++
			s.mu.Unlock()
			conn.WriteTo(buf[:n], src)
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return s
}

func (s *echoServer) addr() string { return s.conn.LocalAddr().String() }

// sourceCount is the number of distinct client addresses the server has seen,
// i.e. how many sessions the broker opened against it.
func (s *echoServer) sourceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.srcs)
}

// startRelay runs relayUDP on a loopback listener and returns its address.
func startRelay(t *testing.T, target string) string {
	t.Helper()
	ln, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("relay listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go relayUDP(ctx, ln, target)
	return ln.LocalAddr().String()
}

func dialRelay(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// sendRecv writes payload and returns the reply, failing the test on timeout.
func sendRecv(t *testing.T, c net.Conn, payload string) string {
	t.Helper()
	if _, err := c.Write([]byte(payload)); err != nil {
		t.Fatalf("client write: %v", err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 2048)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("client read after sending %q: %v", payload, err)
	}
	return string(buf[:n])
}

func TestRelayUDPRoundTrip(t *testing.T) {
	echo := newEchoServer(t)
	client := dialRelay(t, startRelay(t, echo.addr()))

	if got := sendRecv(t, client, "hello"); got != "hello" {
		t.Errorf("reply = %q, want %q", got, "hello")
	}
}

// A single listener serves every client, so replies are only correct if each
// client's traffic rides its own session.
func TestRelayUDPDemultiplexesClients(t *testing.T) {
	echo := newEchoServer(t)
	relay := startRelay(t, echo.addr())

	a := dialRelay(t, relay)
	b := dialRelay(t, relay)

	if got := sendRecv(t, a, "aaa"); got != "aaa" {
		t.Errorf("client A got %q, want %q", got, "aaa")
	}
	if got := sendRecv(t, b, "bbb"); got != "bbb" {
		t.Errorf("client B got %q, want %q", got, "bbb")
	}
	if got := echo.sourceCount(); got != 2 {
		t.Errorf("echo server saw %d sources, want 2 (one session per client)", got)
	}
}

// Repeat traffic from one client must reuse its session rather than opening a
// new socket per datagram.
func TestRelayUDPReusesSession(t *testing.T) {
	echo := newEchoServer(t)
	client := dialRelay(t, startRelay(t, echo.addr()))

	sendRecv(t, client, "first")
	sendRecv(t, client, "second")

	if got := echo.sourceCount(); got != 1 {
		t.Errorf("echo server saw %d sources, want 1 (session should be reused)", got)
	}
}

// After udpIdleTimeout of silence the session is reclaimed, so the next
// datagram arrives at the target from a fresh source port.
func TestRelayUDPSessionExpires(t *testing.T) {
	prev := udpIdleTimeout
	udpIdleTimeout = 1
	t.Cleanup(func() { udpIdleTimeout = prev })

	echo := newEchoServer(t)
	client := dialRelay(t, startRelay(t, echo.addr()))

	sendRecv(t, client, "before")
	if got := echo.sourceCount(); got != 1 {
		t.Fatalf("echo server saw %d sources before idling, want 1", got)
	}

	time.Sleep(time.Duration(udpIdleTimeout)*time.Second + 500*time.Millisecond)

	// The client still works; it just gets a new session behind the scenes.
	if got := sendRecv(t, client, "after"); got != "after" {
		t.Errorf("reply after expiry = %q, want %q", got, "after")
	}
	if got := echo.sourceCount(); got != 2 {
		t.Errorf("echo server saw %d sources, want 2 (session should have expired)", got)
	}
}

// The whole point of the label: a port published on the wildcard inside the
// dockerbox is listened for on one local address only, while the dial still
// goes to dockerHost.
func TestStartForwardersHonoursBindLabel(t *testing.T) {
	// Stand in for the published port on the dockerbox. Its port number is
	// reused on the listen side, exactly as a real forward does.
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("target listen: %v", err)
	}
	defer target.Close()
	go func() {
		for {
			c, err := target.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 64)
				if n, err := c.Read(buf); err == nil {
					c.Write(buf[:n])
				}
			}()
		}
	}()

	_, port, err := net.SplitHostPort(target.Addr().String())
	if err != nil {
		t.Fatalf("split target addr: %v", err)
	}

	// A second loopback address stands in for one interface of many. Not every
	// system has it configured.
	const bindIP = "127.0.0.2"
	if probe, err := net.Listen("tcp", net.JoinHostPort(bindIP, "0")); err != nil {
		t.Skipf("%s is not configured on this host: %v", bindIP, err)
	} else {
		probe.Close()
	}

	prev := dockerHost
	dockerHost = "127.0.0.1"
	t.Cleanup(func() { dockerHost = prev })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	binds, problems := parseBindLabel(bindIP + ":" + port)
	if len(problems) != 0 {
		t.Fatalf("parseBindLabel reported %v, want none", problems)
	}
	startForwarders(ctx, "labelled", map[string][]PortBinding{
		"80/tcp": {{HostIP: "0.0.0.0", HostPort: port}},
	}, binds)

	// Give the forwarder goroutine its listener.
	addr := net.JoinHostPort(bindIP, port)
	var c net.Conn
	for i := 0; i < 50; i++ {
		if c, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()

	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read back through the forwarder: %v", err)
	}
	if got := string(buf[:n]); got != "ping" {
		t.Errorf("got %q, want %q", got, "ping")
	}

	// The label narrows the listen: a third address must still be free on that
	// port, which it would not be had the forwarder taken the wildcard.
	if l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.3", port)); err != nil {
		t.Errorf("port %s looks bound on the wildcard, not just %s: %v", port, bindIP, err)
	} else {
		l.Close()
	}
}

func TestParseBindLabel(t *testing.T) {
	tests := []struct {
		name  string
		label string
		def   string
		ports map[string]string
	}{
		{
			name:  "empty label",
			label: "",
			ports: map[string]string{},
		},
		{
			name:  "bare address is the default",
			label: "192.168.88.220",
			def:   "192.168.88.220",
			ports: map[string]string{},
		},
		{
			name:  "one entry per published port",
			label: "192.168.88.220:80,192.168.88.220:443,10.0.0.5:53/udp",
			ports: map[string]string{"80": "192.168.88.220", "443": "192.168.88.220", "53/udp": "10.0.0.5"},
		},
		{
			name:  "default alongside overrides",
			label: "192.168.88.220, 127.0.0.1:9000",
			def:   "192.168.88.220",
			ports: map[string]string{"9000": "127.0.0.1"},
		},
		{
			name:  "same host port, one entry per protocol",
			label: "192.168.88.220:53/tcp,10.0.0.5:53/udp",
			ports: map[string]string{"53/tcp": "192.168.88.220", "53/udp": "10.0.0.5"},
		},
		{
			name:  "ipv6 takes brackets with a port, and without",
			label: "[2001:db8::5]:8080,2001:db8::1",
			def:   "2001:db8::1",
			ports: map[string]string{"8080": "2001:db8::5"},
		},
		{
			// Unbracketed, this is a valid address in its own right —
			// 2001:db8:0:0:0:0:5:8080 — so it can only be read as one. That
			// ambiguity is the whole reason a port demands brackets.
			name:  "unbracketed ipv6 is an address, never address:port",
			label: "2001:db8::5:8080",
			def:   "2001:db8::5:8080",
			ports: map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, problems := parseBindLabel(tc.label)
			if len(problems) != 0 {
				t.Fatalf("parseBindLabel(%q) reported %v, want none", tc.label, problems)
			}
			if m.def != tc.def {
				t.Errorf("default = %q, want %q", m.def, tc.def)
			}
			if len(m.ports) != len(tc.ports) {
				t.Fatalf("ports = %v, want %v", m.ports, tc.ports)
			}
			for k, want := range tc.ports {
				if got := m.ports[k]; got != want {
					t.Errorf("ports[%q] = %q, want %q", k, got, want)
				}
			}
		})
	}
}

// A bad entry is dropped on its own; the rest of the label still stands, so one
// typo cannot take a container's other ports down with it.
func TestParseBindLabelRejectsBadEntries(t *testing.T) {
	bad := []string{
		"192.168.88.220:80/sctp",  // protocol the broker cannot forward
		"192.168.88.220/tcp",      // default carrying a protocol
		"not-an-ip:80",            // host is not an address
		"192.168.88.220:http",     // port is not a number
		"192.168.88.220:70000",    // port out of range
		"192.168.88.220:80:extra", // not an address:port at all
	}
	for _, entry := range bad {
		t.Run(entry, func(t *testing.T) {
			label := entry + ",10.0.0.5:9999"
			m, problems := parseBindLabel(label)
			if len(problems) != 1 {
				t.Fatalf("parseBindLabel(%q) reported %v, want exactly one problem", label, problems)
			}
			if got := m.ports["9999"]; got != "10.0.0.5" {
				t.Errorf("the good entry was dropped too: ports[9999] = %q, want %q", got, "10.0.0.5")
			}
		})
	}
}

func TestParseBindLabelRejectsDuplicates(t *testing.T) {
	for _, label := range []string{
		"192.168.88.220,10.0.0.5",               // two defaults
		"192.168.88.220:80,10.0.0.5:80",         // same host port twice
		"192.168.88.220:53/udp,10.0.0.5:53/udp", // same host port and protocol twice
	} {
		m, problems := parseBindLabel(label)
		if len(problems) != 1 {
			t.Errorf("parseBindLabel(%q) reported %v, want exactly one problem", label, problems)
		}
		// The first entry wins, so a duplicate cannot quietly move a port.
		if m.def != "" && m.def != "192.168.88.220" {
			t.Errorf("default = %q, want the first entry to win", m.def)
		}
	}
}

// The most specific entry wins: host port with protocol, then host port, then
// the default.
func TestBindMapListenIP(t *testing.T) {
	m, problems := parseBindLabel("192.168.88.220,10.0.0.5:53,172.16.0.1:53/udp")
	if len(problems) != 0 {
		t.Fatalf("parseBindLabel reported %v, want none", problems)
	}

	tests := []struct {
		hostPort string
		proto    string
		want     string
	}{
		{"53", "udp", "172.16.0.1"},     // exact port and protocol
		{"53", "tcp", "10.0.0.5"},       // port matches, protocol does not
		{"80", "tcp", "192.168.88.220"}, // nothing specific: the default
	}
	for _, tc := range tests {
		got, ok := m.listenIP(tc.hostPort, tc.proto)
		if !ok || got != tc.want {
			t.Errorf("listenIP(%q, %q) = (%q, %v), want (%q, true)", tc.hostPort, tc.proto, got, ok, tc.want)
		}
	}

	// With no default declared, an unmatched port falls back to the wildcard.
	m, _ = parseBindLabel("10.0.0.5:53")
	if got, ok := m.listenIP("80", "tcp"); ok {
		t.Errorf("listenIP(80, tcp) = (%q, true), want no match", got)
	}
}

// The zero bindMap is what a container without the label gets; it must never
// claim an address.
func TestBindMapZeroValue(t *testing.T) {
	var m bindMap
	if got, ok := m.listenIP("80", "tcp"); ok {
		t.Errorf("listenIP on zero bindMap = (%q, true), want no match", got)
	}
}

func TestProtoOf(t *testing.T) {
	tests := []struct {
		key       string
		proto     string
		supported bool
	}{
		{"80/tcp", "tcp", true},
		{"53/udp", "udp", true},
		{"80", "tcp", true}, // no separator: Docker's default
		{"132/sctp", "sctp", false},
		{"80/", "", false},
	}
	for _, tc := range tests {
		proto, supported := protoOf(tc.key)
		if proto != tc.proto || supported != tc.supported {
			t.Errorf("protoOf(%q) = (%q, %v), want (%q, %v)",
				tc.key, proto, supported, tc.proto, tc.supported)
		}
	}
}
