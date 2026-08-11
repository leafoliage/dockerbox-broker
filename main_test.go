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
