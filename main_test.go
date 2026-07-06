package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// startEchoServer runs a TCP server on an ephemeral port that echoes
// everything it receives, and returns its address.
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					c.Write(buf[:n])
				}
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// freePort reserves an ephemeral port and returns it for the forwarder to use.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func dialWithRetry(t *testing.T, addr string) net.Conn {
	t.Helper()
	var lastErr error
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dial %s: %v", addr, lastErr)
	return nil
}

func TestForwardPortProxiesData(t *testing.T) {
	target := startEchoServer(t)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go forwardPort(ctx, listenAddr, target)

	conn := dialWithRetry(t, listenAddr)
	defer conn.Close()

	msg := []byte("ping through forwarder")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(msg))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("got %q, want %q", buf, msg)
	}
}

func TestForwardPortCancelClosesListenerAndConns(t *testing.T) {
	target := startEchoServer(t)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	ctx, cancel := context.WithCancel(context.Background())
	go forwardPort(ctx, listenAddr, target)

	conn := dialWithRetry(t, listenAddr)
	defer conn.Close()

	cancel()

	// The live proxied connection must be torn down.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected proxied connection to be closed after cancel")
	}

	// The listener must stop accepting new connections.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", listenAddr, 100*time.Millisecond)
		if err != nil {
			return // refused — listener is gone
		}
		c.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("listener still accepting connections after cancel")
}

func TestRegistryAddDuplicateCancelsOldEntry(t *testing.T) {
	reg := newRegistry()

	oldCancelled := make(chan struct{})
	reg.add("id1", &ContainerEntry{cancel: func() { close(oldCancelled) }})
	reg.add("id1", &ContainerEntry{cancel: func() {}})

	select {
	case <-oldCancelled:
	default:
		t.Fatal("re-adding an ID must cancel the previous entry's forwarders")
	}
}
