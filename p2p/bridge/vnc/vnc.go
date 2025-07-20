package vnc

import (
	"context"
	"fmt"
	"github.com/Qitmeer/qng/log"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"io"
	"net"
	"sync"
	"time"
)

const (
	pid = "/vnc/1.0.0"
)

// RegisterVNCHandler sets up a VNC stream handler on the libp2p host.
// It accepts incoming streams only from authorized peer IDs and proxies
// the traffic to a local TCP VNC server.
//
// Parameters:
//   - h:        The libp2p host instance.
//   - vncAddr:  Local TCP address of the VNC server to forward traffic to.
//   - allowIDs: List of authorized peer IDs allowed to access the VNC service.
func RegisterVNCHandler(h host.Host, vncAddr string, allowedPeers []string) {
	allowed := make(map[string]struct{})
	for _, p := range allowedPeers {
		allowed[p] = struct{}{}
	}

	log.Info(fmt.Sprintf("Registering VNC handler with ACL, forward to %s", vncAddr))

	h.SetStreamHandler(pid, func(s network.Stream) {
		peerID := s.Conn().RemotePeer().String()
		if _, ok := allowed[peerID]; !ok {
			log.Warn(fmt.Sprintf("Unauthorized VNC request from peer: %s", peerID))
			s.Reset()
			return
		}

		log.Info(fmt.Sprintf("Accepted VNC stream from authorized peer: %s", peerID))

		vncConn, err := net.Dial("tcp", vncAddr)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to connect to local VNC server: %v", err))
			s.Reset()
			return
		}
		go func() {
			defer s.Close()
			defer vncConn.Close()
			io.Copy(s, vncConn)
		}()
		go func() {
			defer s.Close()
			defer vncConn.Close()
			io.Copy(vncConn, s)
		}()
	})
}

// StartVNCBridgeClient sets up a local TCP listener and bridges each accepted connection
// to a remote libp2p peer via a VNC stream.
//
// Parameters:
//   - h: libp2p host instance used to initiate streams
//   - target: remote peer ID (string) running the VNC handler
//   - listenAddr: local TCP address for incoming connections (e.g., 127.0.0.1:5900)
func StartVNCBridgeClient(ctx context.Context, h host.Host, target string, listenAddr string) {
	peerID, err := peer.Decode(target)
	if err != nil {
		log.Error(fmt.Sprintf("Invalid VNC proxy target peer ID: %v", err))
		return
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to listen on %s: %v", listenAddr, err))
		return
	}
	log.Info(fmt.Sprintf("Listening for local VNC clients on %s", listenAddr))

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Warn(fmt.Sprintf("Accept error: %v", err))
				continue
			}

			go func(c net.Conn) {
				defer c.Close()
				sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				log.Info(fmt.Sprintf("Dialing VNC peer %s for proxy stream...", peerID))
				stream, err := h.NewStream(sctx, peerID, pid)
				if err != nil {
					log.Error(fmt.Sprintf("Failed to open stream to VNC peer %s: %v", peerID, err))
					c.Close()
					return
				}
				defer stream.Close()

				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					io.Copy(stream, c)
				}()

				go func() {
					defer wg.Done()
					io.Copy(c, stream)
				}()

				wg.Wait()
				log.Info((fmt.Sprintf("VNC proxy connection closed: %s <-> %s", c.RemoteAddr(), peerID)))
			}(conn)
		}
	}()
}

// streamConn wraps a libp2p Stream to implement the net.Conn interface.
// This allows interoperability with libraries or APIs that expect standard TCP sockets.
// Although not currently used, it provides a useful abstraction for potential integration
// with VNC libraries or other services that require net.Conn.
type streamConn struct {
	network.Stream
}

// streamConn stubs to satisfy the net.Conn interface.
func (c *streamConn) LocalAddr() net.Addr                { return dummyAddr("local") }
func (c *streamConn) RemoteAddr() net.Addr               { return dummyAddr("remote") }
func (c *streamConn) SetDeadline(t time.Time) error      { return nil }
func (c *streamConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *streamConn) SetWriteDeadline(t time.Time) error { return nil }

// dummyAddr is a placeholder address used for the net.Conn interface.
type dummyAddr string

func (a dummyAddr) Network() string { return "libp2p" }
func (a dummyAddr) String() string  { return string(a) }
