package gateway

import (
	"fmt"
	"net"
	"syscall"
	"time"
)

func (gs *Server) connect() error {

	sequencerAddr, err := net.ResolveUDPAddr("udp", "172.20.10.4:7072")
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, sequencerAddr)
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}
	fmt.Println("Connected to server")

	// 🔥 設置 UDP 寫緩衝區，避免大封包丟失
	if err := conn.SetWriteBuffer(16 * 1024 * 1024); err != nil {
		fmt.Printf("⚠️  Warning: Failed to set write buffer: %v\n", err)
	}

	// Set the TTL on the socket
	rawConn, err := conn.SyscallConn()
	if err != nil {
		fmt.Println("Error getting raw connection:", err)
	}

	ttl := 171 // Example TTL value
	err = rawConn.Control(func(fd uintptr) {
		// Set TTL at the IP level
		err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
		if err != nil {
			fmt.Println("Error setting TTL:", err)
		}
	})

	gs.UdpGateway = conn

	return nil
}

func (gs *Server) disconnect() error {

	err := gs.UdpGateway.Close()
	if err != nil {
		return fmt.Errorf("error connecting to server: %w", err)
	}

	return nil
}

func (gs *Server) reconnect() error {
	fmt.Println("Attempting to reconnect...")
	gs.UdpGateway.Close()
	time.Sleep(2 * time.Nanosecond) // Wait before attempting to reconnect

	return gs.connect()
}
