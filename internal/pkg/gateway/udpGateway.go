package gateway

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/hyperledger/fabric/internal/pkg/gateway/config"
)

func (gs *Server) connect() error {
	// 根據配置選擇傳輸方式
	fmt.Printf("🔍 [Gateway] 配置檢查: SequencerTransport=%s, SequencerAddress=%s\n",
		gs.options.SequencerTransport, gs.options.SequencerAddress)

	if gs.options.SequencerTransport == config.TransportGRPC {
		fmt.Printf("✅ [Gateway] 使用 gRPC 傳輸方式\n")
		return gs.connectGRPC()
	}
	// 默認使用 UDP
	fmt.Printf("✅ [Gateway] 使用 UDP 傳輸方式\n")
	return gs.connectUDP()
}

// connectUDP 建立 UDP 連接到 sequencer
func (gs *Server) connectUDP() error {
	address := gs.options.SequencerAddress
	if address == "" {
		address = "172.20.10.5:7072" // 默認 UDP 地址
		// address = "10.140.0.10:7072"
	}

	fmt.Printf("🔌 [UDP Gateway] 正在連接到 sequencer: %s\n", address)

	sequencerAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return fmt.Errorf("error resolving sequencer address: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, sequencerAddr)
	if err != nil {
		return fmt.Errorf("error connecting to sequencer: %w", err)
	}

	// 🔥 設置 UDP 寫緩衝區，避免大封包丟失
	if err := conn.SetWriteBuffer(16 * 1024 * 1024); err != nil {
		fmt.Printf("⚠️  [UDP Gateway] Warning: Failed to set write buffer: %v\n", err)
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
	fmt.Printf("✅ [UDP Gateway] 已連接到 sequencer: %s\n", address)

	return nil
}

func (gs *Server) disconnect() error {
	if gs.options.SequencerTransport == config.TransportGRPC {
		return gs.disconnectGRPC()
	}
	// UDP
	if gs.UdpGateway != nil {
		err := gs.UdpGateway.Close()
		if err != nil {
			return fmt.Errorf("error closing UDP connection: %w", err)
		}
		gs.UdpGateway = nil
	}
	return nil
}

func (gs *Server) reconnect() error {
	if gs.options.SequencerTransport == config.TransportGRPC {
		return gs.reconnectGRPC()
	}
	// UDP
	fmt.Println("🔄 [UDP Gateway] Attempting to reconnect...")
	if gs.UdpGateway != nil {
		gs.UdpGateway.Close()
	}
	time.Sleep(100 * time.Millisecond) // Wait before attempting to reconnect
	return gs.connectUDP()
}
