package nopaxos

import (
	"fmt"
	"net"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"google.golang.org/protobuf/proto"
)

type UdpServer struct {
	host        string
	port        uint
	sendChan    chan *message
	exitChanUDP chan struct{}
}

func NewUDPServer(_host string, _port uint, _sendChan chan *message, _exitChanUDP chan struct{}) *UdpServer {
	return &UdpServer{host: _host, port: _port, sendChan: _sendChan, exitChanUDP: _exitChanUDP}
}

// 這個檔案不需要
func (us *UdpServer) Start() error {

	addr, err := net.ResolveUDPAddr("udp", ":7073")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}

	buffer := make([]byte, 10240)
	for {
		select {
		case <-us.exitChanUDP:
			conn.Close()
			return nil
		default:
			// Read UDP data
			n, clientAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				fmt.Println("Error reading from connection:", err)
			}

			// Extract the extra bytes from the tail
			if n < 2 {
				fmt.Println("Not enough data received")
			}
			extraBytes := buffer[n-2 : n] // The last 2 bytes are the extra bytes
			fmt.Printf("Received extra bytes: %x\n", extraBytes)

			// Unmarshal the remaining part into the Envelope struct (excluding the last 2 bytes)
			envelope := &common.Envelope{}
			err = proto.Unmarshal(buffer[:n-2], envelope)
			if err != nil {
				fmt.Println("Failed to unmarshal envelope:", err)
			}

			// Display the received Envelope
			fmt.Printf("Received Envelope from %s: Payload = %s, Signature = %s\n", clientAddr, envelope.GetPayload(), envelope.GetSignature())

		}
	}
}
