package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
)

var wgg sync.WaitGroup

func main() {
	param1 := os.Args[1] // First argument (should be an integer)
	broadcastCount, _ := strconv.Atoi(param1)

	addr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:7072")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer conn.Close()

	// 🔥 設置接收緩衝區，避免 OS 層丟包
	if err := conn.SetReadBuffer(64 * 1024 * 1024); err != nil {
		fmt.Printf("⚠️  [Sequencer] 設置接收緩衝區失敗: %v\n", err)
	}

	var count uint32 = 1

	// 🔥 支援大型 batch：100 筆交易 × 3500 bytes ≈ 350KB，設置為 1MB 更安全
	buffer := make([]byte, 1024*1024) // 1 MB buffer

	ports := [9]string{"3073", "4073", "5073", "6073", "7073", "9073", "10073", "8073"}
	addrs := [9]string{"localhost", "localhost", "localhost", "localhost", "localhost", "localhost", "localhost", "localhost"}

	// 🔥 預先創建並復用 UDP 連接，避免每次循環都創建新連接
	ordererConns := make([]*net.UDPConn, 8)
	for i := 8 - broadcastCount; i < 8; i++ {
		ordererAddress := net.JoinHostPort(addrs[i], ports[i])
		ordererServerAddr, err := net.ResolveUDPAddr("udp", ordererAddress)
		if err != nil {
			fmt.Printf("❌ [Sequencer] 解析地址失敗 (%s): %v\n", ordererAddress, err)
			continue
		}

		ordererConn, err := net.DialUDP("udp", nil, ordererServerAddr)
		if err != nil {
			fmt.Printf("❌ [Sequencer] 連接失敗 (%s): %v\n", ordererAddress, err)
			continue
		}

		// 🔥 設置發送緩衝區，避免丟包
		if err := ordererConn.SetWriteBuffer(64 * 1024 * 1024); err != nil {
			fmt.Printf("⚠️  [Sequencer] 設置發送緩衝區失敗 (%s): %v\n", ordererAddress, err)
		}

		ordererConns[i] = ordererConn
		fmt.Printf("✅ [Sequencer] 已連接到 orderer %s\n", ordererAddress)
	}

	// 🔥 確保程序退出時關閉所有連接
	defer func() {
		for i, conn := range ordererConns {
			if conn != nil {
				conn.Close()
				fmt.Printf("🔒 [Sequencer] 已關閉連接到 orderer %d\n", i)
			}
		}
	}()

	for {
		// Read UDP data
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("❌ [Sequencer] ReadFromUDP 錯誤: %v\n", err)
			continue
		}

		// 📊 印出收到的封包大小
		fmt.Printf("📥 [Sequencer] 收到封包: %d bytes (Seq #%d)\n", n, count)

		// Extract the extra bytes from the tail
		if n < 2 {
			fmt.Println("⚠️  [Sequencer] 數據太小，跳過")
			continue
		}

		seqBytes := make([]byte, 4) // The extra bytes you want to add
		binary.LittleEndian.PutUint32(seqBytes, count)
		dataWithseqBytes := append(buffer[:n-4], seqBytes...)

		if count%100 == 0 || count <= 10 {
			fmt.Printf("📤 [Sequencer] 轉發消息 #%d (%d bytes)\n", count, len(dataWithseqBytes))
		}

		// 🔥 使用預先創建的連接轉發
		successCount := 0
		failCount := 0
		for i := 8 - broadcastCount; i < 8; i++ {
			if ordererConns[i] == nil {
				failCount++
				continue
			}

			err = forward(dataWithseqBytes, ordererConns[i])
			if err != nil {
				fmt.Printf("❌ [Sequencer] 轉發失敗 (orderer %d, seq=%d): %v\n", i, count, err)
				failCount++
			} else {
				successCount++
			}
		}

		if failCount > 0 && count%100 == 0 {
			fmt.Printf("⚠️  [Sequencer] Seq #%d: 成功=%d, 失敗=%d\n", count, successCount, failCount)
		}

		count++

		// Optionally, respond to the client
	}
}

func forward(tx []byte, conn *net.UDPConn) error {
	_, err := conn.Write(tx)
	if err != nil {
		fmt.Println("Error sending envelope with extra bytes:", err)
		return err
	}

	return nil
}
