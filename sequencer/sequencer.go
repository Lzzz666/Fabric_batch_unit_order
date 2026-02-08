package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// GCP orderer addresses (使用內部 IP)
	// orderer-0: 10.140.0.9, orderer-1: 10.140.0.2, orderer-2: 10.140.0.3, orderer-3: 10.140.0.4
	ports := []string{"7073", "8073", "9073", "10073"}
	// addrs := []string{"10.140.0.9", "10.140.0.2", "10.140.0.3", "10.140.0.4"}
	addrs := []string{"localhost", "localhost", "localhost", "localhost"}

	// 檢查是否使用 gRPC 發送到 orderer
	useGRPC := true

	var ordererConns []*net.UDPConn
	var grpcOrdererConns []*grpcOrdererConn

	if useGRPC {
		// 🔥 預先創建並復用 gRPC 連接
		fmt.Printf("🔌 [Sequencer] 使用 gRPC 模式，正在預先創建連接...\n")
		grpcOrdererConns = createGRPCConnections(broadcastCount)
		ordererConns = make([]*net.UDPConn, 4) // 保持為 nil，因為不使用 UDP
	} else {
		// 🔥 預先創建並復用 UDP 連接，避免每次循環都創建新連接
		fmt.Printf("🔌 [Sequencer] 使用 UDP 模式，正在預先創建連接...\n")
		ordererConns = make([]*net.UDPConn, 4)
		for i := 4 - broadcastCount; i < 4; i++ {
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
	}

	// 🔥 確保程序退出時關閉所有連接
	defer func() {
		if useGRPC {
			for i, conn := range grpcOrdererConns {
				if conn != nil && conn.conn != nil {
					conn.conn.Close()
					fmt.Printf("🔒 [Sequencer] 已關閉 gRPC 連接到 orderer %d\n", i)
				}
			}
		} else {
			for i, conn := range ordererConns {
				if conn != nil {
					conn.Close()
					fmt.Printf("🔒 [Sequencer] 已關閉 UDP 連接到 orderer %d\n", i)
				}
			}
		}
	}()

	// 🔥 同時啟動 gRPC 服務端（在 goroutine 中）
	// 注意：需要先從 sequencer.proto 生成 Go 代碼才能使用
	// 運行: protoc --go_out=. --go-grpc_out=. sequencer/sequencer.proto
	go func() {
		// 檢查是否有生成的 gRPC 代碼
		// 如果沒有生成，這個函數會失敗，但不影響 UDP 服務
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("⚠️  [Sequencer] gRPC 服務端未啟動（需要生成 gRPC 代碼）: %v\n", r)
			}
		}()
		startGRPCServer(&count, ordererConns, broadcastCount, grpcOrdererConns)
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
		// 🔥 修復：需要至少 4 bytes 才能安全地使用 buffer[:n-4]
		if n < 4 {
			fmt.Printf("⚠️  [Sequencer] 數據太小 (%d bytes)，跳過（需要至少 4 bytes）\n", n)
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

		// 檢查是否使用 gRPC 發送到 orderer
		useGRPC := true

		if useGRPC {
			// GCP orderer addresses (使用內部 IP)
			// orderer-0: 10.140.0.9, orderer-1: 10.140.0.2, orderer-2: 10.140.0.3, orderer-3: 10.140.0.4
			ports := []string{"7073", "8073", "9073", "10073"}
			// addrs := []string{"10.140.0.9", "10.140.0.2", "10.140.0.3", "10.140.0.4"}
			addrs := []string{"localhost", "localhost", "localhost", "localhost"}

			for i := 4 - broadcastCount; i < 4; i++ {
				// 如果連接不存在，嘗試創建
				if grpcOrdererConns[i] == nil {
					udpPort, _ := strconv.Atoi(ports[i])
					grpcPort := strconv.Itoa(udpPort + 1)
					grpcAddress := fmt.Sprintf("%s:%s", addrs[i], grpcPort)

					// 🔥 修復：使用 defer 確保 context 正確取消
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					conn, err := grpc.DialContext(ctx, grpcAddress,
						grpc.WithTransportCredentials(insecure.NewCredentials()),
						grpc.WithBlock(),
					)
					cancel() // 連接建立後可以安全地取消 context

					if err != nil {
						fmt.Printf("❌ [Sequencer] gRPC 連接失敗 (orderer %d, %s): %v\n", i, grpcAddress, err)
						failCount++
						continue
					}

					client := NewSequencerServiceClient(conn)
					grpcOrdererConns[i] = &grpcOrdererConn{
						address: grpcAddress,
						conn:    conn,
						client:  client,
					}
					fmt.Printf("✅ [Sequencer] 動態連接到 orderer (gRPC) %s\n", grpcAddress)
				}

				// 使用連接發送
				req := &SubmitBatchRequest{
					BatchData: dataWithseqBytes,
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := grpcOrdererConns[i].client.SubmitBatch(ctx, req)
				cancel()

				if err != nil {
					fmt.Printf("❌ [Sequencer] gRPC 轉發失敗 (orderer %d, seq=%d): %v\n", i, count, err)
					// 連接可能已斷開，清除連接以便下次重試
					if grpcOrdererConns[i] != nil && grpcOrdererConns[i].conn != nil {
						grpcOrdererConns[i].conn.Close()
					}
					grpcOrdererConns[i] = nil
					failCount++
				} else if !resp.Success {
					fmt.Printf("❌ [Sequencer] orderer %d 拒絕批次 (seq=%d): %s\n", i, count, resp.ErrorMessage)
					failCount++
				} else {
					successCount++
				}
			}
		} else {
			// 使用 UDP 轉發（原有邏輯）
			for i := 4 - broadcastCount; i < 4; i++ {
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
