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

// SequencerGRPCServer gRPC 服務端實現
type SequencerGRPCServer struct {
	UnimplementedSequencerServiceServer
	count            *uint32
	ordererConns     []*net.UDPConn
	grpcOrdererConns []*grpcOrdererConn
	broadcastCount   int
	useGRPC          bool
	mu               sync.Mutex
}

// SubmitBatch 實現 gRPC 服務方法
func (s *SequencerGRPCServer) SubmitBatch(ctx context.Context, req *SubmitBatchRequest) (*SubmitBatchResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 增加序號
	*s.count++
	seqNum := *s.count

	batchData := req.BatchData
	if len(batchData) < 8 {
		return &SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   "batch data too small",
			SequenceNumber: 0,
		}, nil
	}

	// 複製 batchData 以避免修改原始數據
	batchDataCopy := make([]byte, len(batchData))
	copy(batchDataCopy, batchData)

	// 在最後 4 bytes 填入 sequencer number
	if len(batchDataCopy) >= 4 {
		binary.LittleEndian.PutUint32(batchDataCopy[len(batchDataCopy)-4:], seqNum)
	}

	fmt.Printf("📥 [Sequencer gRPC] 收到批次: %d bytes (Seq #%d)\n", len(batchDataCopy), seqNum)

	// 轉發到 orderers
	successCount := 0
	failCount := 0

	if s.useGRPC {
		// 使用 gRPC 轉發（使用預先創建的連接，如果不存在則動態創建）
		ports := []string{"7073", "8073", "9073", "10073"}
		addrs := []string{"10.140.0.26", "10.140.0.24", "10.140.0.31", "10.140.0.27"}

		for i := 4 - s.broadcastCount; i < 4; i++ {
			// 如果連接不存在，嘗試創建
			if s.grpcOrdererConns[i] == nil {
				udpPort, _ := strconv.Atoi(ports[i])
				grpcPort := strconv.Itoa(udpPort + 1)
				grpcAddress := fmt.Sprintf("%s:%s", addrs[i], grpcPort)

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := grpc.DialContext(ctx, grpcAddress,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(),
				)
				cancel()

				if err != nil {
					fmt.Printf("❌ [Sequencer gRPC] 連接失敗 (orderer %d, %s, seq=%d): %v\n", i, grpcAddress, seqNum, err)
					failCount++
					continue
				}

				client := NewSequencerServiceClient(conn)
				s.grpcOrdererConns[i] = &grpcOrdererConn{
					address: grpcAddress,
					conn:    conn,
					client:  client,
				}
				fmt.Printf("✅ [Sequencer gRPC] 動態連接到 orderer %d: %s\n", i, grpcAddress)
			}

			// 使用連接發送
			req := &SubmitBatchRequest{
				BatchData: batchDataCopy,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := s.grpcOrdererConns[i].client.SubmitBatch(ctx, req)
			cancel()

			if err != nil {
				fmt.Printf("❌ [Sequencer gRPC] 轉發失敗 (orderer %d, seq=%d): %v\n", i, seqNum, err)
				// 連接可能已斷開，清除連接以便下次重試
				if s.grpcOrdererConns[i] != nil && s.grpcOrdererConns[i].conn != nil {
					s.grpcOrdererConns[i].conn.Close()
				}
				s.grpcOrdererConns[i] = nil
				failCount++
			} else if !resp.Success {
				fmt.Printf("❌ [Sequencer gRPC] orderer %d 拒絕批次 (seq=%d): %s\n", i, seqNum, resp.ErrorMessage)
				failCount++
			} else {
				successCount++
			}
		}
	} else {
		// 使用 UDP 轉發（原有邏輯）
		for i := 4 - s.broadcastCount; i < 4; i++ {
			if s.ordererConns[i] == nil {
				failCount++
				continue
			}

			err := forward(batchDataCopy, s.ordererConns[i])
			if err != nil {
				fmt.Printf("❌ [Sequencer gRPC] 轉發失敗 (orderer %d, seq=%d): %v\n", i, seqNum, err)
				failCount++
			} else {
				successCount++
			}
		}
	}

	if failCount > 0 && seqNum%100 == 0 {
		fmt.Printf("⚠️  [Sequencer gRPC] Seq #%d: 成功=%d, 失敗=%d\n", seqNum, successCount, failCount)
	}

	if seqNum%100 == 0 || seqNum <= 10 {
		fmt.Printf("📤 [Sequencer gRPC] 轉發消息 #%d (%d bytes)\n", seqNum, len(batchDataCopy))
	}

	return &SubmitBatchResponse{
		Success:        true,
		ErrorMessage:   "",
		SequenceNumber: seqNum,
	}, nil
}

// startGRPCServer 啟動 gRPC 服務端
func startGRPCServer(count *uint32, ordererConns []*net.UDPConn, broadcastCount int, grpcOrdererConns []*grpcOrdererConn) {
	port := "7073" // gRPC 端口（不同於 UDP 的 7072）
	if envPort := os.Getenv("SEQUENCER_GRPC_PORT"); envPort != "" {
		port = envPort
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("❌ [Sequencer gRPC] 無法監聽端口 %s: %v\n", port, err)
		return
	}

	// 創建 gRPC 服務器（使用 insecure，生產環境應使用 TLS）
	grpcServer := grpc.NewServer()

	// 檢查是否使用 gRPC 發送到 orderer
	useGRPC := true

	// 註冊服務
	server := &SequencerGRPCServer{
		count:            count,
		ordererConns:     ordererConns,
		grpcOrdererConns: grpcOrdererConns,
		broadcastCount:   broadcastCount,
		useGRPC:          useGRPC,
	}
	RegisterSequencerServiceServer(grpcServer, server)

	fmt.Printf("✅ [Sequencer gRPC] 服務端啟動在端口 %s\n", port)

	// 啟動服務
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Printf("❌ [Sequencer gRPC] 服務端錯誤: %v\n", err)
	}
}
