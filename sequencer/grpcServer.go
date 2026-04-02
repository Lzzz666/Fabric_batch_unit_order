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
	// [PROBE-B] lock_wait = 等待前一個 batch 釋放鎖的時間（排隊效應）
	funcStart := time.Now()
	s.mu.Lock()
	lockAcquired := time.Now()
	lockWait := lockAcquired.Sub(funcStart)
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

	// [PROBE-B] copy 時間（應該 µs 級，若變大說明 GC 壓力）
	copyStart := time.Now()
	batchDataCopy := make([]byte, len(batchData))
	copy(batchDataCopy, batchData)
	copyDuration := time.Since(copyStart)

	// 在最後 4 bytes 填入 sequencer number
	if len(batchDataCopy) >= 4 {
		binary.LittleEndian.PutUint32(batchDataCopy[len(batchDataCopy)-4:], seqNum)
	}

	fmt.Printf("📥 [Sequencer gRPC] 收到批次: %d bytes (Seq #%d)\n", len(batchDataCopy), seqNum)

	// 轉發到 orderers
	successCount := 0
	failCount := 0
	forwardStart := time.Now()

	if s.useGRPC {
		ports := []string{"7073", "8073", "9073", "10073"}
		addrs := []string{"10.140.0.19", "10.140.0.20", "10.140.0.21", "10.140.0.22"}
		startIdx := 4 - s.broadcastCount

		// Phase 1：串行建立缺少的連接（仍在 s.mu 保護下）
		for i := startIdx; i < 4; i++ {
			if s.grpcOrdererConns[i] != nil {
				continue
			}
			udpPort, _ := strconv.Atoi(ports[i])
			grpcAddress := fmt.Sprintf("%s:%s", addrs[i], strconv.Itoa(udpPort+1))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, err := grpc.DialContext(ctx, grpcAddress,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			cancel()
			if err != nil {
				fmt.Printf("❌ [Sequencer gRPC] 連接失敗 (orderer %d, %s, seq=%d): %v\n", i, grpcAddress, seqNum, err)
				continue
			}
			s.grpcOrdererConns[i] = &grpcOrdererConn{
				address: grpcAddress,
				conn:    conn,
				client:  NewSequencerServiceClient(conn),
			}
			fmt.Printf("✅ [Sequencer gRPC] 動態連接到 orderer %d: %s\n", i, grpcAddress)
		}

		// Phase 2：並行轉發到所有 orderers（forward = max(orderer_i)，而非 sum）
		type result struct {
			idx     int
			connErr bool
			errMsg  string
		}
		req := &SubmitBatchRequest{BatchData: batchDataCopy}
		resultCh := make(chan result, s.broadcastCount)
		var wg sync.WaitGroup

		for i := startIdx; i < 4; i++ {
			if s.grpcOrdererConns[i] == nil {
				resultCh <- result{idx: i, connErr: false, errMsg: "no connection"}
				continue
			}
			wg.Add(1)
			go func(idx int, client SequencerServiceClient) {
				defer wg.Done()
				sendStart := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := client.SubmitBatch(ctx, req)
				cancel()
				dur := time.Since(sendStart)
				if err != nil {
					fmt.Printf("❌ [Sequencer gRPC] 轉發失敗 (orderer %d, seq=%d, took=%v): %v\n", idx, seqNum, dur, err)
					resultCh <- result{idx: idx, connErr: true}
				} else if !resp.Success {
					fmt.Printf("❌ [Sequencer gRPC] orderer %d 拒絕批次 (seq=%d, took=%v): %s\n", idx, seqNum, dur, resp.ErrorMessage)
					resultCh <- result{idx: idx, connErr: false, errMsg: resp.ErrorMessage}
				} else {
					fmt.Printf("🔬 [PROBE-B] orderer[%d] forward seq#%d size=%dB took=%v\n", idx, seqNum, len(batchDataCopy), dur)
					resultCh <- result{idx: idx}
				}
			}(i, s.grpcOrdererConns[i].client)
		}

		wg.Wait()
		close(resultCh)

		for r := range resultCh {
			if r.errMsg == "" && !r.connErr {
				successCount++
			} else {
				if r.connErr && s.grpcOrdererConns[r.idx] != nil {
					s.grpcOrdererConns[r.idx].conn.Close()
					s.grpcOrdererConns[r.idx] = nil
				}
				failCount++
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

	forwardTotal := time.Since(forwardStart)
	totalHold := time.Since(lockAcquired)
	// [PROBE-B] 並行化後 forward = max(orderer_i)，而非 sum
	// lock_wait 應趨近於 0（因為 P0 fix 後 gateway 不再持 sbc.mu 等 sequencer 回應）
	fmt.Printf("🔬 [PROBE-B] Seq#%d size=%dB lock_wait=%v copy=%v forward=%v total_hold=%v\n",
		seqNum, len(batchData), lockWait, copyDuration, forwardTotal, totalHold)

	if failCount > 0 {
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
