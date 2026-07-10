package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// forwardByGRPC 通過 gRPC 轉發批次到 orderer
func forwardByGRPC(batchData []byte, ordererAddress string) error {
	fmt.Printf("🔌 [Sequencer gRPC Client] 正在連接到 orderer: %s\n", ordererAddress)

	// 創建 gRPC 連接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, ordererAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Printf("❌ [Sequencer gRPC Client] 連接失敗 (%s): %v\n", ordererAddress, err)
		return fmt.Errorf("failed to connect to orderer %s: %w", ordererAddress, err)
	}
	defer conn.Close()

	fmt.Printf("✅ [Sequencer gRPC Client] 已連接到 orderer: %s\n", ordererAddress)

	// 創建 gRPC 客戶端（使用本地生成的類型）
	client := NewSequencerServiceClient(conn)

	// 發送批次請求
	req := &SubmitBatchRequest{
		BatchData: batchData,
	}

	fmt.Printf("📤 [Sequencer gRPC Client] 發送批次到 orderer %s: %d bytes\n", ordererAddress, len(batchData))
	resp, err := client.SubmitBatch(ctx, req)
	if err != nil {
		fmt.Printf("❌ [Sequencer gRPC Client] 發送失敗 (%s): %v\n", ordererAddress, err)
		return fmt.Errorf("failed to submit batch to orderer %s: %w", ordererAddress, err)
	}

	if !resp.Success {
		fmt.Printf("❌ [Sequencer gRPC Client] orderer 拒絕批次 (%s): %s\n", ordererAddress, resp.ErrorMessage)
		return fmt.Errorf("orderer %s rejected batch: %s", ordererAddress, resp.ErrorMessage)
	}

	fmt.Printf("✅ [Sequencer gRPC Client] 批次發送成功到 orderer %s (seq=%d)\n", ordererAddress, resp.SequenceNumber)
	return nil
}

// 預先創建並復用 gRPC 連接
type grpcOrdererConn struct {
	address string
	conn    *grpc.ClientConn
	client  SequencerServiceClient
}

func createGRPCConnections(broadcastCount int) []*grpcOrdererConn {

	// GCP orderer addresses (使用內部 IP)
	// orderer-0: 10.140.0.26, orderer-1: 10.140.0.24, orderer-2: 10.140.0.31, orderer-3: 10.140.0.27
	ports := []string{"7073", "8073", "9073", "10073"}
	addrs := []string{"10.140.0.26", "10.140.0.24", "10.140.0.31", "10.140.0.27"}
	// addrs := []string{"localhost", "localhost", "localhost", "localhost"}

	conns := make([]*grpcOrdererConn, 4)
	for i := 4 - broadcastCount; i < 4; i++ {
		// gRPC 端口 = UDP 端口 + 1
		udpPort, _ := strconv.Atoi(ports[i])
		grpcPort := strconv.Itoa(udpPort + 1)
		grpcAddress := fmt.Sprintf("%s:%s", addrs[i], grpcPort)

		// 嘗試連接（使用較短的超時）
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, grpcAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), // 阻塞直到連接建立
		)
		cancel() // 🔥 連接建立後可以安全地取消 context

		if err != nil {
			fmt.Printf("⚠️  [Sequencer] gRPC 連接失敗 (%s): %v (將在發送時重試)\n", grpcAddress, err)
			// 不設置 conns[i]，讓它在發送時重試連接
			continue
		}

		client := NewSequencerServiceClient(conn)
		conns[i] = &grpcOrdererConn{
			address: grpcAddress,
			conn:    conn,
			client:  client,
		}
		fmt.Printf("✅ [Sequencer] 已連接到 orderer (gRPC) %s\n", grpcAddress)
	}

	return conns
}
