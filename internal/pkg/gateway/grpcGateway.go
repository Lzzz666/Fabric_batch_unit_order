package gateway

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// connectGRPC 建立 gRPC 連接到 sequencer
func (gs *Server) connectGRPC() error {
	address := gs.options.SequencerAddress
	if address == "" {
		// address = "172.20.10.5:7073" // 默認 gRPC 端口（不同於 UDP 的 7072）
		address = "10.140.0.30:7073"
	}

	fmt.Printf("🔌 [gRPC Gateway] 正在連接到 sequencer: %s\n", address)

	ctx, cancel := context.WithTimeout(context.Background(), gs.options.DialTimeout)
	defer cancel()

	// 使用 insecure 連接（生產環境應使用 TLS）
	// 移除 WithBlock() 以允許非阻塞連接，避免阻塞 peer 啟動
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithBlock(), // 移除阻塞，允許啟動時 sequencer 未就緒
	)
	if err != nil {
		// 連接失敗時只記錄警告，不阻塞 peer 啟動
		// 連接將在第一次使用時重試
		fmt.Printf("⚠️  [gRPC Gateway] 連接 sequencer 失敗: %v (將在首次使用時重試)\n", err)
		// 確保 GrpcGateway 為 nil，以便後續重連邏輯正確執行
		gs.GrpcGateway = nil
		return nil // 返回 nil，不阻塞啟動
	}

	gs.GrpcGateway = conn
	// 檢查連接狀態（非阻塞連接可能返回連接對象但連接還沒建立）
	if conn.GetState() == connectivity.Ready {
		fmt.Printf("✅ [gRPC Gateway] 已連接到 sequencer: %s (state: Ready)\n", address)
	} else {
		fmt.Printf("⚠️  [gRPC Gateway] 連接對象已創建但未就緒: %s (state: %v，將在首次使用時重試)\n", address, conn.GetState())
	}

	return nil
}

// disconnectGRPC 關閉 gRPC 連接
func (gs *Server) disconnectGRPC() error {
	if gs.GrpcGateway != nil {
		if conn, ok := gs.GrpcGateway.(*grpc.ClientConn); ok {
			// 檢查連接狀態，只有在連接狀態不是 Shutdown 時才關閉
			state := conn.GetState()
			if state != connectivity.Shutdown {
				fmt.Printf("🔒 [gRPC Gateway] 正在關閉連接 (當前狀態: %v)\n", state)
				// 先設置為 nil，避免後續操作使用舊連接
				gs.GrpcGateway = nil
				// 然後關閉連接（可能非同步）
				conn.Close()
				// 等待一小段時間讓連接完全關閉
				time.Sleep(200 * time.Millisecond)
				fmt.Printf("🔒 [gRPC Gateway] 連接已關閉\n")
			} else {
				fmt.Printf("🔒 [gRPC Gateway] 連接已經處於 Shutdown 狀態\n")
				gs.GrpcGateway = nil
			}
		} else {
			gs.GrpcGateway = nil
		}
	}
	return nil
}

// reconnectGRPC 重新連接 gRPC（使用阻塞模式確保連接成功）
func (gs *Server) reconnectGRPC() error {
	fmt.Println("🔄 [gRPC Gateway] Attempting to reconnect...")

	// 先關閉舊連接，disconnectGRPC 內部已經包含了等待時間
	gs.disconnectGRPC()

	// 額外等待一段時間，確保舊連接完全關閉
	time.Sleep(300 * time.Millisecond)

	address := gs.options.SequencerAddress
	if address == "" {
		address = "172.20.10.5:7073" // 默認 gRPC 端口
	}

	fmt.Printf("🔌 [gRPC Gateway] 重新連接到 sequencer: %s (阻塞模式, timeout=%v)\n", address, gs.options.DialTimeout)
	startTime := time.Now()

	// 創建新的 context，避免與舊連接的 context 衝突
	ctx, cancel := context.WithTimeout(context.Background(), gs.options.DialTimeout)

	// 使用阻塞模式確保連接成功
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // 重連時使用阻塞模式
	)
	elapsed := time.Since(startTime)

	// 連接完成後可以取消 context
	cancel()

	if err != nil {
		fmt.Printf("❌ [gRPC Gateway] 連接失敗 (耗時 %v): %v (address=%s, timeout=%v)\n", elapsed, err, address, gs.options.DialTimeout)
		fmt.Printf("❌ [gRPC Gateway] 請確認：\n")
		fmt.Printf("   1. Sequencer 是否正在運行？\n")
		fmt.Printf("   2. Sequencer 的 gRPC 服務是否已啟動（端口 7073）？\n")
		fmt.Printf("   3. 網絡連接是否正常（從容器內能否訪問 %s）？\n", address)
		return fmt.Errorf("error reconnecting to sequencer via gRPC: %w", err)
	}

	gs.GrpcGateway = conn
	state := conn.GetState()
	fmt.Printf("✅ [gRPC Gateway] 已重新連接到 sequencer: %s (state: %v, 耗時: %v)\n", address, state, elapsed)

	if state != connectivity.Ready {
		fmt.Printf("⚠️  [gRPC Gateway] 警告：連接狀態不是 Ready，而是 %v\n", state)
	}

	return nil
}
