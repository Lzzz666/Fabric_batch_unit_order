package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/common/server/sequencerpb"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type GrpcServer struct {
	host string
	port uint16
	*multichannel.Registrar
	exitChanGRPC chan struct{}
	grpcServer   *grpc.Server
}

// OrdererGRPCServer gRPC 服務端實現
type OrdererGRPCServer struct {
	sequencerpb.UnimplementedSequencerServiceServer
	registrar *multichannel.Registrar
	port      uint16
}

// SubmitBatch 實現 gRPC 服務方法
func (s *OrdererGRPCServer) SubmitBatch(ctx context.Context, req *sequencerpb.SubmitBatchRequest) (*sequencerpb.SubmitBatchResponse, error) {
	fmt.Printf("📥 [gRPC Server Port %d] 收到 SubmitBatch 請求: %d bytes\n", s.port, len(req.BatchData))

	batchData := req.BatchData
	if len(batchData) < 8 {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   "batch data too small",
			SequenceNumber: 0,
		}, nil
	}

	// 複製 batchData 以避免修改原始數據
	batchDataCopy := make([]byte, len(batchData))
	copy(batchDataCopy, batchData)

	// 讀取最後 4 bytes 的 sequencer number
	if len(batchDataCopy) < 4 {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   "batch data too small for sequencer number",
			SequenceNumber: 0,
		}, nil
	}

	extraBytes := batchDataCopy[len(batchDataCopy)-4:]
	paddedBytes := make([]byte, 8)
	copy(paddedBytes[:8-len(extraBytes)], extraBytes)
	var bigEndianValue uint64
	err := binary.Read(bytes.NewReader(paddedBytes), binary.LittleEndian, &bigEndianValue)
	if err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("failed to decode sequencer number: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	fmt.Printf("📥 [gRPC Server Port %d] 收到批次: %d bytes (sequencer: %d)\n", s.port, len(batchDataCopy), bigEndianValue)

	// 檢測是否為批次包
	batchFlag := uint16(batchDataCopy[2])<<8 | uint16(batchDataCopy[3])
	if batchFlag == 0xFFFF {
		// 這是批次包，解析 batch
		if len(batchDataCopy) < 8 {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   "batch packet too small",
				SequenceNumber: 0,
			}, nil
		}

		txnCount := binary.BigEndian.Uint32(batchDataCopy[4:8])
		if txnCount == 0 {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   "batch transaction count is zero",
				SequenceNumber: 0,
			}, nil
		}

		fmt.Printf("📦 [gRPC Server Port %d] 開始解析 batch (交易數量: %d, sequencer: %d)\n", s.port, txnCount, bigEndianValue)

		// 解析 batch 中的每個交易
		batch := make([]*common.Envelope, 0, txnCount)
		offset := 8 // 從 offset 8 開始（跳過前 8 bytes: 2 reserved + 2 flag + 4 txnCount）

		for i := uint32(0); i < txnCount; i++ {
			// 讀取交易長度
			if offset+4 > len(batchDataCopy)-4 {
				return &sequencerpb.SubmitBatchResponse{
					Success:        false,
					ErrorMessage:   fmt.Sprintf("batch parse error: transaction %d length out of range", i),
					SequenceNumber: 0,
				}, nil
			}

			txnLen := binary.BigEndian.Uint32(batchDataCopy[offset : offset+4])
			offset += 4

			// 讀取交易數據
			if offset+int(txnLen) > len(batchDataCopy)-4 {
				return &sequencerpb.SubmitBatchResponse{
					Success:        false,
					ErrorMessage:   fmt.Sprintf("batch parse error: transaction %d data out of range", i),
					SequenceNumber: 0,
				}, nil
			}

			envelope := &common.Envelope{}
			err = proto.Unmarshal(batchDataCopy[offset:offset+int(txnLen)], envelope)
			if err != nil {
				return &sequencerpb.SubmitBatchResponse{
					Success:        false,
					ErrorMessage:   fmt.Sprintf("batch parse error: failed to decode transaction %d: %v", i, err),
					SequenceNumber: 0,
				}, nil
			}

			batch = append(batch, envelope)
			offset += int(txnLen)
		}

		// 驗證解析是否正確
		if offset != len(batchDataCopy)-4 {
			fmt.Printf("⚠️  [gRPC Server Port %d] Batch 解析警告：offset (%d) != len-4 (%d)\n", s.port, offset, len(batchDataCopy)-4)
		}

		// 使用第一個交易獲取 channel support
		if len(batch) == 0 {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   "batch is empty",
				SequenceNumber: 0,
			}, nil
		}

		chdr, isConfig, processor, err := s.registrar.BroadcastChannelSupport(batch[0])
		if err != nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("BroadcastChannelSupport failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		if isConfig {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   "batch contains config transaction, not supported",
				SequenceNumber: 0,
			}, nil
		}

		// 處理 batch：驗證每個交易
		startTime := time.Now()
		configSeq, err := processor.ProcessNormalMsg(batch[0])
		if err != nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("ProcessNormalMsg failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		// 驗證 batch 中的其他交易
		for i := 1; i < len(batch); i++ {
			_, err = processor.ProcessNormalMsg(batch[i])
			if err != nil {
				return &sequencerpb.SubmitBatchResponse{
					Success:        false,
					ErrorMessage:   fmt.Sprintf("Batch[%d] ProcessNormalMsg failed: %v", i, err),
					SequenceNumber: 0,
				}, nil
			}
		}

		if err = processor.WaitReady(); err != nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("WaitReady failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		// 獲取 chain 並調用 OrderBatch
		chain := s.registrar.GetConsensusChain(chdr.ChannelId)
		if chain == nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("cannot get chain (channel: %s)", chdr.ChannelId),
				SequenceNumber: 0,
			}, nil
		}

		orderStart := time.Now()
		err = nopaxos.OrderBatchForChain(chain, batch, configSeq, bigEndianValue)
		orderDuration := time.Since(orderStart)

		if err != nil {
			fmt.Printf("❌ [gRPC Server Port %d] OrderBatch 失敗: %v (Order耗時: %v, 總耗時: %v)\n",
				s.port, err, orderDuration, time.Since(startTime))
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("OrderBatch failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		fmt.Printf("✅ [gRPC Server Port %d] Batch 處理成功 (sequencer: %d, batch size: %d, 總耗時: %v)\n",
			s.port, bigEndianValue, len(batch), time.Since(startTime))
		return &sequencerpb.SubmitBatchResponse{
			Success:        true,
			ErrorMessage:   "",
			SequenceNumber: uint32(bigEndianValue),
		}, nil
	}

	// 單筆交易處理（保持向後兼容）
	envelope := &common.Envelope{}
	err = proto.Unmarshal(batchDataCopy[2:len(batchDataCopy)-4], envelope)
	if err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("failed to unmarshal envelope: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	_, isConfig, processor, err := s.registrar.BroadcastChannelSupport(envelope)
	if err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("BroadcastChannelSupport failed: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	if !isConfig {
		startTime := time.Now()
		fmt.Printf("✅ [gRPC Server Port %d] 開始處理普通交易 (sequencer: %d)\n", s.port, bigEndianValue)

		configSeq, err := processor.ProcessNormalMsg(envelope)
		if err != nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("ProcessNormalMsg failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		if err = processor.WaitReady(); err != nil {
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("WaitReady failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		orderStart := time.Now()
		err = processor.Order(envelope, configSeq, 1, bigEndianValue)
		orderDuration := time.Since(orderStart)

		if err != nil {
			fmt.Printf("❌ [gRPC Server Port %d] Order 失敗: %v (Order耗時: %v, 總耗時: %v)\n",
				s.port, err, orderDuration, time.Since(startTime))
			return &sequencerpb.SubmitBatchResponse{
				Success:        false,
				ErrorMessage:   fmt.Sprintf("Order failed: %v", err),
				SequenceNumber: 0,
			}, nil
		}

		fmt.Printf("✅ [gRPC Server Port %d] 交易處理成功 (sequencer: %d, 總耗時: %v)\n",
			s.port, bigEndianValue, time.Since(startTime))
		return &sequencerpb.SubmitBatchResponse{
			Success:        true,
			ErrorMessage:   "",
			SequenceNumber: uint32(bigEndianValue),
		}, nil
	}

	// Config 交易處理
	config, configSeq, err := processor.ProcessConfigUpdateMsg(envelope)
	if err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("ProcessConfigUpdateMsg failed: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	if err = processor.WaitReady(); err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("WaitReady failed: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	err = processor.Configure(config, configSeq)
	if err != nil {
		return &sequencerpb.SubmitBatchResponse{
			Success:        false,
			ErrorMessage:   fmt.Sprintf("Configure failed: %v", err),
			SequenceNumber: 0,
		}, nil
	}

	return &sequencerpb.SubmitBatchResponse{
		Success:        true,
		ErrorMessage:   "",
		SequenceNumber: uint32(bigEndianValue),
	}, nil
}

func NewGRPCServer(
	_host string,
	_port uint16,
	r *multichannel.Registrar,
) *GrpcServer {
	return &GrpcServer{host: _host, port: _port, exitChanGRPC: make(chan struct{}), Registrar: r}
}

func (gs *GrpcServer) Start() error {
	address := net.JoinHostPort(gs.host, strconv.Itoa(int(gs.port)))
	fmt.Printf("🔌 [gRPC Server] 正在嘗試監聽端口 %s (host=%s, port=%d)\n", address, gs.host, gs.port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("❌ [gRPC Server] 無法監聽端口 %s: %v\n", address, err)
		return err
	}
	fmt.Printf("✅ [gRPC Server] 成功綁定到端口 %s\n", address)

	// 創建 gRPC 服務器（使用 insecure，生產環境應使用 TLS）
	gs.grpcServer = grpc.NewServer()

	// 註冊服務
	server := &OrdererGRPCServer{
		registrar: gs.Registrar,
		port:      gs.port,
	}
	sequencerpb.RegisterSequencerServiceServer(gs.grpcServer, server)

	fmt.Printf("✅ [gRPC Server] 成功啟動在端口 %s\n", address)
	fmt.Printf("✅ [gRPC Server] 等待接收數據...\n")

	// 在 goroutine 中啟動服務，以便可以監聽退出信號
	go func() {
		fmt.Printf("🚀 [gRPC Server] 開始接受連接在端口 %s\n", address)
		if err := gs.grpcServer.Serve(lis); err != nil {
			fmt.Printf("❌ [gRPC Server] 服務端錯誤: %v\n", err)
		}
	}()

	// 等待一小段時間確保服務已啟動
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("✅ [gRPC Server] 服務已啟動並正在監聽端口 %s\n", address)

	// 等待退出信號
	<-gs.exitChanGRPC
	gs.grpcServer.GracefulStop()
	fmt.Printf("🚨 [gRPC Server Port %d] 收到退出信號\n", gs.port)
	return nil
}

func (gs *GrpcServer) Close() {
	fmt.Println("Closing gRPC server on", gs.host, ":", gs.port)
	close(gs.exitChanGRPC)
}
