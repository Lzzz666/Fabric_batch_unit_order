/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	gp "github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	gproto "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/gossip/state"
	"github.com/hyperledger/fabric/internal/pkg/gateway/config"
	"github.com/hyperledger/fabric/internal/pkg/gateway/sequencerpb"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var serverAddrStr = os.Getenv("SERVER_ADDR")
var count uint64 = 0

// SimpleBatchCollector 簡單的批次收集器
type SimpleBatchCollector struct {
	mu            sync.Mutex
	buffer        []*common.Envelope
	hashes        [][]byte // hash buffer for hash-only batch
	batchSize     int
	timeout       time.Duration
	timer         *time.Timer
	sendFunc      func([]*common.Envelope) error
	sendHashFunc  func([][]byte) error                 // hash-only batch send function
	storeTxnFunc  func([]byte, *common.Envelope) error // store transaction in TxnPool
	batchCount    uint64
	totalTxnCount uint64
}

// NewSimpleBatchCollector 創建簡單批次收集器
func NewSimpleBatchCollector(batchSize int, timeout time.Duration, sendFunc func([]*common.Envelope) error, sendHashFunc func([][]byte) error, storeTxnFunc func([]byte, *common.Envelope) error) *SimpleBatchCollector {
	return &SimpleBatchCollector{
		buffer:       make([]*common.Envelope, 0, batchSize),
		hashes:       make([][]byte, 0, batchSize),
		batchSize:    batchSize,
		timeout:      timeout,
		sendFunc:     sendFunc,
		sendHashFunc: sendHashFunc,
		storeTxnFunc: storeTxnFunc,
	}
}

// computeEnvelopeHash 計算交易的 SHA256 hash
func computeEnvelopeHash(env *common.Envelope) ([]byte, error) {
	data, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return hash[:], nil
}

// Add 添加交易到批次
func (sbc *SimpleBatchCollector) Add(txn *common.Envelope) error {
	sbc.mu.Lock()
	defer sbc.mu.Unlock()

	// 計算交易 hash
	hash, err := computeEnvelopeHash(txn)
	if err != nil {
		return fmt.Errorf("failed to compute envelope hash: %w", err)
	}

	// 🔥 存儲交易到 TxnPool（用於 hash-only 模式）
	if sbc.storeTxnFunc != nil {
		if err := sbc.storeTxnFunc(hash, txn); err != nil {
			fmt.Printf("⚠️  [Batch] 存儲交易到 TxnPool 失敗: %v\n", err)
			// 繼續處理，不阻止批次發送
		}
	}

	// 添加到緩衝區
	sbc.buffer = append(sbc.buffer, txn)
	sbc.hashes = append(sbc.hashes, hash)
	sbc.totalTxnCount++

	// 如果是第一筆交易，啟動計時器
	if len(sbc.buffer) == 1 {
		fmt.Printf("⏳ [Batch] 第一筆交易加入，啟動 %v 超時計時器 (total=%d, hash=%s)\n",
			sbc.timeout, sbc.totalTxnCount, hex.EncodeToString(hash)[:16])
		sbc.timer = time.AfterFunc(sbc.timeout, func() {
			sbc.mu.Lock()
			defer sbc.mu.Unlock()
			if len(sbc.buffer) > 0 {
				fmt.Printf("⏰ [Batch] Timeout triggered, flushing %d txns\n", len(sbc.buffer))
				sbc.flushLocked()
			}
		})
	} else {
		fmt.Printf("📝 [Batch] 交易加入 batch: buffer size=%d/%d, total=%d, hash=%s\n",
			len(sbc.buffer), sbc.batchSize, sbc.totalTxnCount, hex.EncodeToString(hash)[:16])
	}

	// 檢查是否達到批次大小
	if len(sbc.buffer) >= sbc.batchSize {
		return sbc.flushLocked()
	}

	return nil
}

// flushLocked 發送當前批次（需持有鎖）
func (sbc *SimpleBatchCollector) flushLocked() error {
	if len(sbc.buffer) == 0 {
		return nil
	}

	// 停止計時器
	if sbc.timer != nil {
		sbc.timer.Stop()
		sbc.timer = nil
	}

	// 複製 hash 批次
	hashBatch := make([][]byte, len(sbc.hashes))
	copy(hashBatch, sbc.hashes)

	// 複製完整交易批次（用於 gossip 廣播）
	txnBatch := make([]*common.Envelope, len(sbc.buffer))
	copy(txnBatch, sbc.buffer)

	// 清空緩衝區
	sbc.buffer = sbc.buffer[:0]
	sbc.hashes = sbc.hashes[:0]
	sbc.batchCount++

	fmt.Printf("📦 [Batch] Flushing batch #%d with %d txns (hash-only mode)\n", sbc.batchCount, len(hashBatch))

	// 發送 hash-only batch（快速路徑）
	if sbc.sendHashFunc != nil {
		return sbc.sendHashFunc(hashBatch)
	}

	// Fallback: 發送完整交易批次（向後兼容）
	return sbc.sendFunc(txnBatch)
}

// Submit will send the signed transaction to the ordering service. The response indicates whether the transaction was
// successfully received by the orderer. This does not imply successful commit of the transaction, only that is has
// been delivered to the orderer.
func (gs *Server) Submit(ctx context.Context, request *gp.SubmitRequest) (*gp.SubmitResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "a submit request is required")
	}
	/*
		type SubmitRequest struct {
			state         protoimpl.MessageState
			sizeCache     protoimpl.SizeCache
			unknownFields protoimpl.UnknownFields

			TransactionId string `protobuf:"bytes,1,opt,name=transaction_id,json=transactionId,proto3" json:"transaction_id,omitempty"`
			ChannelId string `protobuf:"bytes,2,opt,name=channel_id,json=channelId,proto3" json:"channel_id,omitempty"`
			PreparedTransaction *common.Envelope `protobuf:"bytes,3,opt,name=prepared_transaction,json=preparedTransaction,proto3" json:"prepared_transaction,omitempty"`
		}
	*/

	// TransactionId	bytes,1	string	交易的唯一識別符。標識要提交的特定交易。
	// ChannelId	bytes,2	string	通道/鏈的識別符。指定此請求應提交到哪個特定的區塊鏈通道或分佈式帳本。
	// PreparedTransaction	bytes,3	*common.Envelope	準備好的交易數據。這是一個指向 common.Envelope 結構的指針，其中包含了 已簽名的、**經背書（Endorsed）**的交易提案響應，這是實際要寫入帳本的數據包。

	// 在這裡製作 txn
	// fmt.Printf("[lz debug] request: %x\n", request)
	txn := request.GetPreparedTransaction()
	if txn == nil {
		return nil, status.Error(codes.InvalidArgument, "a prepared transaction is required")
	}
	if len(txn.Signature) == 0 {
		return nil, status.Error(codes.InvalidArgument, "prepared transaction must be signed")
	}
	// gwBench.s1AddTxn() // [BENCH] Stage1: txn arrival
	orderers, clusterSize, err := gs.registry.orderers(request.ChannelId)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", err)
	}

	if len(orderers) == 0 {
		return nil, status.Errorf(codes.Unavailable, "no orderer nodes available")
	}

	logger := logger.With("txID", request.TransactionId)
	fmt.Printf("[lz debug] txID: %s\n", request.TransactionId)

	config := gs.getChannelConfig(request.ChannelId) //  config 基本上大家都一樣？因為只有一個 channel (mychannel)
	oc, ok := config.OrdererConfig()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "failed to create block deliverer for channel `%s`, missing OrdererConfig", request.ChannelId)
	}
	if oc.ConsensusType() == "BFT" {
		fmt.Printf("[lz debug] submitBFT\n")
		return gs.submitBFT(ctx, orderers, txn, clusterSize, logger)
	} else {
		fmt.Printf("[lz debug] submitNonBFT\n")
		return gs.submitNonBFT(ctx, orderers, txn, logger)
	}
}

func (gs *Server) submitBFT(ctx context.Context, orderers []*orderer, txn *common.Envelope, clusterSize int, logger *flogging.FabricLogger) (*gp.SubmitResponse, error) {
	// For BFT, we send transaction to ALL orderers
	waitCh := make(chan *gp.ErrorDetail, len(orderers))
	go gs.broadcastToAll(orderers, txn, waitCh, logger)

	quorum, _ := computeBFTQuorum(uint64(clusterSize))
	successes, failures := 0, 0
	var errDetails []proto.Message
loop:
	for i, total := 0, len(orderers); i < total; i++ {
		select {
		case osnErr := <-waitCh:
			// Broadcast completed normally
			if osnErr != nil {
				errDetails = append(errDetails, osnErr)
				failures++
				if failures > total-quorum {
					break loop
				}
			} else {
				successes++
				if successes >= quorum {
					return &gp.SubmitResponse{}, nil
				}
			}
		case <-ctx.Done():
			// Overall submit timeout expired
			logger.Warnw("Submit call timed out while broadcasting to ordering service")
			return nil, newRpcError(codes.DeadlineExceeded, "submit timeout expired while broadcasting to ordering service")
		}
	}
	logger.Warnw("Insufficient number of orderers could successfully process transaction to satisfy quorum requirement", "successes", successes, "quorum", quorum)
	return nil, newRpcError(codes.Unavailable, "insufficient number of orderers could successfully process transaction to satisfy quorum requirement", errDetails...)
}

func (gs *Server) broadcastToAll(orderers []*orderer, txn *common.Envelope, waitCh chan<- *gp.ErrorDetail, logger *flogging.FabricLogger) {
	everyoneSubmitted := make(chan struct{})
	var numFinishedSend uint32

	broadcastContext, broadcastCancel := context.WithCancel(context.Background())
	defer broadcastCancel()
	for _, o := range orderers {
		go func(ord *orderer) {
			logger.Infow("Sending transaction to orderer", "endpoint", ord.logAddress)
			ctx, cancel := context.WithCancel(broadcastContext)
			defer cancel()
			response, err := gs.broadcast(ctx, ord, txn)
			// If I'm the last to submit, notify this
			if atomic.AddUint32(&numFinishedSend, 1) == uint32(len(orderers)) {
				close(everyoneSubmitted)
			}
			if err != nil {
				logger.Warnw("Error sending transaction to orderer", "endpoint", ord.logAddress, "err", err)
				waitCh <- errorDetail(ord.endpointConfig, err.Error())
			} else if status := response.GetStatus(); status == common.Status_SUCCESS {
				logger.Infow("Successful response from orderer", "endpoint", ord.logAddress)
				waitCh <- nil
			} else {
				logger.Warnw("Unsuccessful response sending transaction to orderer", "endpoint", ord.logAddress, "status", status, "info", response.GetInfo())
				if status == common.Status_SERVICE_UNAVAILABLE && response.GetInfo() == "failed to submit request: request already exists" {
					// the orderer already has this transaction - let it continue
					waitCh <- nil
				} else {
					waitCh <- errorDetail(ord.endpointConfig, fmt.Sprintf("received unsuccessful response from orderer: status=%s, info=%s", common.Status_name[int32(status)], response.GetInfo()))
				}
			}
		}(o)
	}

	t1 := time.NewTimer(gs.options.BroadcastTimeout)
	defer t1.Stop()
	select {
	case <-everyoneSubmitted:
		return
	case <-t1.C:
		return
	}
}

// copied from the smartbft library...
// computeBFTQuorum calculates the quorums size Q, given a cluster size N.
//
// The calculation satisfies the following:
// Given a cluster size of N nodes, which tolerates f failures according to:
//
//	f = argmax ( N >= 3f+1 )
//
// Q is the size of the quorum such that:
//
//	any two subsets q1, q2 of size Q, intersect in at least f+1 nodes.
//
// Note that this is different from N-f (the number of correct nodes), when N=3f+3. That is, we have two extra nodes
// above the minimum required to tolerate f failures.
func computeBFTQuorum(N uint64) (Q int, F int) {
	F = (int(N) - 1) / 3
	Q = int(math.Ceil((float64(N) + float64(F) + 1) / 2.0))
	return
}

func (gs *Server) submitNonBFT(ctx context.Context, orderers []*orderer, txn *common.Envelope, logger *flogging.FabricLogger) (*gp.SubmitResponse, error) {
	// non-BFT - only need one successful response
	// try each orderer in random order
	logger.Infow("Sending transaction to orderer", "Count:", count)
	count++

	// 初始化 batch collector（lazy initialization）
	if gs.batchCollector == nil {
		batchSize := 200                // 批次大小：100 筆交易
		batchTimeout := 2 * time.Second // 超時：2s

		fmt.Printf("🚀 [Batch] 初始化批次收集器 (hash-only mode): size=%d, timeout=%v, transport=%s\n",
			batchSize, batchTimeout, gs.options.SequencerTransport)

		// 完整交易發送函數（用於向後兼容）
		var sendFunc func([]*common.Envelope) error
		// hash-only batch 發送函數（主要路徑）
		var sendHashFunc func([][]byte) error
		// 交易存儲函數（存入 TxnPool）
		var storeTxnFunc func([]byte, *common.Envelope) error

		if gs.options.SequencerTransport == config.TransportGRPC {
			sendFunc = gs.broadcastBatchByGRPC
			sendHashFunc = gs.broadcastHashBatchByGRPC
		} else {
			sendFunc = gs.broadcastBatchByUDP
			sendHashFunc = gs.broadcastHashBatchByUDP
		}

		// 設置交易存儲函數 - 使用 global TxnPool（與 GossipStateProvider 共享）
		// 注意：這裡的 channelID 需要從 request 中獲取
		storeTxnFunc = func(hash []byte, env *common.Envelope) error {
			// 嘗試從所有已知的 channel 獲取 TxnPool
			channelID := "mychannel" // 預設 channel，實際應從交易中提取
			fmt.Printf("💾 [Gateway] 嘗試存儲交易到 TxnPool (channel=%s, hash=%x...)\n",
				channelID, hash[:8])

			pool, exists := state.GetGlobalTxnPoolIfExists(channelID)
			if !exists {
				fmt.Printf("⚠️  [Gateway] TxnPool for channel %s NOT FOUND! 交易將無法被解析\n", channelID)
				return nil // 不阻塞，繼續處理
			}

			err := pool.Put(hash, env, state.DefaultTxnPoolTTL)
			if err != nil {
				fmt.Printf("❌ [Gateway] 存儲交易失敗: %v\n", err)
				return err
			}
			fmt.Printf("✅ [Gateway] 交易已存儲到 TxnPool (pool_size=%d)\n", pool.Size())

			// 🔥 通過 gossip 廣播交易給其他 peer
			gs.broadcastTxnViaGossip(channelID, hash, env)

			return nil
		}
		fmt.Printf("✅ [Batch] 交易存儲函數已設置，將使用 global TxnPool + Gossip 廣播\n")

		gs.batchCollector = NewSimpleBatchCollector(
			batchSize,
			batchTimeout,
			sendFunc,
			sendHashFunc,
			storeTxnFunc,
		)
	}
	// fmt.Printf("[lz debug] txn: %x\n", txn)
	// 使用批次收集器添加交易
	err := gs.batchCollector.Add(txn)
	if err != nil {
		logger.Warnw("Failed to add transaction to batch", "error", err)
		return nil, err
	}

	return &gp.SubmitResponse{}, nil
}

func (gs *Server) broadcast(ctx context.Context, orderer *orderer, txn *common.Envelope) (*ab.BroadcastResponse, error) {
	broadcast, err := orderer.client.Broadcast(ctx)
	if err != nil {
		return nil, err
	}

	if err := broadcast.Send(txn); err != nil {
		return nil, err
	}

	response, err := broadcast.Recv()
	if err != nil {
		return nil, err
	}

	return response, nil
}

// buildHashBatchPacket 構建 hash-only 批次封包
// 格式: [2B reserved][2B flag=0xFFFE][4B txnCount][N * 32B hash][4B seqNum reserved]
func (gs *Server) buildHashBatchPacket(hashes [][]byte) ([]byte, error) {
	if len(hashes) == 0 {
		return nil, fmt.Errorf("empty hash batch")
	}

	// 計算總包大小
	// 2 (front reserve) + 2 (batch flag) + 4 (txn count) + N*32 (hashes) + 4 (seq reserve)
	batchPacketSize := 2 + 2 + 4 + (len(hashes) * 32) + 4
	batchPacket := make([]byte, 0, batchPacketSize)

	// 1. 前置保留位 (2 bytes)
	batchPacket = append(batchPacket, 0x00, 0x00)

	// 2. Hash Batch 標記 (2 bytes): 0xFF 0xFE 表示這是一個 hash-only batch
	batchPacket = append(batchPacket, 0xFF, 0xFE)

	// 3. 交易數量 (4 bytes, big-endian)
	txnCount := uint32(len(hashes))
	batchPacket = append(batchPacket,
		byte(txnCount>>24),
		byte(txnCount>>16),
		byte(txnCount>>8),
		byte(txnCount))

	// 4. 依次添加每個 hash (32 bytes each)
	for _, hash := range hashes {
		if len(hash) != 32 {
			return nil, fmt.Errorf("invalid hash length: expected 32, got %d", len(hash))
		}
		batchPacket = append(batchPacket, hash...)
	}

	// 5. 後置 sequencer 預留位 (4 bytes) - sequencer 會填入 sequencer number
	batchPacket = append(batchPacket, 0x00, 0x00, 0x00, 0x00)

	return batchPacket, nil
}

// buildBatchPacket 構建批次封包（UDP 和 gRPC 共用）
func (gs *Server) buildBatchPacket(batch []*common.Envelope) ([]byte, error) {
	if len(batch) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// 1. 序列化所有交易
	txnDataList := make([][]byte, len(batch))
	totalDataSize := 0

	for i, txn := range batch {
		data, err := proto.Marshal(txn)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal transaction %d: %w", i, err)
		}
		txnDataList[i] = data
		totalDataSize += len(data)
	}

	// 2. 計算總包大小
	// 2 (front reserve) + 2 (batch flag) + 4 (txn count) + N*(4 + data_len) + 4 (seq reserve)
	batchPacketSize := 2 + 2 + 4 + (len(batch) * 4) + totalDataSize + 4
	batchPacket := make([]byte, 0, batchPacketSize)

	// 3. 前置保留位 (2 bytes)
	batchPacket = append(batchPacket, 0x00, 0x00)

	// 4. Batch 標記 (2 bytes): 0xFF 0xFF 表示這是一個 batch
	batchPacket = append(batchPacket, 0xFF, 0xFF)

	// 5. 交易數量 (4 bytes, big-endian)
	txnCount := uint32(len(batch))
	batchPacket = append(batchPacket,
		byte(txnCount>>24),
		byte(txnCount>>16),
		byte(txnCount>>8),
		byte(txnCount))

	// 6. 依次添加每個交易 (長度 + 數據)
	for _, txnData := range txnDataList {
		txnLen := uint32(len(txnData))

		// 交易長度 (4 bytes, big-endian)
		batchPacket = append(batchPacket,
			byte(txnLen>>24),
			byte(txnLen>>16),
			byte(txnLen>>8),
			byte(txnLen))

		// 交易數據
		batchPacket = append(batchPacket, txnData...)
	}

	// 7. 後置 sequencer 預留位 (4 bytes) - sequencer 會填入 sequencer number
	batchPacket = append(batchPacket, 0x00, 0x00, 0x00, 0x00)

	return batchPacket, nil
}

// broadcastHashBatchByUDP 批次發送 hash-only batch 到 sequencer (UDP)
func (gs *Server) broadcastHashBatchByUDP(hashes [][]byte) error {
	if len(hashes) == 0 {
		return fmt.Errorf("empty hash batch")
	}

	fmt.Printf("📤 [HashBatchUDP] Broadcasting hash batch of %d transactions\n", len(hashes))
	startTime := time.Now()

	// 構建 hash batch 封包
	batchPacket, err := gs.buildHashBatchPacket(hashes)
	if err != nil {
		return err
	}

	fmt.Printf("📊 [HashBatchUDP] Packet: %d bytes, %d hashes (32 bytes each)\n",
		len(batchPacket), len(hashes))

	// 發送 hash batch 包
	n, err := gs.UdpGateway.Write(batchPacket)
	if err == nil && n != len(batchPacket) {
		fmt.Printf("⚠️  [HashBatchUDP] 部分發送: 只發送了 %d/%d bytes\n", n, len(batchPacket))
	}
	if err != nil {
		// 嘗試重連
		if err := gs.reconnect(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// 重試發送
		_, err = gs.UdpGateway.Write(batchPacket)
		if err != nil {
			return fmt.Errorf("failed to resend hash batch after reconnecting: %w", err)
		}
	}

	elapsed := time.Since(startTime)
	throughput := float64(len(hashes)) / elapsed.Seconds()
	fmt.Printf("✅ [HashBatchUDP] Sent %d hashes in %v (%.0f hash/s)\n",
		len(hashes), elapsed, throughput)
	// gwBench.s2AddBatch(len(hashes), elapsed) // [BENCH] Stage2: batch→seq RTT

	return nil
}

// broadcastHashBatchByGRPC 批次發送 hash-only batch 到 sequencer (gRPC)
func (gs *Server) broadcastHashBatchByGRPC(hashes [][]byte) error {
	if len(hashes) == 0 {
		return fmt.Errorf("empty hash batch")
	}

	fmt.Printf("📤 [HashBatchGRPC] Broadcasting hash batch of %d transactions\n", len(hashes))
	startTime := time.Now()

	// 構建 hash batch 封包
	batchPacket, err := gs.buildHashBatchPacket(hashes)
	if err != nil {
		return err
	}

	fmt.Printf("📊 [HashBatchGRPC] Packet: %d bytes, %d hashes (32 bytes each)\n",
		len(batchPacket), len(hashes))

	// 檢查 gRPC 連接
	if gs.GrpcGateway == nil {
		fmt.Printf("⚠️  [HashBatchGRPC] gRPC 連接未建立，使用阻塞模式連接...\n")
		if err := gs.reconnectGRPC(); err != nil {
			return fmt.Errorf("failed to establish gRPC connection: %w", err)
		}
	} else {
		if conn, ok := gs.GrpcGateway.(*grpc.ClientConn); ok {
			state := conn.GetState()
			if state != connectivity.Ready {
				fmt.Printf("⚠️  [HashBatchGRPC] gRPC 連接狀態: %v，嘗試重新連接...\n", state)
				if err := gs.reconnectGRPC(); err != nil {
					return fmt.Errorf("failed to reconnect gRPC (state: %v): %w", state, err)
				}
			}
		}
	}

	// 創建 gRPC 客戶端
	client := sequencerpb.NewSequencerServiceClient(gs.GrpcGateway)

	// 創建帶超時的 context
	ctx, cancel := context.WithTimeout(context.Background(), gs.options.BroadcastTimeout)
	defer cancel()

	// 發送 hash batch 請求
	req := &sequencerpb.SubmitBatchRequest{
		BatchData: batchPacket,
	}

	fmt.Printf("🚀 [HashBatchGRPC] 發送到 sequencer: %s, packet size=%d bytes\n",
		gs.options.SequencerAddress, len(batchPacket))
	resp, err := client.SubmitBatch(ctx, req)
	if err != nil {
		// 嘗試重連
		if reconnErr := gs.reconnectGRPC(); reconnErr != nil {
			return fmt.Errorf("failed to reconnect: %w", reconnErr)
		}

		// 重試發送
		client = sequencerpb.NewSequencerServiceClient(gs.GrpcGateway)
		resp, err = client.SubmitBatch(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to submit hash batch via gRPC after reconnect: %w", err)
		}
	}

	// 檢查響應
	if !resp.Success {
		return fmt.Errorf("sequencer rejected hash batch: %s", resp.ErrorMessage)
	}

	elapsed := time.Since(startTime)
	throughput := float64(len(hashes)) / elapsed.Seconds()
	fmt.Printf("✅ [HashBatchGRPC] Sent %d hashes in %v (%.0f hash/s), seq=%d\n",
		len(hashes), elapsed, throughput, resp.SequenceNumber)
	// gwBench.s2AddBatch(len(hashes), elapsed) // [BENCH] Stage2: batch→seq RTT

	return nil
}

// broadcastBatchByUDP 批次發送交易到 sequencer (UDP) - 保留用於向後兼容
func (gs *Server) broadcastBatchByUDP(batch []*common.Envelope) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	fmt.Printf("📤 [BatchUDP] Broadcasting batch of %d transactions\n", len(batch))
	startTime := time.Now()

	// 構建批次封包
	batchPacket, err := gs.buildBatchPacket(batch)
	if err != nil {
		return err
	}

	fmt.Printf("📊 [BatchUDP] Packet: %d bytes, %d txns, avg %d bytes/txn\n",
		len(batchPacket), len(batch), len(batchPacket)/len(batch))

	// ⚠️ UDP 大封包警告
	if len(batchPacket) > 1472 {
		fmt.Printf("⚠️  [BatchUDP] 警告: 封包大小 %d bytes 超過 UDP MTU (1472 bytes)，可能會分片或丟失！\n", len(batchPacket))
	}

	// 8. 發送批次包
	n, err := gs.UdpGateway.Write(batchPacket)
	if err == nil && n != len(batchPacket) {
		fmt.Printf("⚠️  [BatchUDP] 部分發送: 只發送了 %d/%d bytes\n", n, len(batchPacket))
	}
	if err != nil {
		// 嘗試重連
		if err := gs.reconnect(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// 重試發送
		_, err = gs.UdpGateway.Write(batchPacket)
		if err != nil {
			return fmt.Errorf("failed to resend batch after reconnecting: %w", err)
		}
	}

	elapsed := time.Since(startTime)
	throughput := float64(len(batch)) / elapsed.Seconds()
	fmt.Printf("✅ [BatchUDP] Sent %d txns in %v (%.0f txn/s)\n",
		len(batch), elapsed, throughput)

	return nil
}

// broadcastBatchByGRPC 批次發送交易到 sequencer (gRPC)
func (gs *Server) broadcastBatchByGRPC(batch []*common.Envelope) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	fmt.Printf("📤 [BatchGRPC] Broadcasting batch of %d transactions\n", len(batch))
	startTime := time.Now()

	// 構建批次封包
	batchPacket, err := gs.buildBatchPacket(batch)
	if err != nil {
		return err
	}

	fmt.Printf("📊 [BatchGRPC] Packet: %d bytes, %d txns, avg %d bytes/txn\n",
		len(batchPacket), len(batch), len(batchPacket)/len(batch))

	// 檢查 gRPC 連接，如果未連接或連接未就緒則嘗試連接
	if gs.GrpcGateway == nil {
		fmt.Printf("⚠️  [BatchGRPC] gRPC 連接未建立，使用阻塞模式連接...\n")
		if err := gs.reconnectGRPC(); err != nil {
			return fmt.Errorf("failed to establish gRPC connection: %w", err)
		}
	} else {
		// 檢查連接狀態（非阻塞模式可能返回連接對象但連接還沒建立）
		if conn, ok := gs.GrpcGateway.(*grpc.ClientConn); ok {
			state := conn.GetState()
			fmt.Printf("🔍 [BatchGRPC] 當前 gRPC 連接狀態: %v\n", state)
			if state != connectivity.Ready {
				fmt.Printf("⚠️  [BatchGRPC] gRPC 連接狀態: %v，嘗試重新連接...\n", state)
				if err := gs.reconnectGRPC(); err != nil {
					return fmt.Errorf("failed to reconnect gRPC (state: %v): %w", state, err)
				}
			} else {
				fmt.Printf("✅ [BatchGRPC] gRPC 連接已就緒 (state: Ready)\n")
			}
		} else {
			fmt.Printf("⚠️  [BatchGRPC] gRPC 連接類型錯誤，強制重新連接...\n")
			gs.GrpcGateway = nil
			if err := gs.reconnectGRPC(); err != nil {
				return fmt.Errorf("failed to reconnect gRPC: %w", err)
			}
		}
	}

	// 創建 gRPC 客戶端
	client := sequencerpb.NewSequencerServiceClient(gs.GrpcGateway)

	// 創建帶超時的 context
	ctx, cancel := context.WithTimeout(context.Background(), gs.options.BroadcastTimeout)
	defer cancel()

	// 發送批次請求
	req := &sequencerpb.SubmitBatchRequest{
		BatchData: batchPacket,
	}

	fmt.Printf("🚀 [BatchGRPC] 發送到 sequencer: %s, packet size=%d bytes\n", gs.options.SequencerAddress, len(batchPacket))
	resp, err := client.SubmitBatch(ctx, req)
	if err != nil {
		// 嘗試重連
		if err := gs.reconnectGRPC(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// 重試發送
		client = sequencerpb.NewSequencerServiceClient(gs.GrpcGateway)
		resp, err = client.SubmitBatch(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to submit batch via gRPC after reconnect: %w", err)
		}
	}

	// 檢查響應
	if !resp.Success {
		return fmt.Errorf("sequencer rejected batch: %s", resp.ErrorMessage)
	}

	elapsed := time.Since(startTime)
	throughput := float64(len(batch)) / elapsed.Seconds()
	fmt.Printf("✅ [BatchGRPC] Sent %d txns in %v (%.0f txn/s), seq=%d\n",
		len(batch), elapsed, throughput, resp.SequenceNumber)

	return nil
}

func (gs *Server) broadcastByUDP(txn *common.Envelope) error {
	fmt.Printf("[lz debug] broadcastByUDP\n")
	data, err := proto.Marshal(txn)
	if err != nil {
		fmt.Println("Failed to marshal envelope:", err)
		return err
	}

	seqBytes := []byte{0x00, 0x00, 0x00, 0x00} // The extra bytes you want to add
	dataWithseqBytes := append(data, seqBytes...)
	frontResver := []byte{0x00, 0x00}
	newType := append(frontResver, dataWithseqBytes...)

	_, err = gs.UdpGateway.Write(newType)
	if err != nil {
		// Attempt to reconnect
		if err := gs.reconnect(); err != nil {
			return fmt.Errorf("failed to reconnect: %w", err)
		}

		// Retry sending the message after reconnecting
		_, err = gs.UdpGateway.Write(newType)
		if err != nil {
			return fmt.Errorf("failed to resend message after reconnecting: %w", err)
		}
	}

	return nil
}

func prepareTransaction(header *common.Header, payload *peer.ChaincodeProposalPayload, action *peer.ChaincodeEndorsedAction) (*common.Envelope, error) {
	cppNoTransient := &peer.ChaincodeProposalPayload{Input: payload.Input, TransientMap: nil}
	cppBytes, err := protoutil.GetBytesChaincodeProposalPayload(cppNoTransient)
	if err != nil {
		return nil, err
	}

	cap := &peer.ChaincodeActionPayload{ChaincodeProposalPayload: cppBytes, Action: action}
	capBytes, err := protoutil.GetBytesChaincodeActionPayload(cap)
	if err != nil {
		return nil, err
	}

	tx := &peer.Transaction{Actions: []*peer.TransactionAction{{Header: header.SignatureHeader, Payload: capBytes}}}
	txBytes, err := protoutil.GetBytesTransaction(tx)
	if err != nil {
		return nil, err
	}

	payl := &common.Payload{Header: header, Data: txBytes}
	paylBytes, err := protoutil.GetBytesPayload(payl)
	if err != nil {
		return nil, err
	}

	return &common.Envelope{Payload: paylBytes}, nil
}

// TxnBroadcastMagic 是用於標記交易廣播消息的魔數
// 0x54584E45 = "TXNE" (Transaction Envelope)
const TxnBroadcastMagic uint32 = 0x54584E45

// broadcastTxnViaGossip 通過 gossip 廣播交易給其他 peer
// 這確保所有 peer 都有交易的完整數據，用於 hash-only block 模式
func (gs *Server) broadcastTxnViaGossip(channelID string, hash []byte, env *common.Envelope) {
	if gs.gossipService == nil {
		fmt.Printf("⚠️  [Gateway Gossip] gossipService 未初始化，跳過廣播\n")
		return
	}

	// 序列化交易
	envBytes, err := proto.Marshal(env)
	if err != nil {
		fmt.Printf("❌ [Gateway Gossip] 序列化交易失敗: %v\n", err)
		return
	}

	// 構建 gossip 消息
	// 使用 DataMessage 類型，將交易數據放在 Payload 中
	// SeqNum 使用一個特殊的高位值來標識這是交易廣播（不會與區塊編號衝突）
	// 區塊編號通常是從 0 開始的小數字，我們使用 0xFFFFFFFF00000000 + hash 前 4 bytes
	seqNum := uint64(0xFFFFFFFF00000000) |
		uint64(hash[0])<<24 | uint64(hash[1])<<16 | uint64(hash[2])<<8 | uint64(hash[3])

	// 構建自定義的 payload，包含魔數、hash 和交易數據
	// 格式: [4B magic=0x54584E45][32B hash][envelope bytes]
	payloadData := make([]byte, 4+32+len(envBytes))
	// 寫入魔數（大端序）
	binary.BigEndian.PutUint32(payloadData[0:4], TxnBroadcastMagic)
	// 寫入 hash
	copy(payloadData[4:36], hash)
	// 寫入 envelope
	copy(payloadData[36:], envBytes)

	gossipMsg := &gproto.GossipMessage{
		Channel: []byte(channelID),
		Tag:     gproto.GossipMessage_CHAN_AND_ORG, // 在 channel 和 org 範圍內廣播
		Content: &gproto.GossipMessage_DataMsg{
			DataMsg: &gproto.DataMessage{
				Payload: &gproto.Payload{
					SeqNum: seqNum,
					Data:   payloadData,
				},
			},
		},
	}

	// 廣播消息
	gs.gossipService.Gossip(gossipMsg)
	fmt.Printf("📡 [Gateway Gossip] 廣播交易 hash=%x... (channel=%s, size=%d)\n",
		hash[:8], channelID, len(envBytes))
}
