/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	gp "github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
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
	batchSize     int
	timeout       time.Duration
	timer         *time.Timer
	sendFunc      func([]*common.Envelope) error
	batchCount    uint64
	totalTxnCount uint64
}

// NewSimpleBatchCollector 創建簡單批次收集器
func NewSimpleBatchCollector(batchSize int, timeout time.Duration, sendFunc func([]*common.Envelope) error) *SimpleBatchCollector {
	return &SimpleBatchCollector{
		buffer:    make([]*common.Envelope, 0, batchSize),
		batchSize: batchSize,
		timeout:   timeout,
		sendFunc:  sendFunc,
	}
}

// Add 添加交易到批次
func (sbc *SimpleBatchCollector) Add(txn *common.Envelope) error {
	sbc.mu.Lock()
	defer sbc.mu.Unlock()

	// 添加到緩衝區
	sbc.buffer = append(sbc.buffer, txn)
	sbc.totalTxnCount++

	// 如果是第一筆交易，啟動計時器
	if len(sbc.buffer) == 1 {
		fmt.Printf("⏳ [Batch] 第一筆交易加入，啟動 %v 超時計時器 (total=%d)\n", sbc.timeout, sbc.totalTxnCount)
		sbc.timer = time.AfterFunc(sbc.timeout, func() {
			sbc.mu.Lock()
			defer sbc.mu.Unlock()
			if len(sbc.buffer) > 0 {
				fmt.Printf("⏰ [Batch] Timeout triggered, flushing %d txns\n", len(sbc.buffer))
				sbc.flushLocked()
			}
		})
	} else {
		fmt.Printf("📝 [Batch] 交易加入 batch: buffer size=%d/%d, total=%d\n", len(sbc.buffer), sbc.batchSize, sbc.totalTxnCount)
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

	// 複製批次
	batch := make([]*common.Envelope, len(sbc.buffer))
	copy(batch, sbc.buffer)

	// 清空緩衝區
	sbc.buffer = sbc.buffer[:0]
	sbc.batchCount++

	fmt.Printf("📦 [Batch] Flushing batch #%d with %d txns\n", sbc.batchCount, len(batch))

	// 發送批次
	return sbc.sendFunc(batch)
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
		batchSize := 100                       // 批次大小：100 筆交易
		batchTimeout := 200 * time.Millisecond // 超時：20ms

		fmt.Printf("🚀 [Batch] 初始化批次收集器: size=%d, timeout=%v, transport=%s\n",
			batchSize, batchTimeout, gs.options.SequencerTransport)

		var sendFunc func([]*common.Envelope) error
		if gs.options.SequencerTransport == config.TransportGRPC {
			sendFunc = gs.broadcastBatchByGRPC
		} else {
			sendFunc = gs.broadcastBatchByUDP
		}

		gs.batchCollector = NewSimpleBatchCollector(
			batchSize,
			batchTimeout,
			sendFunc,
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

// broadcastBatchByUDP 批次發送交易到 sequencer (UDP)
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
