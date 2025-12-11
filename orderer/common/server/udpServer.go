package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/common/multichannel"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos"
	"google.golang.org/protobuf/proto"
)

type UdpServer struct {
	host string
	port uint16
	*multichannel.Registrar
	exitChanUDP chan struct{}
}

func NewUDPServer(
	_host string,
	_port uint16,
	r *multichannel.Registrar,
) *UdpServer {
	return &UdpServer{host: _host, port: _port, exitChanUDP: make(chan struct{}), Registrar: r}
}

func (us *UdpServer) Start() error {
	address := net.JoinHostPort("", strconv.Itoa(int(us.port)))
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return err
	}

	// 🔥 設置更大的 socket 接收緩衝區（預設可能只有幾百 KB）
	// 這可以防止在處理慢時丟包
	if err := conn.SetReadBuffer(64 * 1024 * 1024); err != nil { // 64MB
		fmt.Printf("⚠️  [UDP Server] 設置接收緩衝區失敗: %v\n", err)
	} else {
		fmt.Printf("✅ [UDP Server] 已設置 64MB 接收緩衝區\n")
	}

	fmt.Printf("✅ [UDP Server] 成功啟動在端口 %d\n", us.port)
	fmt.Printf("✅ [UDP Server] 等待接收數據...\n")

	buffer := make([]byte, 10240)
	lastLogTime := time.Now()
	receiveCount := 0
	processedCount := 0
	droppedCount := 0

	for {
		select {
		case <-us.exitChanUDP:
			conn.Close()
			fmt.Printf("🚨 [UDP Server Port %d] 收到退出信號\n", us.port)
			return nil
		default:
			// 🔥 添加 read timeout，避免 ReadFromUDP 永遠阻塞
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

			// Read UDP data
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				// timeout 錯誤忽略，繼續下一次讀取
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				fmt.Printf("❌ [UDP Server Port %d] ReadFromUDP 錯誤: %v\n", us.port, err)
				continue
			}

			receiveCount++

			// 🔥 每 10 秒打印詳細統計
			if time.Since(lastLogTime) > 10*time.Second {
				successRate := float64(processedCount) / float64(receiveCount) * 100
				fmt.Printf("\n📊 ========== UDP Server 統計 (Port %d) ==========\n", us.port)
				fmt.Printf("   接收: %d 個包\n", receiveCount)
				fmt.Printf("   處理成功: %d 個\n", processedCount)
				fmt.Printf("   丟棄: %d 個\n", droppedCount)
				fmt.Printf("   成功率: %.2f%%\n", successRate)
				fmt.Printf("================================================\n\n")
				receiveCount = 0
				processedCount = 0
				droppedCount = 0
				lastLogTime = time.Now()
			}

			fmt.Printf("📥 [UDP Server Port %d] 收到數據 #%d: %d bytes from %s\n", us.port, receiveCount, n, remoteAddr)

			// Extract the extra bytes from the tail
			if n < 2 {
				fmt.Println("Not enough data received")
				continue
			}

			// ================================================================
			// 🔍 檢測是否為批次包並印出信息 (orderer 端收到的 batch 地方)
			// ================================================================

			batchFlag := uint16(buffer[2])<<8 | uint16(buffer[3])
			if batchFlag == 0xFFFF {
				// 這是批次包！
				fmt.Printf("🔍 [UDP Server Port %d] *** 檢測到批次包！***\n", us.port)
				fmt.Printf("   ├─ 前置保留位: 0x%02X 0x%02X\n", buffer[0], buffer[1])
				fmt.Printf("   ├─ Batch 標記: 0x%02X 0x%02X (0xFFFF)\n", buffer[2], buffer[3])

				// 讀取交易數量（需要至少 8 bytes：4 bytes batch flag + 4 bytes txn count）
				if n >= 8 {
					txnCount := binary.BigEndian.Uint32(buffer[4:8])
					fmt.Printf("   ├─ 交易數量: %d 筆\n", txnCount)
					fmt.Printf("   ├─ 包大小: %d bytes\n", n)

					// 讀取最後 4 bytes 的 sequencer number
					// (已經確保 n >= 8，所以一定有最後 4 bytes)
					extraBytes := buffer[n-4 : n]
					paddedBytes := make([]byte, 8)
					copy(paddedBytes[:8-len(extraBytes)], extraBytes)
					var seqNum uint64
					binary.Read(bytes.NewReader(paddedBytes), binary.LittleEndian, &seqNum)
					fmt.Printf("   └─ Sequencer Number: %d\n", seqNum)
					fmt.Printf("   └─ buffer[4:8]: %x\n", buffer[4:8])
				}
			} else {
				fmt.Printf("📝 [UDP Server Port %d] 單筆交易\n", us.port)
			}
			fmt.Printf("   └─ buffer[4:8]: %x\n", buffer[4:8])
			// ================================================================

			extraBytes := buffer[n-4 : n] // The last 2 bytes are the extra bytes
			fmt.Printf("Received extra bytes: %x\n", extraBytes)

			paddedBytes := make([]byte, 8)
			copy(paddedBytes[:8-len(extraBytes)], extraBytes)
			var bigEndianValue uint64
			err = binary.Read(bytes.NewReader(paddedBytes), binary.LittleEndian, &bigEndianValue)
			if err != nil {
				fmt.Println("Error decoding Big Endian value:", err)
			}
			fmt.Printf("Big Endian interpreted value (uint64): %d (0x%x)\n", bigEndianValue, bigEndianValue)
			// fmt.Printf("buffer[2:n-4]: %x\n", buffer[2:n-4])

			// [0-1]   reserved (2 bytes)
			// [2-3]   flag (2 bytes) 0xFFFF = batch
			// [4-7]   txnCount (uint32)
			// ------------------------------------
			// Loop txnCount 次:
			// 	[len]   txnLen (uint32)
			// 	[data]  txnData (bytes)
			// ------------------------------------
			// [last 4 bytes] sequencer reserve

			// 🔥 處理 batch 情況
			if batchFlag == 0xFFFF {
				// 這是批次包，解析 batch
				if n < 8 {
					fmt.Printf("❌ [UDP Server Port %d] Batch 包太小，無法解析\n", us.port)
					droppedCount++
					continue
				}

				txnCount := binary.BigEndian.Uint32(buffer[4:8])
				if txnCount == 0 {
					fmt.Printf("❌ [UDP Server Port %d] Batch 交易數量為 0\n", us.port)
					droppedCount++
					continue
				}

				fmt.Printf("📦 [UDP Server Port %d] 開始解析 batch (交易數量: %d, sequencer: %d)\n", us.port, txnCount, bigEndianValue)

				// 解析 batch 中的每個交易
				batch := make([]*common.Envelope, 0, txnCount)
				offset := 8 // 從 offset 8 開始（跳過前 8 bytes: 2 reserved + 2 flag + 4 txnCount）

				for i := uint32(0); i < txnCount; i++ {
					// 讀取交易長度
					if offset+4 > n-4 {
						fmt.Printf("❌ [UDP Server Port %d] Batch 解析錯誤：交易 %d 長度超出範圍\n", us.port, i)
						droppedCount++
						goto nextPacket
					}

					txnLen := binary.BigEndian.Uint32(buffer[offset : offset+4])
					offset += 4

					// 讀取交易數據
					if offset+int(txnLen) > n-4 {
						fmt.Printf("❌ [UDP Server Port %d] Batch 解析錯誤：交易 %d 數據超出範圍\n", us.port, i)
						droppedCount++
						goto nextPacket
					}

					envelope := &common.Envelope{}
					err = proto.Unmarshal(buffer[offset:offset+int(txnLen)], envelope)
					if err != nil {
						fmt.Printf("❌ [UDP Server Port %d] Batch 解析錯誤：無法解碼交易 %d: %v\n", us.port, i, err)
						droppedCount++
						goto nextPacket
					}

					batch = append(batch, envelope)
					offset += int(txnLen)
				}

				// 驗證解析是否正確（offset 應該等於 n-4，即最後 4 bytes 是 sequencer number）
				if offset != n-4 {
					fmt.Printf("⚠️  [UDP Server Port %d] Batch 解析警告：offset (%d) != n-4 (%d)\n", us.port, offset, n-4)
				}

				// 使用第一個交易獲取 channel support（假設 batch 中所有交易都是同一個 channel）
				if len(batch) == 0 {
					fmt.Printf("❌ [UDP Server Port %d] Batch 為空\n", us.port)
					droppedCount++
					continue
				}

				chdr, isConfig, processor, err := us.BroadcastChannelSupport(batch[0])
				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] BroadcastChannelSupport 失敗: %v\n", us.port, err)
					droppedCount++
					continue
				}

				if isConfig {
					fmt.Printf("❌ [UDP Server Port %d] Batch 中包含 config 交易，暫不支持\n", us.port)
					droppedCount++
					continue
				}

				// 處理 batch：驗證每個交易
				startTime := time.Now()
				configSeq, err := processor.ProcessNormalMsg(batch[0]) // 使用第一個交易獲取 configSeq
				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] ProcessNormalMsg 失敗: %v\n", us.port, err)
					droppedCount++
					continue
				}

				// 驗證 batch 中的其他交易
				for i := 1; i < len(batch); i++ {
					_, err = processor.ProcessNormalMsg(batch[i])
					if err != nil {
						fmt.Printf("❌ [UDP Server Port %d] Batch[%d] ProcessNormalMsg 失敗: %v\n", us.port, i, err)
						droppedCount++
						goto nextPacket
					}
				}

				if err = processor.WaitReady(); err != nil {
					fmt.Printf("❌ [UDP Server Port %d] WaitReady 失敗: %v\n", us.port, err)
					droppedCount++
					continue
				}

				// 🔥 獲取 chain 並調用 OrderBatch

				chain := us.GetConsensusChain(chdr.ChannelId)
				if chain == nil {
					fmt.Printf("❌ [UDP Server Port %d] 無法獲取 chain (channel: %s)\n", us.port, chdr.ChannelId)
					droppedCount++
					continue
				}

				orderStart := time.Now()
				err = nopaxos.OrderBatchForChain(chain, batch, configSeq, bigEndianValue)
				orderDuration := time.Since(orderStart)

				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] OrderBatch 失敗: %v (Order耗時: %v, 總耗時: %v)\n",
						us.port, err, orderDuration, time.Since(startTime))
					droppedCount++
					continue
				}

				processedCount++
				fmt.Printf("✅ [UDP Server Port %d] Batch 處理成功 (sequencer: %d, batch size: %d, 總耗時: %v)\n",
					us.port, bigEndianValue, len(batch), time.Since(startTime))
				continue
			}

		nextPacket:
			// 單筆交易處理（保持原有邏輯）
			envelope := &common.Envelope{}
			err = proto.Unmarshal(buffer[2:n-4], envelope)
			if err != nil {
				fmt.Println("Failed to unmarshal envelope:", err)
				continue
			}

			_, isConfig, processor, err := us.BroadcastChannelSupport(envelope)
			if err != nil {
				fmt.Printf("❌ [UDP Server Port %d] BroadcastChannelSupport 失敗: %v\n", us.port, err)
				droppedCount++
				continue
			}

			if !isConfig {
				startTime := time.Now()
				fmt.Printf("✅ [UDP Server Port %d] 開始處理普通交易 (sequencer: %d)\n", us.port, bigEndianValue)

				configSeq, err := processor.ProcessNormalMsg(envelope)
				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] ProcessNormalMsg 失敗: %v (耗時: %v)\n", us.port, err, time.Since(startTime))
					droppedCount++
					continue
				}

				if err = processor.WaitReady(); err != nil {
					fmt.Printf("❌ [UDP Server Port %d] WaitReady 失敗: %v (耗時: %v)\n", us.port, err, time.Since(startTime))
					droppedCount++
					continue
				}

				orderStart := time.Now()
				err = processor.Order(envelope, configSeq, 1, bigEndianValue)
				orderDuration := time.Since(orderStart)

				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] Order 失敗: %v (Order耗時: %v, 總耗時: %v)\n",
						us.port, err, orderDuration, time.Since(startTime))
					droppedCount++
					continue
				}

				if orderDuration > time.Second {
					fmt.Printf("⚠️  [UDP Server Port %d] Order 調用耗時過長: %v (可能阻塞)\n", us.port, orderDuration)
				}

				processedCount++
				fmt.Printf("✅ [UDP Server Port %d] 交易處理成功 (sequencer: %d, 總耗時: %v)\n",
					us.port, bigEndianValue, time.Since(startTime))
			} else { // isConfig
				config, configSeq, err := processor.ProcessConfigUpdateMsg(envelope)
				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] ProcessConfigUpdateMsg 失敗: %v\n", us.port, err)
					continue
				}

				if err = processor.WaitReady(); err != nil {
					fmt.Printf("❌ [UDP Server Port %d] WaitReady 失敗: %v\n", us.port, err)
					continue
				}

				err = processor.Configure(config, configSeq)
				if err != nil {
					fmt.Printf("❌ [UDP Server Port %d] Configure 失敗: %v\n", us.port, err)
					continue
				}
			}
		}
	}
}

func (s *UdpServer) Close() {
	// Implementation to close the UDP server
	fmt.Println("Closing UDP server on", s.host, ":", s.port)
	// Logic to clean up resources goes here
	close(s.exitChanUDP) // Signal server exit, if needed
}
