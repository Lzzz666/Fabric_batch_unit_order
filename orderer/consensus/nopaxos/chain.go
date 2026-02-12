package nopaxos

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	pb "github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	nopaxosConfig "github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/protocol"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

var logger = flogging.MustGetLogger("orderer.consensus.nopaxos")

type consenter struct {
	config *localconfig.TopLevel
}

type chain struct {
	support       consensus.ConsenterSupport
	sendChan      chan *message
	deliverChan   chan []*message
	exitChan      chan struct{}
	consenters    []*etcdraft.Consenter
	NopaxosServer *Server
	Count         uint64
	batch         []*cb.Envelope
	isInit        bool
	sessionNumber protocol.SessionID
	peerEndpoint  string // peer 的地址，用於同步區塊
	useTLS        bool   // 是否使用 TLS

	// View Change 同步控制
	syncInProgress chan struct{} // 關閉表示同步完成
	syncCompleted  bool          // 標記是否已完成初始同步
	syncMutex      sync.Mutex    // 保護 peer sync 與正常區塊創建的互斥鎖
	isSyncing      bool          // 標記是否正在進行區塊同步
	isSyncingMutex sync.RWMutex  // 保護 isSyncing 標志

	// 🔥 Deadlock 檢測：main goroutine 心跳
	mainHeartbeat     time.Time
	mainHeartbeatLock sync.RWMutex
}

type message struct {
	configSeq       uint64
	sequencerNumber uint64
	sessionNumber   protocol.SessionID
	normalMsg       *cb.Envelope
	configMsg       *cb.Envelope
	batch           []*cb.Envelope // 🔥 當 batch != nil 時，表示這是一個 batch，直接使用它創建區塊
	hashBatch       [][]byte       // 🔥 當 hashBatch != nil 時，表示這是一個 hash-only batch
}

type batchMessage struct {
	batch           []*cb.Envelope
	sessionNumber   protocol.SessionID
	sequencerNumber uint64
	configSeq       uint64
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New(config *localconfig.TopLevel) consensus.Consenter {
	return &consenter{
		config: config,
	}
}

func (nps *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	m := &etcdraft.ConfigMetadata{}
	if err := proto.Unmarshal(support.SharedConfig().ConsensusMetadata(), m); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	return newChain(support, m.Consenters, nps.config), nil
}

func (nps *consenter) IsChannelMember(joinBlock *cb.Block) (bool, error) {
	return true, nil
}

func newChain(support consensus.ConsenterSupport, consenters []*etcdraft.Consenter, config *localconfig.TopLevel) *chain {

	nopaxosServerConfig := &nopaxosConfig.ProtocolConfig{}

	// 從環境變數讀取節點 ID，這必須與 consenters 中的 Host 匹配
	nodeID := os.Getenv("NOPAXOS_NODE_ID")
	if nodeID == "" {
		// 如果沒有設置環境變數，嘗試從 config.General.ListenPort 推斷
		// 這是一個 fallback，建議明確設置 NOPAXOS_NODE_ID
		logger.Warning("NOPAXOS_NODE_ID 環境變數未設置，嘗試從 ListenPort 推斷節點身份")
		listenPort := config.General.ListenPort
		for _, consenter := range consenters {
			if int(consenter.GetPort()) == int(listenPort) {
				nodeID = consenter.GetHost()
				logger.Infof("根據 ListenPort %d 推斷節點 ID: %s", listenPort, nodeID)
				break
			}
		}
	}

	if nodeID == "" {
		panic("無法確定節點身份！請設置 NOPAXOS_NODE_ID 環境變數，例如: NOPAXOS_NODE_ID=orderer1.example.com")
	}

	logger.Infof("NOPaxos 節點 ID: %s", nodeID)

	members := make(map[string]protocol.Member)
	for _, consenter := range consenters {
		if consenter.GetHost() != "orderer4.example.com" {
			members[consenter.GetHost()] = protocol.Member{
				ID:           consenter.GetHost(),
				Host:         consenter.GetHost(),
				APIPort:      int(consenter.GetPort()) + 35,
				ProtocolPort: int(consenter.GetPort()) + 36,
			}
		}
	}

	cluster := protocol.NodeCluster{
		MemberID: nodeID,
		Members:  members,
	}

	deliverChan := make(chan []struct{})

	// TODO: 從配置文件讀取 peer endpoint，這裡先硬編碼
	peerEndpoint := "peer1.org1.example.com:13051"

	// 檢查是否使用 TLS（從環境變量或配置讀取）
	useTLS := true // 預設使用 TLS
	if tlsDisabled := os.Getenv("PEER_SYNC_DISABLE_TLS"); tlsDisabled == "true" {
		useTLS = false
		logger.Warning("⚠️  PEER_SYNC_DISABLE_TLS=true，將使用非 TLS 連接")
	}

	ch := &chain{
		support: support,
		// sendChan:    make(chan *message),
		sendChan:    make(chan *message, 10000), // 🔥 添加緩衝，防止 Order() 阻塞
		exitChan:    make(chan struct{}),
		deliverChan: make(chan []*message),
		consenters:  consenters,
		NopaxosServer: NewNodeServer(
			cluster,
			nopaxosServerConfig,
			deliverChan,
		),
		Count:         1,
		batch:         make([]*cb.Envelope, 0),
		sessionNumber: 1,
		isInit:        true,
		peerEndpoint:  peerEndpoint,
		useTLS:        useTLS,
		mainHeartbeat: time.Now(), // 🔥 初始化心跳
	}

	// 設置 View Change 模式
	// 從環境變量讀取，若未設置則默認使用簡化版（true）
	useSimplifiedViewChange := true // 預設使用簡化版
	if mode := os.Getenv("NOPAXOS_VIEW_CHANGE_MODE"); mode == "full" {
		useSimplifiedViewChange = false
		logger.Info("✓ 使用完整版 View Change（含 Log Repair）")
	} else {
		logger.Info("✓ 使用簡化版 View Change（依賴 Peer Sync）")
	}
	ch.NopaxosServer.nopaxos.SetSimplifiedViewChange(useSimplifiedViewChange)

	return ch
}

func (ch *chain) Start() {
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// OrderBatch 接收 batch 並發送到 sendChan（用於 UDP server 直接發送 batch）
func (ch *chain) OrderBatch(batch []*cb.Envelope, configSeq uint64, sequencerNumber uint64) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}

	// 🔥 只有 leader 才能處理 batch 排序
	if !ch.NopaxosServer.nopaxos.IsLeader() {
		fmt.Printf("[OrderBatch] ⏭️  非 leader 節點，跳過 batch 處理 (sequencer: %d, batch size: %d)\n",
			sequencerNumber, len(batch))
		return nil
	}

	// 🔥 使用 non-blocking send 避免永遠阻塞
	select {
	case ch.sendChan <- &message{
		sequencerNumber: sequencerNumber,
		sessionNumber:   ch.sessionNumber,
		configSeq:       configSeq,
		batch:           batch, // 🔥 設置 batch 字段
	}:
		fmt.Printf("[OrderBatch] ✓ 成功發送 batch 到 sendChan (sequencer: %d, batch size: %d)\n", sequencerNumber, len(batch))
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	default:
		fmt.Printf("❌ [OrderBatch] sendChan 已滿，丟棄 batch (sequencer: %d, batch size: %d)\n", sequencerNumber, len(batch))
		return fmt.Errorf("sendChan full, dropping batch")
	}
}

// OrderHashBatch 接收 hash batch 並發送到 sendChan（用於 hash-only 模式）
func (ch *chain) OrderHashBatch(hashes [][]byte, configSeq uint64, sequencerNumber uint64) error {
	if len(hashes) == 0 {
		return fmt.Errorf("empty hash batch")
	}

	// 🔥 只有 leader 才能處理 hash batch 排序
	if !ch.NopaxosServer.nopaxos.IsLeader() {
		fmt.Printf("[OrderHashBatch] ⏭️  非 leader 節點，跳過 hash batch 處理 (sequencer: %d, hash count: %d)\n",
			sequencerNumber, len(hashes))
		return nil
	}

	// 🔥 使用 non-blocking send 避免永遠阻塞
	select {
	case ch.sendChan <- &message{
		sequencerNumber: sequencerNumber,
		sessionNumber:   ch.sessionNumber,
		configSeq:       configSeq,
		hashBatch:       hashes, // 🔥 設置 hashBatch 字段
	}:
		fmt.Printf("[OrderHashBatch] ✓ 成功發送 hash batch 到 sendChan (sequencer: %d, hash count: %d)\n",
			sequencerNumber, len(hashes))
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	default:
		fmt.Printf("❌ [OrderHashBatch] sendChan 已滿，丟棄 hash batch (sequencer: %d, hash count: %d)\n",
			sequencerNumber, len(hashes))
		return fmt.Errorf("sendChan full, dropping hash batch")
	}
}

// BatchOrderer 接口用於處理 batch
type BatchOrderer interface {
	OrderBatch(batch []*cb.Envelope, configSeq uint64, sequencerNumber uint64) error
}

// HashBatchOrderer 接口用於處理 hash-only batch
type HashBatchOrderer interface {
	OrderHashBatch(hashes [][]byte, configSeq uint64, sequencerNumber uint64) error
}

// OrderBatchForChain 是導出函數，用於從外部調用 OrderBatch（避免直接訪問私有類型）
func OrderBatchForChain(chain consensus.Chain, batch []*cb.Envelope, configSeq uint64, sequencerNumber uint64) error {
	// 類型斷言到 BatchOrderer 接口
	batchOrderer, ok := chain.(BatchOrderer)
	if !ok {
		return fmt.Errorf("chain does not support batch ordering")
	}

	// 🔥 只有 leader 才能處理 batch 排序
	// 使用類型斷言訪問私有字段（通過接口無法直接訪問，需要在 OrderBatch 內部檢查）
	// 這裡先調用 OrderBatch，讓它在內部檢查 leader
	return batchOrderer.OrderBatch(batch, configSeq, sequencerNumber)
}

// OrderHashBatchForChain 是導出函數，用於從外部調用 OrderHashBatch（處理 hash-only batch）
func OrderHashBatchForChain(chain consensus.Chain, hashes [][]byte, configSeq uint64, sequencerNumber uint64) error {
	// 類型斷言到 HashBatchOrderer 接口
	hashBatchOrderer, ok := chain.(HashBatchOrderer)
	if !ok {
		return fmt.Errorf("chain does not support hash batch ordering")
	}

	// 調用 OrderHashBatch，讓它在內部檢查 leader
	return hashBatchOrderer.OrderHashBatch(hashes, configSeq, sequencerNumber)
}

// Order accepts normal messages for ordering
// 排序是排 envelope 的
func (ch *chain) Order(env *cb.Envelope, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
	fmt.Println("=======TESTTEST=======")
	fmt.Println(ch.sessionNumber, sequencerNumber)
	fmt.Println("===========================")
	fmt.Println("=====configSeq=====")
	fmt.Println(configSeq)
	fmt.Println("===========================")
	// 這裡希望是 Marshaling Batch 的 Envelope， msg 可以不用改名
	msg, err := proto.Marshal(env)
	if err != nil {
		return err
	}

	ch.NopaxosServer.nopaxos.Command(
		&protocol.NewCommandRequest{
			CommandRequest: &protocol.CommandRequest{
				SessionNum: ch.sessionNumber,
				MessageNum: protocol.MessageID(sequencerNumber),
				Timestamp:  time.Now(),
				Value:      msg,
			},
			ConfigSeq: configSeq,
		},
		nil,
	)

	ch.Count = ch.Count + 1

	// 🔥 診斷：每次都打印當前狀態
	if ch.Count%10 == 0 {
		status := ch.NopaxosServer.nopaxos.GetStatus()
		isLeader := ch.NopaxosServer.nopaxos.IsLeader()
		fmt.Printf("[診斷] Count=%d, Seq=%d, Status=%s, IsLeader=%v\n",
			ch.Count, sequencerNumber, status, isLeader)
	}

	// 每 100 個訊息打印一次 gap 統計
	if ch.Count%100 == 0 {
		totalSlots, actualEntries, gaps, gapRate := ch.NopaxosServer.nopaxos.GetLogStatistics()
		fmt.Println("\n========== GAP STATISTICS ==========")
		fmt.Printf("Processed Messages: %d\n", ch.Count)
		fmt.Printf("Sequencer Number: %d\n", sequencerNumber)
		fmt.Printf("Total Slots: %d, Entries: %d, Gaps: %d\n", totalSlots, actualEntries, gaps)
		fmt.Printf("Gap Rate: %.2f%%\n", gapRate)

		// 計算訊息丟失率（基於 sequencer number）
		if sequencerNumber > 0 {
			expectedMessages := sequencerNumber + 1
			receivedMessages := ch.Count
			lossRate := (1.0 - float64(receivedMessages)/float64(expectedMessages)) * 100.0
			fmt.Printf("Message Loss Rate: %.2f%% (%d received / %d expected)\n",
				lossRate, receivedMessages, expectedMessages)
		}
		fmt.Println("====================================")
	}

	// 只有當以下條件都滿足時，才將交易發送到區塊鏈層：
	// 1. 這個節點是 leader
	// 2. NOPaxos 狀態是 Normal（不在 recovery、view change 或 gap commit 中）
	fmt.Println("=====IsLeader=====")
	fmt.Println(ch.NopaxosServer.nopaxos.IsLeader())
	fmt.Println("===========================")

	// 🔥 重要：只有 leader 會 write block 到本地
	if !ch.NopaxosServer.nopaxos.IsLeader() {
		return nil
	}

	// 🔥 重要：檢查是否正在進行區塊同步，如果是則丟棄交易
	ch.isSyncingMutex.RLock()
	syncing := ch.isSyncing
	ch.isSyncingMutex.RUnlock()

	if syncing {
		fmt.Printf("[Order] 丟棄交易：正在進行區塊同步（sequencer: %d）\n", sequencerNumber)
		return nil
	}

	// 🔥 重要：檢查 NOPaxos 狀態，在 ViewChange 期間不處理新交易
	status := ch.NopaxosServer.nopaxos.GetStatus()
	if status != protocol.StatusNormal {
		fmt.Printf("[Order] Skipping: NOPaxos status is %s (not Normal), waiting for view change to complete\n", status)
		return nil
	}

	// 🔥 診斷：檢查 sendChan 使用情況
	chanLen := len(ch.sendChan)
	chanCap := cap(ch.sendChan)
	chanUsage := float64(chanLen) / float64(chanCap) * 100

	if chanUsage > 80 {
		fmt.Printf("⚠️  [Order] sendChan 接近滿載！ 使用率: %.1f%% (%d/%d)\n", chanUsage, chanLen, chanCap)
	}

	fmt.Printf("[Order] 準備發送到 sendChan (sequencer: %d, chan: %d/%d, %.1f%%)\n",
		sequencerNumber, chanLen, chanCap, chanUsage)

	// 🔥 使用 non-blocking send 避免永遠阻塞
	// 這裡的 env 希望就是 batch 的 Envelope
	select {
	case ch.sendChan <- &message{
		sequencerNumber: sequencerNumber,
		sessionNumber:   ch.sessionNumber,
		configSeq:       configSeq,
		normalMsg:       env,
	}:
		fmt.Printf("[Order] ✓ 成功發送到 sendChan (sequencer: %d)\n", sequencerNumber)
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	default:
		// 🔥 如果 sendChan 滿了，不要阻塞，而是返回錯誤
		fmt.Printf("❌ [Order] sendChan 已滿，丟棄交易 (sequencer: %d)\n", sequencerNumber)
		return fmt.Errorf("sendChan full, dropping transaction")
	}
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

// createTLSCredentials 創建 TLS 憑證配置
func createTLSCredentials() (credentials.TransportCredentials, error) {
	// 嘗試從環境變量讀取 TLS 證書路徑
	tlsCertPath := os.Getenv("PEER_TLS_ROOTCERT_FILE")

	// 如果沒有設置，使用跳過驗證的 TLS（僅用於開發/測試）
	if tlsCertPath == "" {
		logger.Warning("未設置 PEER_TLS_ROOTCERT_FILE，使用跳過驗證的 TLS 連接")
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		return credentials.NewTLS(tlsConfig), nil
	}

	// 讀取 CA 證書
	caCert, err := os.ReadFile(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("無法讀取 CA 證書 %s: %v", tlsCertPath, err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("無法解析 CA 證書")
	}

	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}

	return credentials.NewTLS(tlsConfig), nil
}

// PullAllBlocksFromPeer 從 peer 拉取所有區塊 （這裡不用改動，都留著 可以用）
func (ch *chain) PullAllBlocksFromPeer(peerEndpoint string) ([]*cb.Block, error) {
	creds, err := createTLSCredentials()
	if err != nil {
		return nil, fmt.Errorf("創建 TLS 配置失敗: %v", err)
	}

	conn, err := grpc.NewClient(peerEndpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("連接 peer 失敗: %v", err)
	}
	defer conn.Close()

	client := pb.NewDeliverClient(conn)
	stream, err := client.Deliver(context.Background())
	if err != nil {
		return nil, fmt.Errorf("創建 deliver stream 失敗: %v", err)
	}

	// 創建 seek envelope，從區塊 0 開始拉取所有區塊
	seekInfo := &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT, // 使用 BEST_EFFORT 模式
	}

	envelope, err := ch.createSeekEnvelope(seekInfo)
	if err != nil {
		return nil, fmt.Errorf("創建 seek envelope 失敗: %v", err)
	}

	if err := stream.Send(envelope); err != nil {
		return nil, fmt.Errorf("發送 seek 請求失敗: %v", err)
	}

	blocks := []*cb.Block{}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return nil, fmt.Errorf("接收區塊失敗: %v", err)
		}

		switch t := resp.Type.(type) {
		case *pb.DeliverResponse_Block:
			blocks = append(blocks, t.Block)
			logger.Infof("從 peer %s 拉取區塊 %d", peerEndpoint, t.Block.Header.Number)
		case *pb.DeliverResponse_Status:
			if t.Status == cb.Status_SUCCESS {
				logger.Infof("成功從 peer %s 拉取所有區塊，總共 %d 個", peerEndpoint, len(blocks))
				return blocks, nil
			}
			return nil, fmt.Errorf("peer 返回狀態: %v", t.Status)
		default:
			return nil, fmt.Errorf("未知的 response 類型: %T", t)
		}
	}
}

// PullLatestBlockFromPeer 從 peer 拉取最新的區塊
func (ch *chain) PullLatestBlockFromPeer(peerEndpoint string) (*cb.Block, error) {
	logger.Infof("========== 開始連接 Peer ==========")
	logger.Infof("目標 Peer: %s", peerEndpoint)
	logger.Infof("Channel: %s", ch.support.ChannelID())

	logger.Infof("正在建立 gRPC 連接（使用 TLS）...")
	creds, err := createTLSCredentials()
	if err != nil {
		logger.Errorf("❌ 創建 TLS 配置失敗: %v", err)
		return nil, fmt.Errorf("創建 TLS 配置失敗: %v", err)
	}

	conn, err := grpc.NewClient(peerEndpoint,
		grpc.WithTransportCredentials(creds))
	if err != nil {
		logger.Errorf("❌ gRPC 連接失敗")
		logger.Errorf("  - Peer 地址: %s", peerEndpoint)
		logger.Errorf("  - 錯誤: %v", err)
		logger.Errorf("  - 可能原因:")
		logger.Errorf("    1. Peer 沒有運行")
		logger.Errorf("    2. Peer 地址配置錯誤")
		logger.Errorf("    3. 網絡不通或防火牆阻擋")
		logger.Errorf("    4. Peer 端口不是 7051")
		return nil, fmt.Errorf("連接 peer %s 失敗: %v", peerEndpoint, err)
	}
	defer conn.Close()
	logger.Infof("✓ gRPC 連接成功")

	client := pb.NewDeliverClient(conn)
	logger.Infof("正在創建 Deliver stream...")

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer streamCancel()

	stream, err := client.Deliver(streamCtx)
	if err != nil {
		logger.Errorf("❌ 創建 Deliver stream 失敗")
		logger.Errorf("  - 錯誤: %v", err)
		return nil, fmt.Errorf("創建 deliver stream 失敗: %v", err)
	}
	logger.Infof("✓ Deliver stream 創建成功")

	// 創建 seek envelope，只拉取最新區塊
	logger.Infof("正在創建 Seek envelope（請求最新區塊）...")
	seekInfo := &ab.SeekInfo{
		Start:         &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}},
		Stop:          &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      ab.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: ab.SeekInfo_BEST_EFFORT, // 使用 BEST_EFFORT 模式
	}

	envelope, err := ch.createSeekEnvelope(seekInfo)
	logger.Infof("send envelope: %v", envelope)
	if err != nil {
		logger.Errorf("❌ 創建 seek envelope 失敗: %v", err)
		return nil, fmt.Errorf("創建 seek envelope 失敗: %v", err)
	}
	logger.Infof("✓ Seek envelope 創建成功")

	logger.Infof("正在發送 Seek 請求到 peer...")
	if err := stream.Send(envelope); err != nil {
		logger.Errorf("❌ 發送 seek 請求失敗: %v", err)
		return nil, fmt.Errorf("發送 seek 請求失敗: %v", err)
	}
	logger.Infof("✓ Seek 請求已發送")

	logger.Infof("等待 peer 響應...")
	resp, err := stream.Recv()
	if err != nil {
		logger.Errorf("❌ 接收區塊失敗: %v", err)
		return nil, fmt.Errorf("接收區塊失敗: %v", err)
	}

	switch t := resp.Type.(type) {
	case *pb.DeliverResponse_Block:
		logger.Infof("✓ 成功從 peer %s 拉取最新區塊", peerEndpoint)
		logger.Infof("  - 區塊編號: %d", t.Block.Header.Number)
		logger.Infof("  - 交易數量: %d", len(t.Block.Data.Data))
		logger.Infof("  - 資料雜湊: %x", t.Block.Header.DataHash[:8])
		logger.Infof("========================================")
		return t.Block, nil
	case *pb.DeliverResponse_Status:
		logger.Errorf("❌ Peer 返回錯誤狀態: %v", t.Status)
		return nil, fmt.Errorf("peer 返回狀態: %v", t.Status)
	default:
		logger.Errorf("❌ 未知的 response 類型: %T", t)
		return nil, fmt.Errorf("未知的 response 類型: %T", t)
	}
}

// createSeekEnvelope 創建 seek envelope 用於向 peer 請求區塊
func (ch *chain) createSeekEnvelope(seekInfo *ab.SeekInfo) (*cb.Envelope, error) {
	channelID := ch.support.ChannelID()

	// 使用 Orderer 的身份來創建請求
	// ConsenterSupport 嵌入了 identity.SignerSerializer，可以直接作為 signer 使用
	// 即使使用 BEST_EFFORT 模式，也需要提供基本的身份信息

	// 使用 protoutil 創建帶簽名的 envelope
	// 這個方法會自動填充所有必要的字段（Creator, Nonce, TxId, Timestamp 等）
	envelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		cb.HeaderType_DELIVER_SEEK_INFO,
		channelID,
		ch.support, // ConsenterSupport 實現了 SignerSerializer 接口
		seekInfo,
		int32(0),  // msgVersion
		uint64(0), // epoch
		nil,       // tlsCertHash (orderer 通常不需要)
	)
	if err != nil {
		logger.Errorf("創建 seek envelope 失敗: %v", err)
		return nil, fmt.Errorf("創建 seek envelope 失敗: %v", err)
	}

	logger.Infof("✓ 創建 Seek envelope - Channel: %s, 使用 Orderer 身份簽名", channelID)
	return envelope, nil
}

// syncBlocksFromPeerAfterViewChange 在 view change 後從 peer 同步區塊
func (ch *chain) syncBlocksFromPeerAfterViewChange() error {
	// 🔥 設置同步標志，在同步期間丟棄所有新交易
	ch.isSyncingMutex.Lock()
	ch.isSyncing = true
	ch.isSyncingMutex.Unlock()
	logger.Info("🚫 已設置 isSyncing=true，同步期間將丟棄所有新交易")

	// 🔥 確保函數結束時清除同步標志
	defer func() {
		ch.isSyncingMutex.Lock()
		ch.isSyncing = false
		ch.isSyncingMutex.Unlock()
		logger.Info("✅ 已清除 isSyncing=false，恢復正常交易處理")
	}()

	logger.Info("╔════════════════════════════════════════════════════════════╗")
	logger.Info("║       View Change 區塊同步開始                              ║")
	logger.Info("╚════════════════════════════════════════════════════════════╝")

	// 顯示系統信息
	logger.Infof("【系統信息】")
	logger.Infof("  - Channel ID: %s", ch.support.ChannelID())
	logger.Infof("  - Peer Endpoint: %s", ch.peerEndpoint)
	logger.Infof("  - 是否為 Leader: %v", ch.NopaxosServer.nopaxos.IsLeader())

	// 1. 獲取本地當前區塊高度
	localHeight := ch.support.Height()
	logger.Infof("【本地狀態】")
	logger.Infof("  - 本地區塊高度: %d", localHeight)
	if localHeight > 0 {
		latestLocalBlock := ch.support.Block(localHeight - 1)
		if latestLocalBlock != nil {
			logger.Infof("  - 最新本地區塊編號: %d", latestLocalBlock.Header.Number)
		}
	}

	// 2. 從 peer 拉取最新區塊以檢查 peer 的高度（帶重試）
	logger.Infof("【開始檢查 Peer 狀態】")

	var latestBlock *cb.Block
	var err error
	maxRetries := 3

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			logger.Warningf("重試連接 peer (%d/%d)...", retry+1, maxRetries)
			time.Sleep(time.Duration(retry*2) * time.Second)
		}

		latestBlock, err = ch.PullLatestBlockFromPeer(ch.peerEndpoint)
		if err == nil {
			break
		}

		logger.Errorf("第 %d 次嘗試失敗: %v", retry+1, err)
	}

	if err != nil {
		logger.Errorf("╔════════════════════════════════════════════════════════════╗")
		logger.Errorf("║  ❌ 無法從 Peer 獲取最新區塊（已重試 %d 次）               ║", maxRetries)
		logger.Errorf("╚════════════════════════════════════════════════════════════╝")
		logger.Errorf("【診斷建議】")
		logger.Errorf("  1. 檢查 peer 是否正在運行:")
		logger.Errorf("     docker ps | grep peer")
		logger.Errorf("  2. 檢查 peer 地址是否正確:")
		logger.Errorf("     當前配置: %s", ch.peerEndpoint)
		logger.Errorf("  3. 測試網絡連通性:")
		logger.Errorf("     telnet %s", ch.peerEndpoint)
		logger.Errorf("  4. 查看 peer 日誌:")
		logger.Errorf("     docker logs peer0.org1.example.com")
		logger.Errorf("  5. 確認 peer 的 deliver 服務已啟動")
		return fmt.Errorf("無法從 peer 獲取最新區塊（重試 %d 次後失敗）: %v", maxRetries, err)
	}

	peerHeight := latestBlock.Header.Number + 1
	logger.Infof("【Peer 狀態】")
	logger.Infof("  - Peer 區塊高度: %d", peerHeight)
	logger.Infof("  - Peer 最新區塊編號: %d", latestBlock.Header.Number)

	// 3. 判斷是否需要同步
	gap := peerHeight - localHeight
	logger.Infof("【同步評估】")
	logger.Infof("  - 本地高度: %d", localHeight)
	logger.Infof("  - Peer 高度: %d", peerHeight)
	logger.Infof("  - 差距: %d 個區塊", gap)

	if peerHeight <= localHeight {
		logger.Info("╔════════════════════════════════════════════════════════════╗")
		logger.Info("║  ✓ 本地區塊已是最新，無需同步                                  ║")
		logger.Info("╚════════════════════════════════════════════════════════════╝")
		return nil
	}

	// 4. 需要同步，拉取所有區塊
	logger.Warningf("【需要同步】本地區塊落後 %d 個，開始從 peer 同步...", gap)

	allBlocks, err := ch.PullAllBlocksFromPeer(ch.peerEndpoint)
	if err != nil {
		logger.Errorf("❌ 從 peer 拉取區塊失敗: %v", err)
		return fmt.Errorf("從 peer 拉取區塊失敗: %v", err)
	}

	logger.Infof("✓ 成功從 peer 拉取 %d 個區塊", len(allBlocks))

	// 5. 寫入缺少的區塊到本地賬本
	logger.Infof("【開始寫入區塊】")
	syncedCount := 0
	startTime := time.Now()

	// 🔥 重要：使用互斥鎖保護，防止與 main() goroutine 的區塊創建衝突
	ch.syncMutex.Lock()
	logger.Info("🔒 已獲取 syncMutex，開始寫入區塊")
	defer func() {
		ch.syncMutex.Unlock()
		logger.Info("🔓 已釋放 syncMutex")
	}()

	for i := localHeight; i < uint64(len(allBlocks)); i++ {
		block := allBlocks[i]

		// 驗證區塊編號是否正確
		if block.Header.Number != i {
			logger.Errorf("❌ 區塊編號不匹配: 期望 %d，實際 %d", i, block.Header.Number)
			return fmt.Errorf("區塊編號不匹配: 期望 %d，實際 %d", i, block.Header.Number)
		}

		// 這裡這樣用可以嗎
		ch.support.WriteBlock(block, nil)

		syncedCount++
		progress := float64(syncedCount) / float64(gap) * 100
		logger.Infof("✓ 同步區塊 #%d (%d/%d, %.1f%%)", i, syncedCount, gap, progress)
	}

	elapsed := time.Since(startTime)
	logger.Info("╔════════════════════════════════════════════════════════════╗")
	logger.Infof("║  ✅ 區塊同步完成！                                         ║")
	logger.Info("╚════════════════════════════════════════════════════════════╝")
	logger.Infof("【同步統計】")
	logger.Infof("  - 同步區塊數: %d", syncedCount)
	logger.Infof("  - 耗時: %v", elapsed)
	logger.Infof("  - 平均速度: %.2f 區塊/秒", float64(syncedCount)/elapsed.Seconds())
	logger.Infof("  - 最終本地高度: %d", ch.support.Height())

	// ⚠️ 注意：使用 Append 繞過了 BlockWriter，BlockWriter.lastBlock 未更新
	// 影響：下次 CreateNextBlock 會使用過時的 lastBlock 信息
	// 緩解措施：
	//   1. 同步期間持有 syncMutex，阻止其他地方創建區塊
	//   2. 同步完成後，下一次 main() 調用 CreateNextBlock 時會從 ledger 讀取
	//   3. 因為 CreateNextBlock 內部使用 bw.lastBlock，所以第一次會錯誤...
	//
	// TODO: 改進方案 - 在同步完成後，手動更新 BlockWriter.lastBlock
	// 但由於 lastBlock 是私有字段，目前只能接受這個限制
	logger.Infof("【同步後狀態檢查】")
	if ch.support.Height() > 0 {
		latestBlock := ch.support.Block(ch.support.Height() - 1)
		if latestBlock != nil {
			logger.Infof("  - Ledger 最新區塊: #%d", latestBlock.Header.Number)
			logger.Warningf("  ⚠️  BlockWriter.lastBlock 未更新，可能導致下次 CreateNextBlock 錯誤")
			logger.Warningf("  → 建議在同步完成後使用 WriteBlock 寫入一個空區塊來更新狀態")
		}
	}
	logger.Infof("✓ 區塊同步完成")

	return nil
}

// setupViewChangeWatcher 設置 view change 監聽器
// 當 NOPaxos 狀態從 ViewChange 變為 Normal 且成為 leader 時，觸發區塊同步
func (ch *chain) setupViewChangeWatcher() {
	var previousStatus = protocol.StatusRecovering

	ch.NopaxosServer.nopaxos.Watch(func(newStatus protocol.Status) {
		logger.Infof("NOPaxos 狀態變化: %s -> %s", previousStatus, newStatus)

		// 檢測：從 ViewChange 變為 Normal（view change 完成）
		if previousStatus == protocol.StatusViewChange && newStatus == protocol.StatusNormal {
			// 檢查是否成為 leader
			if ch.NopaxosServer.nopaxos.IsLeader() {
				logger.Info("========== View Change 完成：成為新 Leader ==========")

				// 異步執行同步，避免阻塞 NOPaxos 狀態機
				go func() {
					if err := ch.syncBlocksFromPeerAfterViewChange(); err != nil {
						logger.Errorf("View Change 後同步區塊失敗: %v", err)
					} else {
						logger.Info("✓ View Change 後區塊同步成功")
					}
				}()
			} else {
				logger.Info("View Change 完成，但不是 leader，無需同步")
			}
		}

		previousStatus = newStatus
	})

	logger.Info("✓ View Change 監聽器已設置")
}

// monitorSystemHealth 定期監控系統健康狀態
func (ch *chain) monitorSystemHealth() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	var lastCount uint64 = 0

	for {
		select {
		case <-ticker.C:
			// 獲取各種統計信息
			chanLen := len(ch.sendChan)
			chanCap := cap(ch.sendChan)
			chanUsage := float64(chanLen) / float64(chanCap) * 100

			batchSize := len(ch.batch)
			currentCount := ch.Count
			throughput := currentCount - lastCount
			lastCount = currentCount

			isLeader := ch.NopaxosServer.nopaxos.IsLeader()
			status := ch.NopaxosServer.nopaxos.GetStatus()

			// 檢查是否在同步
			ch.isSyncingMutex.RLock()
			syncing := ch.isSyncing
			ch.isSyncingMutex.RUnlock()

			// 🔥 檢查 main goroutine 心跳
			ch.mainHeartbeatLock.RLock()
			lastHeartbeat := ch.mainHeartbeat
			ch.mainHeartbeatLock.RUnlock()
			timeSinceHeartbeat := time.Since(lastHeartbeat)
			mainGoroutineAlive := timeSinceHeartbeat < 30*time.Second

			// 打印系統健康報告
			fmt.Printf("\n╔═══════════════════════════════════════════════════════════╗\n")
			fmt.Printf("║          系統健康監控報告 (%s)                      \n", time.Now().Format("15:04:05"))
			fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
			fmt.Printf("║ NOPaxos 狀態                                              \n")
			fmt.Printf("║   - 狀態: %-20s   IsLeader: %-5v           \n", status, isLeader)
			fmt.Printf("║   - Session: %-10d                                     \n", ch.sessionNumber)
			fmt.Printf("║   - 正在同步: %-5v                                       \n", syncing)
			fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
			fmt.Printf("║ Channel 狀態                                              \n")
			fmt.Printf("║   - sendChan: %d/%d (%.1f%%)                          \n", chanLen, chanCap, chanUsage)
			if chanUsage > 90 {
				fmt.Printf("║   ⚠️  警告：sendChan 接近滿載！                           \n")
			}
			fmt.Printf("║   - 當前 batch: %d 筆交易                                 \n", batchSize)
			fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
			fmt.Printf("║ 交易統計                                                  \n")
			fmt.Printf("║   - 總處理數: %d                                          \n", currentCount)
			fmt.Printf("║   - 最近 15秒: %d 筆 (%.1f/秒)                           \n", throughput, float64(throughput)/15.0)
			fmt.Printf("╠═══════════════════════════════════════════════════════════╣\n")
			fmt.Printf("║ Goroutine 健康狀態                                        \n")
			if mainGoroutineAlive {
				fmt.Printf("║   - main() goroutine: ✅ 正常 (心跳: %.1fs 前)            \n", timeSinceHeartbeat.Seconds())
			} else {
				fmt.Printf("║   - main() goroutine: ❌ 可能死鎖 (心跳: %.1fs 前)       \n", timeSinceHeartbeat.Seconds())
			}
			fmt.Printf("╚═══════════════════════════════════════════════════════════╝\n\n")

			// 🔥 Deadlock 檢測：如果 main goroutine 超過 30 秒沒有心跳
			if !mainGoroutineAlive {
				fmt.Printf("🚨🚨🚨 嚴重警告：main() goroutine 可能已 DEADLOCK！🚨🚨🚨\n")
				fmt.Printf("   最後心跳時間: %.1f 秒前\n", timeSinceHeartbeat.Seconds())
				fmt.Printf("   可能原因：\n")
				fmt.Printf("   1. WriteBlock() 阻塞\n")
				fmt.Printf("   2. CreateNextBlock() 阻塞\n")
				fmt.Printf("   3. syncMutex 死鎖\n")
				fmt.Printf("   4. 其他互斥鎖問題\n\n")
			}

			// 如果 sendChan 持續高位，警告可能的問題
			if chanUsage > 95 {
				fmt.Printf("🚨 緊急警告：sendChan 使用率 %.1f%%，可能即將阻塞！\n", chanUsage)
				fmt.Printf("   建議檢查：\n")
				fmt.Printf("   1. main() goroutine 是否正常運行\n")
				fmt.Printf("   2. 區塊寫入是否阻塞\n")
				fmt.Printf("   3. 是否有 deadlock\n\n")
			}

		case <-ch.exitChan:
			return
		}
	}
}

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	go ch.NopaxosServer.Start()
	defer ch.NopaxosServer.Stop()

	// 設置 view change 監聽器（使用 NOPaxos 的 Watch 機制）
	ch.setupViewChangeWatcher()

	// 🔥 啟動系統監控 goroutine
	// go ch.monitorSystemHealth()

	for {
		// 🔥 更新心跳，證明 main goroutine 還活著
		ch.mainHeartbeatLock.Lock()
		ch.mainHeartbeat = time.Now()
		ch.mainHeartbeatLock.Unlock()

		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:

			fmt.Printf("[main] 📩 收到消息 (sequencer: %d)\n", msg.sequencerNumber)
			fmt.Println("=====msg=====")
			fmt.Println(msg.sequencerNumber)
			fmt.Println("===========================")

			// 🔥 Debug: 顯示消息內容
			fmt.Printf("🔍 [NOPaxos main] 消息類型檢查: batch_len=%d, hashBatch_len=%d, normalMsg=%v, configMsg=%v\n",
				len(msg.batch), len(msg.hashBatch), msg.normalMsg != nil, msg.configMsg != nil)

			// 🔥 優先處理 hash batch：如果 message 包含 hashBatch，創建 hash-only 區塊
			if len(msg.hashBatch) > 0 {
				fmt.Printf("✅ [NOPaxos main] 收到 hash batch! hash_count=%d\n", len(msg.hashBatch))
				// 🔥 只有 leader 才能創建區塊
				if !ch.NopaxosServer.nopaxos.IsLeader() {
					fmt.Printf("[main] ⏭️  非 leader 節點，跳過 hash batch 處理 (sequencer: %d, hash count: %d)\n",
						msg.sequencerNumber, len(msg.hashBatch))
					continue
				}

				fmt.Printf("[main] 🔨 收到 hash batch，創建 hash-only 區塊 (hash count: %d, sequencer: %d)\n",
					len(msg.hashBatch), msg.sequencerNumber)

				// 使用 CreateNextHashBlock 創建 hash-only 區塊
				block := ch.support.CreateNextHashBlock(msg.hashBatch)

				// 🔥 調試：檢查區塊順序
				currentHeight := ch.support.Height()
				expectedBlockNum := currentHeight
				if block.Header.Number != expectedBlockNum {
					logger.Warningf("⚠️  區塊編號不匹配！期望: %d, 實際: %d (sequencer: %d)",
						expectedBlockNum, block.Header.Number, msg.sequencerNumber)
				}

				fmt.Printf("[main] ✓ Hash-only 區塊已創建 (block #%d, hash count: %d, sequencer: %d, current height: %d)\n",
					block.Header.Number, len(msg.hashBatch), msg.sequencerNumber, currentHeight)

				// 寫入區塊
				fmt.Printf("[main] 💾 開始寫入 hash-only 區塊 #%d (PreviousHash: %x)...\n",
					block.Header.Number, block.Header.PreviousHash[:8])

				heightBefore := ch.support.Height()
				ch.support.WriteBlock(block, nil)

				// 等待一下讓異步寫入完成
				// time.Sleep(100 * time.Millisecond)
				heightAfter := ch.support.Height()

				fmt.Printf("[main] ✓ Hash-only 區塊 #%d WriteBlock 返回 (height: %d → %d)\n",
					block.Header.Number, heightBefore, heightAfter)

				if heightAfter <= heightBefore {
					fmt.Printf("⚠️  [main] WARNING: Height 沒有增加！Block 可能沒有被正確寫入！\n")
				}

				// 清空本地 batch
				ch.batch = []*cb.Envelope{}

				if timer != nil {
					timer = nil
				}
				continue
			}

			// 🔥 處理完整 batch：如果 message 包含 batch，直接使用它創建區塊
			if len(msg.batch) > 0 {
				// 🔥 只有 leader 才能創建區塊
				if !ch.NopaxosServer.nopaxos.IsLeader() {
					fmt.Printf("[main] ⏭️  非 leader 節點，跳過 batch 處理 (sequencer: %d, batch size: %d)\n",
						msg.sequencerNumber, len(msg.batch))
					continue
				}

				fmt.Printf("[main] 🔨 收到 batch，直接創建區塊 (batch size: %d, sequencer: %d)\n", len(msg.batch), msg.sequencerNumber)

				// 處理 batch 中的每個交易（驗證） 可以不用
				// for i, env := range msg.batch {
				// 	if msg.configSeq < seq {
				// 		_, err = ch.support.ProcessNormalMsg(env)
				// 		if err != nil {
				// 			logger.Warningf("Discarding bad normal message in batch[%d]: %s", i, err)
				// 			// 可以選擇跳過這個交易或整個 batch，這裡選擇跳過單個交易
				// 			continue
				// 		}
				// 	}
				// }

				// 直接使用 batch 創建區塊
				block := ch.support.CreateNextBlock(msg.batch)

				// 🔥 調試：檢查區塊順序
				currentHeight := ch.support.Height()
				expectedBlockNum := currentHeight
				if block.Header.Number != expectedBlockNum {
					logger.Warningf("⚠️  區塊編號不匹配！期望: %d, 實際: %d (sequencer: %d)",
						expectedBlockNum, block.Header.Number, msg.sequencerNumber)
				}

				fmt.Printf("[main] ✓ 區塊已創建 (block #%d, batch size: %d, sequencer: %d, current height: %d)\n", block.Header.Number, len(msg.batch), msg.sequencerNumber, currentHeight)

				// 🔥 使用 WriteBlockSync 確保區塊按順序寫入，避免 PreviousHash 不匹配
				// WriteBlock 是異步的，會立即更新 bw.lastBlock，可能導致下一個 CreateNextBlock
				// 使用錯誤的 previousBlockHash
				fmt.Printf("[main] 💾 開始寫入區塊 #%d (PreviousHash: %x)...\n", block.Header.Number, block.Header.PreviousHash[:8])
				ch.support.WriteBlock(block, nil)
				fmt.Printf("[main] ✓ 區塊 #%d 寫入完成 (新 height: %d)\n", block.Header.Number, ch.support.Height())

				// 清空本地 batch（因為已經用收到的 batch 創建了區塊）
				ch.batch = []*cb.Envelope{}

				if timer != nil {
					timer = nil
				}
				continue
			}

			if msg.configMsg == nil {
				fmt.Println("=====NormalMsg=====")
				// NormalMsg (單筆交易)
				if msg.configSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					if err != nil {
						logger.Warningf("Discarding bad normal message: %s", err)
						continue
					}
				}

				ch.batch = append(ch.batch, msg.normalMsg)

				// 這裡就不用做 cut 因為我已經是傳 batch 近來 直接 CreateNextBlock, WriteBlock 就好
				if len(ch.batch) > 512 {
					fmt.Println("=====CreateNextBlock: test view change=====: ", msg.sequencerNumber)
					fmt.Printf("[main] 🔨 開始創建區塊 (batch size: %d, sequencer: %d)\n", len(ch.batch), msg.sequencerNumber)

					block := ch.support.CreateNextBlock(ch.batch)
					fmt.Printf("[main] ✓ 區塊已創建 (block #%d)\n", block.Header.Number)

					fmt.Printf("[main] 💾 開始寫入區塊 #%d...\n", block.Header.Number)
					ch.support.WriteBlock(block, nil)
					fmt.Printf("[main] ✓ 區塊 #%d 寫入完成\n", block.Header.Number)

					// 清空 batch
					ch.batch = []*cb.Envelope{}

					if timer != nil {
						timer = nil
					}
				}

				pending := len(ch.batch) > 0

				switch {
				case timer != nil && !pending:
					// Timer is already running but there are no messages pending, stop the timer
					timer = nil
				case timer == nil && pending:
					// Timer is not already running and there are messages pending, so start it
					timer = time.After(250 * time.Millisecond)
					logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
				default:
					// Do nothing when:
					// 1. Timer is already running and there are messages pending
					// 2. Timer is not set and there are no messages pending
				}
			} else {
				// ConfigMsg
				fmt.Println("=====ConfigMsg=====")
				if msg.configSeq < seq {
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				fmt.Println("=====CreateNextBlock: test view change=====: ", msg.sequencerNumber)
				fmt.Printf("[configMsg] 🔨 開始創建區塊 (batch size: %d, sequencer: %d)\n", len(ch.batch), msg.sequencerNumber)
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					fmt.Printf("[configMsg] ✓ 區塊已創建 (block #%d)\n", block.Header.Number)
					fmt.Printf("[configMsg] 💾 開始寫入區塊 #%d...\n", block.Header.Number)
					ch.support.WriteBlock(block, nil)
					fmt.Printf("[configMsg] ✓ 區塊 #%d 寫入完成\n", block.Header.Number)
				}

				fmt.Printf("[configMsg] 🔨 開始創建區塊 (batch size: %d, sequencer: %d)\n", len(ch.batch), msg.sequencerNumber)

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})

				fmt.Printf("[configMsg] ✓ 區塊已創建 (block #%d)\n", block.Header.Number)
				fmt.Printf("[configMsg] 💾 開始寫入區塊 #%d...\n", block.Header.Number)

				ch.support.WriteConfigBlock(block, nil)

				fmt.Printf("[configMsg] ✓ 區塊 #%d 寫入完成\n", block.Header.Number)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			if len(ch.batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}

			block := ch.support.CreateNextBlock(ch.batch)

			ch.support.WriteBlock(block, nil)
			fmt.Printf("[time] ✓ 區塊 #%d 寫入完成\n", block.Header.Number)
			ch.batch = []*cb.Envelope{}

		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
