package nopaxos

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	nopaxosConfig "github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/protocol"
	"github.com/pkg/errors"
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
	seqNumToSlot  map[protocol.SessionID]map[int]int
}

type message struct {
	configSeq       uint64
	sequencerNumber uint64
	sessionNumber   protocol.SessionID
	normalMsg       *cb.Envelope
	configMsg       *cb.Envelope
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

	host, _, err := net.SplitHostPort(config.Operations.ListenAddress)
	if err != nil {
		fmt.Println("Error:", err)
	}

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
		MemberID: host,
		Members:  members,
	}

	deliverChan := make(chan []struct{})

	return &chain{
		support:     support,
		sendChan:    make(chan *message),
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
		seqNumToSlot:  make(map[protocol.SessionID]map[int]int),
	}
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

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64, sequencerId uint64, sequencerNumber uint64) error {
	fmt.Println("=======TESTTEST=======")
	fmt.Println(ch.sessionNumber, sequencerNumber)
	fmt.Println("===========================")
	fmt.Println("=====isInit=====")
	fmt.Println(ch.isInit)
	fmt.Println("===========================")
	fmt.Println("=====configSeq=====")
	fmt.Println(configSeq)
	fmt.Println("===========================")
	msg, err := proto.Marshal(env)
	if err != nil {
		return err
	}

	// 先調用 NOPaxos 的 Command 處理
	// 根本沒有處理 session 跟 view change
	// 怎麼判斷是否為新的 session??
	// 出現 seqnumber 為 1 就代表是新的 session (先這樣設計，但需要考慮 1 也會掉包)
	// if sequencerNumber == 1 && !ch.isInit {
	// 	ch.sessionNumber += 1
	// 	fmt.Println("=====sessionNumber=====")
	// 	fmt.Println(ch.sessionNumber)
	// 	fmt.Println("===========================")
	// }
	ch.isInit = false

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
	if !ch.NopaxosServer.nopaxos.IsLeader() {
		return nil
	}

	// 檢查 NOPaxos 狀態 （我這裡不應該用狀態判斷，而是判斷 leader 是否真的有寫入 slot）
	// status := ch.NopaxosServer.nopaxos.GetStatus()
	// if status != protocol.StatusNormal {
	// 	fmt.Printf("[Order] Skipping block creation: NOPaxos status is %s (not Normal)\n", status)
	// 	return nil
	// }

	// seq
	if !ch.NopaxosServer.nopaxos.CanCommit() && sequencerNumber > 0 {
		return nil
	}
	fmt.Println("=====Order Normal=====")
	// 狀態正常，發送到區塊鏈處理通道
	select {
	case ch.sendChan <- &message{
		sequencerNumber: sequencerNumber,
		sessionNumber:   ch.sessionNumber,
		configSeq:       configSeq,
		normalMsg:       env,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
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

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	go ch.NopaxosServer.Start()
	defer ch.NopaxosServer.Stop()

	for {
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			fmt.Println("=====msg=====")
			fmt.Println(msg.sequencerNumber)
			fmt.Println("===========================")
			if msg.configMsg == nil {
				// NormalMsg
				if msg.configSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
					if err != nil {
						logger.Warningf("Discarding bad normal message: %s", err)
						continue
					}
				}

				ch.batch = append(ch.batch, msg.normalMsg)

				// 維護一個 map table 來記錄 sequencer number 到 slot 的映射 map table 應該要有 session number 才不會因為換 sequencer number 就重置
				// 然後每次切 block 的時候 把 map table 的資料傳給 peer 讓 peer 與 leader 的 log 同步

				// 確保外層 map 已存在
				if _, ok := ch.seqNumToSlot[msg.sessionNumber]; !ok {
					ch.seqNumToSlot[msg.sessionNumber] = make(map[int]int)
				}

				// 建立映射：sequencerNumber -> slotNumber
				// TODO: 需要從 NOPaxos log 中獲取實際的 slotNumber
				slotNumber := int(msg.sequencerNumber) // 暫時使用 sequencerNumber 作為 slotNumber
				ch.seqNumToSlot[msg.sessionNumber][int(msg.sequencerNumber)] = slotNumber

				fmt.Println("=====seqNumToSlot=====")
				fmt.Println(ch.seqNumToSlot)
				fmt.Println("len:", len(ch.seqNumToSlot[msg.sessionNumber]))
				fmt.Println("===========================")

				if len(ch.batch) > 512 {
					block := ch.support.CreateNextBlock(ch.batch)

					// 將 seqNumToSlot 映射資料編碼為區塊元數據
					seqNumToSlotData, err := json.Marshal(ch.seqNumToSlot[msg.sessionNumber])
					if err != nil {
						logger.Errorf("Failed to marshal seqNumToSlot: %v", err)
						seqNumToSlotData = []byte{}
					}

					// 將元數據寫入區塊，peer 可以從區塊元數據中讀取這些資訊
					ch.support.WriteBlock(block, seqNumToSlotData)

					logger.Infof("Block written with seqNumToSlot metadata, session: %d, mappings: %d",
						msg.sessionNumber, len(ch.seqNumToSlot[msg.sessionNumber]))

					// 清空已發送的映射資料
					ch.batch = []*cb.Envelope{}
					ch.seqNumToSlot[msg.sessionNumber] = make(map[int]int)

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
				if msg.configSeq < seq {
					msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
					if err != nil {
						logger.Warningf("Discarding bad config message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)

					// 對於 config 更新前的批次，也附加 seqNumToSlot 元數據
					seqNumToSlotData, err := json.Marshal(ch.seqNumToSlot[msg.sessionNumber])
					if err != nil {
						logger.Errorf("Failed to marshal seqNumToSlot: %v", err)
						seqNumToSlotData = []byte{}
					}

					ch.support.WriteBlock(block, seqNumToSlotData)
					ch.seqNumToSlot[msg.sessionNumber] = make(map[int]int)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			if len(ch.batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(ch.batch)

			// Timer 觸發時也附加 seqNumToSlot 元數據
			// 取得當前 session 的 seqNumToSlot 資料
			var seqNumToSlotData []byte
			if len(ch.seqNumToSlot[ch.sessionNumber]) > 0 {
				var err error
				seqNumToSlotData, err = json.Marshal(ch.seqNumToSlot[ch.sessionNumber])
				if err != nil {
					logger.Errorf("Failed to marshal seqNumToSlot: %v", err)
					seqNumToSlotData = []byte{}
				}
				logger.Infof("Timer expired: Block written with seqNumToSlot metadata, session: %d, mappings: %d",
					ch.sessionNumber, len(ch.seqNumToSlot[ch.sessionNumber]))
			}

			ch.support.WriteBlock(block, seqNumToSlotData)
			ch.batch = []*cb.Envelope{}

			// 清空已發送的映射資料
			if len(ch.seqNumToSlot[ch.sessionNumber]) > 0 {
				ch.seqNumToSlot[ch.sessionNumber] = make(map[int]int)
			}

		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
