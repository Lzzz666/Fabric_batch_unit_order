// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/config"
	"github.com/hyperledger/fabric/orderer/consensus/nopaxos/util"
)

const bloomFilterHashFunctions = 5

// NewNOPaxos returns a new NOPaxos protocol state struct
func NewNOPaxos(cluster Cluster, config *config.ProtocolConfig, deliverChan chan []struct{}) *NOPaxos {
	nopaxos := &NOPaxos{
		logger:   util.NewNodeLogger(string(cluster.Member())),
		config:   config,
		cluster:  cluster,
		status:   StatusRecovering,
		watchers: make([]func(Status), 0),
		viewID: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		lastNormView: &ViewId{
			SessionNum: 1,
			LeaderNum:  1,
		},
		sessionMessageNum:    1,
		log:                  newLog(1),
		recoveryID:           uuid.New().String(),
		recoverReps:          make(map[MemberID]*RecoverReply),
		viewChanges:          make(map[MemberID]*ViewChange),
		viewChangeRepairs:    make(map[MemberID]*ViewChangeRepair),
		viewChangeRepairReps: make(map[MemberID]*ViewChangeRepairReply),
		gapCommitReps:        make(map[MemberID]*GapCommitReply),
		syncReps:             make(map[MemberID]*SyncReply),
		deliverChan:          deliverChan,

		// 預設使用簡化版 view change（適合有 peer sync 的場景）
		useSimplifiedViewChange: true, // false = 完整版, true = 簡化版
	}
	nopaxos.start()
	return nopaxos
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// MemberID is the ID of a NOPaxos cluster member
type MemberID string

// LeaderID is the leader identifier
type LeaderID uint64

// SessionID is a sequencer session ID
type SessionID uint64

// MessageID is a sequencer message ID
type MessageID uint64

// Status is the protocol status
type Status string

const (
	// StatusRecovering is used by a recovering replica to avoid operating on old state
	StatusRecovering Status = "Recovering"
	// StatusNormal is the normal status
	StatusNormal Status = "Normal"
	// StatusViewChange is used to ignore certain messages during view changes
	StatusViewChange Status = "ViewChange"
	// StatusGapCommit indicates the replica is undergoing a gap commit
	StatusGapCommit Status = "GapCommit"
)

// NOPaxos is an interface for managing the state of the NOPaxos consensus protocol
type NOPaxos struct {
	logger               util.Logger
	config               *config.ProtocolConfig
	cluster              Cluster                             // The cluster state
	applied              LogSlotID                           // The highest slot applied to the state machine
	sequencer            ClientService_ClientStreamServer    // The stream to the sequencer
	log                  *Log                                // The primary log
	status               Status                              // The status of the replica
	watchers             []func(Status)                      // A list of status watchers
	recoveryID           string                              // A nonce indicating the recovery attempt
	recoverReps          map[MemberID]*RecoverReply          // The set of recover replies received
	currentCheckpoint    *Checkpoint                         // The current checkpoint (if any)
	sessionMessageNum    MessageID                           // The latest message num for the sequencer session
	viewID               *ViewId                             // The current view ID
	lastNormView         *ViewId                             // The last normal view ID
	viewChanges          map[MemberID]*ViewChange            // The set of view changes received
	viewChangeRepairs    map[MemberID]*ViewChangeRepair      // The set of view change repairs received
	viewChangeRepairReps map[MemberID]*ViewChangeRepairReply // The set of view change repair replies received
	viewRepair           *ViewRepair                         // The last view repair requested
	viewLog              *Log                                // A temporary log for view starts
	currentGapSlot       LogSlotID                           // The current slot for which a gap is being committed
	gapCommitReps        map[MemberID]*GapCommitReply        // The set of gap commit replies
	tentativeSync        LogSlotID                           // The tentative sync point
	syncPoint            LogSlotID                           // The last known sync point
	syncRepair           *SyncRepair                         // The last sent sync repair
	syncReps             map[MemberID]*SyncReply             // The set of sync replies received
	syncLog              *Log                                // A temporary log for synchronization
	pingTicker           *time.Ticker
	checkpointTicker     *time.Ticker
	syncTicker           *time.Ticker
	timeoutTimer         *time.Timer
	mu                   sync.RWMutex
	deliverChan          chan []struct{}
	pendingTxs           []*message
	canCommit            bool // 標記 leader 是否已寫入 slot

	// View Change 模式選擇：true = 簡化版（僅切換 leader），false = 完整版（含 log repair）
	useSimplifiedViewChange bool
}

func (s *NOPaxos) start() {
	s.mu.Lock()
	s.setPingTicker()
	s.setCheckpointTicker()
	go s.resetTimeout()
	s.mu.Unlock()
	fmt.Println("=====NoPaxos Protocol Start=====")
}

func (s *NOPaxos) Watch(watcher func(Status)) {
	s.watchers = append(s.watchers, watcher)
}

func (s *NOPaxos) setStatus(status Status) {
	if s.status != status {
		s.logger.Debug("Replica status changed: %s", status)
		s.status = status
		for _, watcher := range s.watchers {
			watcher(status)
		}
	}
}

func (s *NOPaxos) resetTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timeoutTimer != nil {
		s.timeoutTimer.Stop()
	}
	s.timeoutTimer = time.NewTimer(s.config.GetLeaderTimeoutOrDefault())
	go func() {
		select {
		case _, ok := <-s.timeoutTimer.C:
			if ok {
				s.Timeout()
			}
		}
	}()
}

func (s *NOPaxos) setPingTicker() {
	s.logger.Debug("Setting ping ticker")
	s.pingTicker = time.NewTicker(s.config.GetPingIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.pingTicker.C:
				if !ok {
					return
				}
				s.sendPing()
			}
		}
	}()
}

// 每隔一段時間，自動保存目前系統狀態的快照（snapshot）
func (s *NOPaxos) setCheckpointTicker() {
	s.logger.Debug("Setting checkpoint ticker")
	s.checkpointTicker = time.NewTicker(s.config.GetCheckpointIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.checkpointTicker.C:
				if !ok {
					return
				}
				s.checkpoint()
			}
		}
	}()
}

func (s *NOPaxos) setSyncTicker() {
	s.logger.Debug("Setting sync ticker")
	s.checkpointTicker = time.NewTicker(s.config.GetSyncIntervalOrDefault())
	go func() {
		for {
			select {
			case _, ok := <-s.syncTicker.C:
				if !ok {
					return
				}
				s.startSync()
			}
		}
	}()
}

func (s *NOPaxos) ClientStream(stream ClientService_ClientStreamServer) error {
	s.mu.Lock()
	s.sequencer = stream
	s.mu.Unlock()
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.handleClient(message, stream)
	}
}

func (s *NOPaxos) handleClient(message *ClientMessage, stream ClientService_ClientStreamServer) {
	switch m := message.Message.(type) {
	case *ClientMessage_Command:
		nc := &NewCommandRequest{
			m.Command,
			0,
		}
		s.Command(nc, stream)
	case *ClientMessage_Query:
		s.query(m.Query, stream)
	}
}

func (s *NOPaxos) ReplicaStream(stream ReplicaService_ReplicaStreamServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		go s.handleReplica(message)
	}
}

func (s *NOPaxos) handleReplica(message *ReplicaMessage) {
	switch m := message.Message.(type) {
	case *ReplicaMessage_Command:
		nc := &NewCommandRequest{
			m.Command,
			0,
		}
		s.handleSlot(nc)
	case *ReplicaMessage_SlotLookup:
		s.handleSlotLookup(m.SlotLookup)
	case *ReplicaMessage_GapCommit:
		s.handleGapCommit(m.GapCommit)
	case *ReplicaMessage_GapCommitReply:
		s.handleGapCommitReply(m.GapCommitReply)
	case *ReplicaMessage_ViewChangeRequest:
		s.handleViewChangeRequest(m.ViewChangeRequest)
	case *ReplicaMessage_ViewChange:
		s.handleViewChange(m.ViewChange)
	case *ReplicaMessage_ViewChangeRepair:
		s.handleViewChangeRepair(m.ViewChangeRepair)
	case *ReplicaMessage_ViewChangeRepairReply:
		s.handleViewChangeRepairReply(m.ViewChangeRepairReply)
	case *ReplicaMessage_StartView:
		s.handleStartView(m.StartView)
	case *ReplicaMessage_ViewRepair:
		s.handleViewRepair(m.ViewRepair)
	case *ReplicaMessage_ViewRepairReply:
		s.handleViewRepairReply(m.ViewRepairReply)
	case *ReplicaMessage_SyncPrepare:
		s.handleSyncPrepare(m.SyncPrepare)
	case *ReplicaMessage_SyncRepair:
		s.handleSyncRepair(m.SyncRepair)
	case *ReplicaMessage_SyncRepairReply:
		s.handleSyncRepairReply(m.SyncRepairReply)
	case *ReplicaMessage_SyncReply:
		s.handleSyncReply(m.SyncReply)
	case *ReplicaMessage_SyncCommit:
		s.handleSyncCommit(m.SyncCommit)
	case *ReplicaMessage_Recover:
		s.handleRecover(m.Recover)
	case *ReplicaMessage_RecoverReply:
		s.handleRecoverReply(m.RecoverReply)
	case *ReplicaMessage_Ping:
		s.handlePing(m.Ping)
	}
}

func (s *NOPaxos) send(message *ReplicaMessage, member MemberID) {
	if stream, err := s.cluster.GetStream(member); err == nil {
		err := stream.Send(message)
		if err != nil {
			// 降低日誌級別，避免大量錯誤消息
			s.logger.Debug("Failed to send to %s: %v (node may be offline)", member, err)
		}
	} else {
		// 節點不可用時只記錄 debug 日誌，不影響正常運行
		s.logger.Debug("Failed to open stream to %s: %v (node may be offline)", member, err)
	}
}

func (s *NOPaxos) getLeader(viewID *ViewId) MemberID {
	members := s.cluster.Members()
	return members[int(uint64(viewID.LeaderNum)%uint64(len(members)))]
}

func (s *NOPaxos) IsLeader() bool {
	members := s.cluster.Members()
	return members[int(uint64(s.viewID.LeaderNum)%uint64(len(members)))] == s.cluster.Member()
}

// GetStatus 返回當前協議狀態（線程安全）
func (s *NOPaxos) GetStatus() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// GetLogStatistics 返回當前 log 的 gap 統計信息（線程安全）
func (s *NOPaxos) GetLogStatistics() (totalSlots int, actualEntries int, gaps int, gapRate float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.GetGapStatistics()
}

func (s *NOPaxos) CanCommit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.canCommit
	s.canCommit = false // 重置標誌
	return result
}

// SetSessionMessageNum 設置 session message number（線程安全）
// 用於 view change 後從 peer 同步區塊時更新協議狀態
func (s *NOPaxos) SetSessionMessageNum(messageNum MessageID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionMessageNum = messageNum
}

// GetSessionMessageNum 獲取當前 session message number（線程安全）
func (s *NOPaxos) GetSessionMessageNum() MessageID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessionMessageNum
}

// SetSimplifiedViewChange 設置是否使用簡化版本的 view change
// simplified = true: 使用簡化版（僅切換 leader，依賴 peer sync）
// simplified = false: 使用完整版（包含 log repair 邏輯）
func (s *NOPaxos) SetSimplifiedViewChange(simplified bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.useSimplifiedViewChange = simplified
	if simplified {
		fmt.Println("✓ View Change 模式: 簡化版（Simplified - 依賴 Peer Sync）")
	} else {
		fmt.Println("✓ View Change 模式: 完整版（Full - 含 Log Repair）")
	}
}
