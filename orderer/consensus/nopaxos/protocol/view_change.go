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
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/willf/bloom"
)

// compareViewID 比較兩個 ViewID，返回值：> 0 表示 v1 > v2，< 0 表示 v1 < v2，0 表示相等
func compareViewID(v1, v2 *ViewId) int {
	if v1.LeaderNum != v2.LeaderNum {
		return int(v1.LeaderNum - v2.LeaderNum)
	}
	return int(v1.SessionNum - v2.SessionNum)
}

func (s *NOPaxos) startLeaderChange() {
	fmt.Println("====================startLeaderChange====================")

	s.mu.Lock()

	// 檢查是否已經在 view change 中，避免重複觸發
	if s.status == StatusViewChange {
		s.mu.Unlock()
		return
	}

	// Create new view ID with incremented leader number
	newViewID := &ViewId{
		SessionNum: s.viewID.SessionNum,
		LeaderNum:  s.viewID.LeaderNum + 1,
	}

	s.mu.Unlock()

	// Send ViewChangeRequest to all replicas (including self)
	viewChangeRequest := &ViewChangeRequest{
		Sender: s.cluster.Member(),
		ViewID: newViewID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRequest{
			ViewChangeRequest: viewChangeRequest,
		},
	}

	for _, member := range s.cluster.Members() {
		s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
		go s.send(message, member)
	}

	go s.resetTimeout()
}

func (s *NOPaxos) handleViewChangeRequest(request *ViewChangeRequest) {
	// 根據配置選擇使用簡化版或完整版
	s.mu.RLock()
	useSimplified := s.useSimplifiedViewChange
	s.mu.RUnlock()

	if useSimplified {
		s.handleViewChangeRequestSimplified(request)
		return
	}

	// 以下是完整版的邏輯
	s.logger.ReceiveFrom("ViewChangeRequest", request, request.Sender)

	s.mu.Lock()

	// If the replica is recovering, ignore the view change
	if s.status == StatusRecovering {
		s.mu.Unlock()
		return
	}

	var newViewID *ViewId
	if compareViewID(request.ViewID, s.viewID) > 0 { // 如果 request.ViewID 比 s.viewID 大，則使用 request.ViewID
		newViewID = request.ViewID
	} else {
		newViewID = s.viewID
	}

	// If the view IDs match, ignore the request
	// 如果 viewID 相同，但 session number 不同勒？要額外處理？
	if s.viewID.LeaderNum == newViewID.LeaderNum && s.viewID.SessionNum == newViewID.SessionNum {
		s.logger.Debug("Dropping ViewChangeRequest: Already in the requested view")
		s.mu.Unlock()
		return
	}

	// 記錄之前的狀態，用於判斷是否需要廣播
	previousStatus := s.status

	// Set the replica's status to ViewChange
	s.setStatus(StatusViewChange)
	// Set the replica's view ID to the new view ID
	s.viewID = newViewID

	// Reset the view changes
	s.viewChanges = make(map[MemberID]*ViewChange)

	// 修正：Create a bloom filter for NO-OP slots (empty slots)
	// NO-OP filter 應該標記**沒有**資料的 slot
	// 這裡應該是記錄所有有資料的 slot
	// 之後資料給 peer 也可以用這種方式，可是好像會有碰撞，有可能會遇到 false positive
	// 所以 Bloom Filter 永遠不會錯報「不存在的元素為不存在」，
	// 只有「不存在的元素被誤判為存在」。
	noOpFilter := bloom.New(uint(s.log.LastSlot()-s.log.FirstSlot()+1), bloomFilterHashFunctions)
	for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
		if entry := s.log.Get(slotNum); entry == nil { // entry == nil 才是 no-op
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(slotNum))
			noOpFilter.Add(key)
			fmt.Printf("empty slot: %d\n", slotNum)
		}
	}

	// Marshall the bloom filter to bytes
	noOpFilterBytes, err := json.Marshal(noOpFilter)
	if err != nil {
		s.logger.Error("Failed to encode bloom filter", err)
		s.mu.Unlock()
		return
	}

	// 準備要發送的消息（在鎖內準備）
	leader := s.getLeader(newViewID)
	// 把自己的 log 發送給 new leader
	viewChange := &ViewChange{
		Sender:          s.cluster.Member(),
		ViewID:          newViewID,
		LastNormal:      s.lastNormView,
		MessageNum:      s.sessionMessageNum,
		NoOpFilter:      noOpFilterBytes,
		FirstLogSlotNum: s.log.FirstSlot(),
		LastLogSlotNum:  s.log.LastSlot(),
	}
	viewChangeMessage := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChange{
			ViewChange: viewChange,
		},
	}

	viewChangeRequest := &ViewChangeRequest{
		Sender: s.cluster.Member(),
		ViewID: newViewID,
	}
	viewChangeRequestMessage := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRequest{
			ViewChangeRequest: viewChangeRequest,
		},
	}

	members := s.cluster.Members()
	myMember := s.cluster.Member()

	// 釋放鎖後再發送消息
	s.mu.Unlock()

	// Send a ViewChange message (自己ㄉ log) to the leader
	s.logger.SendTo("ViewChange", viewChange, leader)
	go s.send(viewChangeMessage, leader)

	// 只有在首次進入 view change 時才廣播 ViewChangeRequest（避免無窮遞迴）
	if previousStatus != StatusViewChange {
		// Send a ViewChangeRequest to all other replicas (不包括自己)
		for _, member := range members {
			if member != myMember {
				s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
				go s.send(viewChangeRequestMessage, member)
			}
		}
	} else {
		fmt.Println("=====Already was in ViewChange, skipping broadcast=====")
	}

	// Reset timeout to wait for StartView from the new leader
	go s.resetTimeout()
}

// 我要自己寫一個跟 peer 拉 log 的 view change function
// func (s *NOPaxos) handleViewChangePullLogFromPeer() {
// 	// TODO: 實作這個 function
// }

// 看一下這裡的流程
// 1. 收到 ViewChange 消息
// 2. 檢查 viewID 是否匹配
// 3. 檢查狀態是否為 ViewChange
// 4. 檢查是否為 leader
// 5. 檢查 viewChanges 是否達到 quorum
// 6. 找出最大的 lastNormal view
// 7. 重建 log
// 8. 發送 StartView 消息
// 9. 發送 ViewChangeRepair 消息
// 10. 發送 ViewChangeRepairReply 消息
// 11. 發送 StartView 消息

func (s *NOPaxos) handleViewChange(request *ViewChange) {
	// 根據配置選擇使用簡化版或完整版
	s.mu.RLock()
	useSimplified := s.useSimplifiedViewChange
	s.mu.RUnlock()

	if useSimplified {
		s.handleViewChangeSimplified(request)
		return
	}

	// 以下是完整版的邏輯
	s.logger.ReceiveFrom("ViewChange", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		s.logger.Debug("Dropping ViewChange: Views do not match")
		return
	}

	// If the replica's status is not ViewChange, ignore the request
	if s.status != StatusViewChange {
		s.logger.Debug("Dropping ViewChange: Replica status is not ViewChange")
		return
	}

	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(request.ViewID) != s.cluster.Member() {
		s.logger.Debug("Dropping ViewChange: Replica is not the leader of the requested view")
		fmt.Println("=====DROPPING ViewChange: I am not the leader for this view=====")
		return
	}

	fmt.Println("=====I AM THE LEADER, processing ViewChange=====")

	// Add the view change to the set of view changes

	s.viewChanges[request.Sender] = request

	localViewChanged := false
	viewChanges := make([]*ViewChange, 0, len(s.viewChanges))
	for _, viewChange := range s.viewChanges {
		if viewChange.ViewID.SessionNum == s.viewID.SessionNum && viewChange.ViewID.LeaderNum == s.viewID.LeaderNum {
			viewChanges = append(viewChanges, viewChange)
			if viewChange.Sender == s.cluster.Member() {
				localViewChanged = true
			}
		}
	}
	fmt.Println("=====viewChanges=====")
	fmt.Println(viewChanges)
	fmt.Println("========================================================")

	// If the view changes have reached a quorum, start the new view
	fmt.Println("=====ViewChange quorum check=====")
	fmt.Println("=====localViewChanged=====", localViewChanged)
	fmt.Println("=====len(viewChanges)=====", len(viewChanges))
	fmt.Println("=====QuorumSize=====", s.cluster.QuorumSize())

	if localViewChanged && len(viewChanges) >= s.cluster.QuorumSize() {
		fmt.Println("=====QUORUM REACHED! Starting new view=====")

		// 找出最大的 lastNormal view（修正算法）
		lastNormal := viewChanges[0].LastNormal
		for _, viewChange := range viewChanges[1:] {
			if viewChange.LastNormal.SessionNum > lastNormal.SessionNum ||
				(viewChange.LastNormal.SessionNum == lastNormal.SessionNum &&
					viewChange.LastNormal.LeaderNum > lastNormal.LeaderNum) {
				lastNormal = viewChange.LastNormal
			}
		}

		fmt.Println("=====Selected lastNormal view=====", lastNormal)

		var newMessageID MessageID
		var minSlotNum, maxSlotNum LogSlotID
		var maxCheckpoint LogSlotID

		noOpFilters := make(map[MemberID]*noOpFilter)
		for _, viewChange := range viewChanges {
			if viewChange.LastNormal.SessionNum == lastNormal.SessionNum && viewChange.LastNormal.LeaderNum == lastNormal.LeaderNum {
				noOpFilter := newNoOpFilter(viewChange.FirstLogSlotNum, viewChange.LastLogSlotNum)
				if err := noOpFilter.unmarshal(viewChange.NoOpFilter); err != nil {
					s.logger.Error("Failed to decode no-op filter", err)
					return
				}
				noOpFilters[viewChange.Sender] = noOpFilter

				// Record the min and max slot for all view changes
				if minSlotNum == 0 || viewChange.FirstLogSlotNum < minSlotNum {
					minSlotNum = viewChange.FirstLogSlotNum
				}
				if maxSlotNum == 0 || viewChange.LastLogSlotNum > maxSlotNum {
					maxSlotNum = viewChange.LastLogSlotNum
				}

				// If the replica has no checkpoint or its checkpoint is older than the view change's log
				// we need to request the checkpoint from a replica.
				if (s.currentCheckpoint == nil || s.currentCheckpoint.SlotNum < viewChange.FirstLogSlotNum) && viewChange.FirstLogSlotNum-1 > maxCheckpoint {
					maxCheckpoint = viewChange.FirstLogSlotNum - 1
				}

				// If the session has changed, take the maximum message ID
				if lastNormal.SessionNum == s.viewID.SessionNum && viewChange.MessageNum > newMessageID {
					newMessageID = viewChange.MessageNum
				}
			}
		}

		fmt.Println("=====Log range: minSlot=", minSlotNum, "maxSlot=", maxSlotNum, "=====")

		// 修正：重建 log 時，使用多數派規則
		newLog := newLog(minSlotNum)
		noOpSlots := make(map[MemberID][]LogSlotID)

		for slotNum := minSlotNum; slotNum <= maxSlotNum; slotNum++ {
			// 統計有多少 replica 認為這個 slot 是 no-op
			noOpCount := 0
			hasDataCount := 0

			for member, noOpFilter := range noOpFilters {
				if noOpFilter.isMaybeNoOp(slotNum) {
					noOpCount++
					// 需要從該 replica 請求確認
					slots := noOpSlots[member]
					if slots == nil {
						slots = make([]LogSlotID, 0)
					}
					noOpSlots[member] = append(slots, slotNum)
				} else {
					hasDataCount++
				}
			}

			fmt.Printf("=====Slot %d: noOp=%d, hasData=%d=====\n", slotNum, noOpCount, hasDataCount)

			// 如果本地有這個 entry，先加入 newLog
			// 後續透過 repair 機制確認是否真的該保留
			if entry := s.log.Get(slotNum); entry != nil {
				newLog.Set(entry)
			}
		}

		// Set the view change log. Note this log is maintained separate from the primary log until the view is started.
		s.viewLog = newLog

		fmt.Println("=====noOpSlots (need repair)=====", noOpSlots)
		fmt.Println("=====maxCheckpoint=====", maxCheckpoint)

		// If there are any missing slots in the log, store the new log and send LogRepair requests to peers to
		// determine whether a no-op entry should be written to the log. Otherwise, send a StartView.
		if len(noOpSlots) > 0 || maxCheckpoint > 0 {
			fmt.Println("=====Initiating repair phase=====")
			s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
			s.viewChangeRepairReps = make(map[MemberID]*ViewChangeRepairReply) // 初始化

			for member, slots := range noOpSlots {
				repair := &ViewChangeRepair{
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					MessageNum: newMessageID,
					Checkpoint: maxCheckpoint,
					SlotNums:   slots,
				}
				s.viewChangeRepairs[member] = repair // 記錄發送的 repair request

				message := &ReplicaMessage{
					Message: &ReplicaMessage_ViewChangeRepair{
						ViewChangeRepair: repair,
					},
				}
				s.logger.SendTo("ViewChangeRepair", repair, member)
				fmt.Printf("=====Sending repair request to %v for slots %v=====\n", member, slots)
				go s.send(message, member)
			}
		} else {
			fmt.Println("=====No repair needed, sending StartView directly=====")
			s.sendStartView(newMessageID)
		}
	} else {
		fmt.Println("=====NOT starting new view: quorum not reached=====")
	}
}

// sendStartView 提取為獨立函數，避免重複程式碼
func (s *NOPaxos) sendStartView(newMessageID MessageID) {
	// Create a new no-op filter and add no-op entries
	filter := newNoOpFilter(s.viewLog.FirstSlot(), s.viewLog.LastSlot())
	for slotNum := s.viewLog.FirstSlot(); slotNum <= s.viewLog.LastSlot(); slotNum++ {
		if entry := s.viewLog.Get(slotNum); entry == nil {
			filter.add(slotNum)
		}
	}

	// Marshal the no-op filter to JSON
	filterJson, err := filter.marshal()
	if err != nil {
		s.logger.Error("Failed to marshal bloom filter", err)
		return
	}

	// Create and send a StartView message to each replica with the no-op filter
	startView := &StartView{
		Sender:          s.cluster.Member(),
		ViewID:          s.viewID,
		MessageNum:      newMessageID,
		NoOpFilter:      filterJson,
		FirstLogSlotNum: s.viewLog.FirstSlot(),
		LastLogSlotNum:  s.viewLog.LastSlot(),
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_StartView{
			StartView: startView,
		},
	}

	// Send a StartView to each replica
	fmt.Println("=====SENDING StartView to all replicas=====")
	fmt.Println("=====StartView content=====", startView)
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("StartView", startView, member)
		fmt.Println("=====Sending StartView to", member, "=====")
		go s.send(message, member)
	}
}

func (s *NOPaxos) handleViewChangeRepair(request *ViewChangeRepair) {
	// 根據配置選擇使用簡化版或完整版
	s.mu.RLock()
	useSimplified := s.useSimplifiedViewChange
	s.mu.RUnlock()

	if useSimplified {
		s.handleViewChangeRepairSimplified(request)
		return
	}

	// 以下是完整版的邏輯
	fmt.Println("=====handleViewChangeRepair=====")
	fmt.Println("=====From:", request.Sender, "for slots:", request.SlotNums, "=====")

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the request views do not match, ignore the request
	if s.viewID.SessionNum != request.ViewID.SessionNum || s.viewID.LeaderNum != request.ViewID.LeaderNum {
		s.logger.Debug("Dropping ViewChangeRepair: Views do not match")
		fmt.Println("=====DROPPING: View mismatch=====")
		return
	}

	// Lookup entries for the requested slots. For any entry that's present in the log,
	// append the slot num.
	slots := make([]LogSlotID, 0, len(request.SlotNums))
	for _, slotNum := range request.SlotNums {
		if entry := s.log.Get(slotNum); entry != nil {
			slots = append(slots, slotNum)
			fmt.Printf("=====Slot %d: has entry=====\n", slotNum)
		} else {
			fmt.Printf("=====Slot %d: is NO-OP=====\n", slotNum)
		}
	}

	// If this replica's checkpoint was requested, send the checkpoint
	var checkpointSlotNum LogSlotID
	var checkpointData []byte
	if request.Checkpoint > 0 && s.currentCheckpoint != nil && request.Checkpoint <= s.currentCheckpoint.SlotNum {
		checkpointSlotNum = s.currentCheckpoint.SlotNum
		checkpointData = s.currentCheckpoint.Data
		fmt.Println("=====Sending checkpoint at slot", checkpointSlotNum, "=====")
	}

	// Send non-nil entries back to the sender
	viewChangeReply := &ViewChangeRepairReply{
		Sender:            s.cluster.Member(),
		ViewID:            s.viewID,
		MessageNum:        request.MessageNum,
		CheckpointSlotNum: checkpointSlotNum,
		Checkpoint:        checkpointData,
		SlotNums:          slots,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRepairReply{
			ViewChangeRepairReply: viewChangeReply,
		},
	}
	s.logger.SendTo("ViewChangeRepairReply", viewChangeReply, request.Sender)
	fmt.Printf("=====Replying to %v: slots with data=%v=====\n", request.Sender, slots)
	go s.send(message, request.Sender)
}

func (s *NOPaxos) handleViewChangeRepairReply(reply *ViewChangeRepairReply) {
	// 根據配置選擇使用簡化版或完整版
	s.mu.RLock()
	useSimplified := s.useSimplifiedViewChange
	s.mu.RUnlock()

	if useSimplified {
		s.handleViewChangeRepairReplySimplified(reply)
		return
	}

	// 以下是完整版的邏輯
	fmt.Println("=====handleViewChangeRepairReply=====")
	fmt.Println("=====From:", reply.Sender, "with slots:", reply.SlotNums, "=====")

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the reply view does not match the current view, skip the reply
	if s.viewID.SessionNum != reply.ViewID.SessionNum || s.viewID.LeaderNum != reply.ViewID.LeaderNum {
		s.logger.Debug("Dropping ViewChangeRepairReply: Views do not match")
		return
	}

	// Add the reply to the SlotRepair replies list
	s.viewChangeRepairReps[reply.Sender] = reply

	// If a checkpoint has been returned and the checkpoint is newer than the local checkpoint, update it
	// This is safe to do without waiting for replies from all replicas since snapshots can only be
	// taken of a consistent log.
	if reply.CheckpointSlotNum > 0 && (s.currentCheckpoint == nil || reply.CheckpointSlotNum > s.currentCheckpoint.SlotNum) {
		s.currentCheckpoint = newCheckpoint(reply.CheckpointSlotNum)
		s.currentCheckpoint.Data = reply.Checkpoint
		fmt.Println("=====Updated checkpoint to slot", reply.CheckpointSlotNum, "=====")
	}

	fmt.Printf("=====Repair progress: %d/%d replies received=====\n", len(s.viewChangeRepairReps), len(s.viewChangeRepairs))

	// If all view repairs have been responded to, remove entries where any slot is empty
	// and populate slots where all entries have been returned
	if len(s.viewChangeRepairs) == len(s.viewChangeRepairReps) {
		fmt.Println("=====All repair replies received, reconciling log=====")

		// Compute the number of requests for each slot
		slots := make(map[LogSlotID]*repairState)
		for _, slotRepair := range s.viewChangeRepairs {
			for _, slotNum := range slotRepair.SlotNums {
				state := slots[slotNum]
				if state == nil {
					state = &repairState{}
					slots[slotNum] = state
				}
				state.requests++
			}
		}

		// For each repair reply, add the replies to each slot
		for _, viewRepairRep := range s.viewChangeRepairReps {
			for _, slotNum := range viewRepairRep.SlotNums {
				state := slots[slotNum]
				if state != nil {
					state.replies++
				}
			}
		}

		// 關鍵修正：使用多數派規則決定保留還是刪除
		quorumSize := s.cluster.QuorumSize()
		for slotNum, slot := range slots {
			fmt.Printf("=====Slot %d: requests=%d, replies=%d, quorum=%d=====\n",
				slotNum, slot.requests, slot.replies, quorumSize)

			// 如果回報有資料的 replica 數量 < quorum，則刪除（認定為 no-op）
			if slot.replies < quorumSize {
				fmt.Printf("=====Deleting slot %d (insufficient confirmation: %d < %d)=====\n",
					slotNum, slot.replies, quorumSize)
				s.viewLog.Delete(slotNum)
			} else {
				fmt.Printf("=====Keeping slot %d (confirmed by quorum: %d >= %d)=====\n",
					slotNum, slot.replies, quorumSize)
			}
		}

		// Send StartView
		s.sendStartView(reply.MessageNum)

		// Unset repair fields
		s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
		s.viewChangeRepairReps = make(map[MemberID]*ViewChangeRepairReply)
		fmt.Println("=====Repair phase completed=====")
	}
}

// repairState holds the state of a slot repair
type repairState struct {
	requests int
	replies  int
}

func newNoOpFilter(firstSlotNum LogSlotID, lastSlotNum LogSlotID) *noOpFilter {
	filter := bloom.New(uint(lastSlotNum-firstSlotNum+1), bloomFilterHashFunctions)
	return &noOpFilter{
		firstSlotNum: firstSlotNum,
		lastSlotNum:  lastSlotNum,
		filter:       filter,
	}
}

type noOpFilter struct {
	firstSlotNum LogSlotID
	lastSlotNum  LogSlotID
	filter       *bloom.BloomFilter
}

func (f *noOpFilter) add(slotNum LogSlotID) {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(slotNum))
	f.filter.Add(key)
}

func (f *noOpFilter) isMaybeNoOp(slotNum LogSlotID) bool {
	if slotNum < f.firstSlotNum || slotNum > f.lastSlotNum {
		return false
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(slotNum))
	return f.filter.Test(key)
}

func (f *noOpFilter) marshal() ([]byte, error) {
	return json.Marshal(f.filter)
}

func (f *noOpFilter) unmarshal(bytes []byte) error {
	f.filter = &bloom.BloomFilter{}
	if err := json.Unmarshal(bytes, f.filter); err != nil {
		return err
	}
	return nil
}
