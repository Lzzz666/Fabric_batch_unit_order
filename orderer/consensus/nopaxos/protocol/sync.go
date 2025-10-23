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

import "fmt"

// 同步流程（Sync）概觀：
// - Leader 透過 SyncPrepare 廣播目前視圖下自己 log 的連續區間與 no-op bloom filter，要求追上。
// - Follower 收到 SyncPrepare 後，用濾器判斷哪些 slot 需要向 leader 索取（SyncRepair），
//   或若已齊全則直接回覆 SyncReply（並回推 CommandReply 給 sequencer）。
// - Leader/Follower 收到 SyncRepair 後回覆 SyncRepairReply，可能夾帶 checkpoint。
// - 當 leader 收到本地與多數（quorum）的 SyncReply 且同一同步點一致時，發出 SyncCommit，
//   要求所有副本將 applied 推進至該同步點。

func (s *NOPaxos) startSync() {
	fmt.Println("=====startSync=====")
	s.mu.Lock()
	// 修改：避免持鎖期間進行網路 I/O，改為鎖內建構、鎖外發送

	// If this replica is not the leader of the view, ignore the request
	if s.getLeader(s.viewID) != s.cluster.Member() {
		s.mu.Unlock()
		return
	}

	// If the replica's status is not Normal, do not attempt the sync
	if s.status != StatusNormal {
		s.mu.Unlock()
		return
	}

	s.syncReps = make(map[MemberID]*SyncReply)
	s.tentativeSync = s.log.LastSlot()

	// Create a no-op filter
	noOpFilter := newNoOpFilter(s.log.FirstSlot(), s.log.LastSlot())
	for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
		if entry := s.log.Get(slotNum); entry == nil {
			noOpFilter.add(slotNum)
		}
	}

	// Marshall the bloom filter to bytes
	noOpFilterBytes, err := noOpFilter.marshal()
	if err != nil {
		s.logger.Error("Failed to marshal bloom filter", err)
		s.mu.Unlock()
		return
	}

	syncPrepare := &SyncPrepare{
		Sender:          s.cluster.Member(),
		ViewID:          s.viewID,
		MessageNum:      s.sessionMessageNum,
		NoOpFilter:      noOpFilterBytes,
		FirstLogSlotNum: s.log.FirstSlot(),
		LastLogSlotNum:  s.log.LastSlot(),
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_SyncPrepare{
			SyncPrepare: syncPrepare,
		},
	}
	// 收集收件者於鎖內
	recipients := make([]MemberID, 0, len(s.cluster.Members()))
	for _, member := range s.cluster.Members() {
		if member != s.cluster.Member() {
			recipients = append(recipients, member)
		}
	}
	s.mu.Unlock()

	// 修改：鎖外發送
	for _, member := range recipients {
		s.logger.SendTo("SyncPrepare", syncPrepare, member)
		go s.send(message, member)
	}
}

func (s *NOPaxos) handleSyncPrepare(request *SyncPrepare) {
	fmt.Println("=====handleSyncPrepare=====")
	s.logger.ReceiveFrom("SyncPrepare", request, request.Sender)

	s.mu.Lock()
	// 修改：將發送移至鎖外，鎖內僅處理狀態

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		s.mu.Unlock()
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		s.mu.Unlock()
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.Sender != s.getLeader(request.ViewID) {
		s.mu.Unlock()
		return
	}

	// Unmarshal the leader's no-op filter
	noOpFilter := newNoOpFilter(request.FirstLogSlotNum, request.LastLogSlotNum)
	if err := noOpFilter.unmarshal(request.NoOpFilter); err != nil {
		s.logger.Error("Failed to decode bloom filter", err)
		s.mu.Unlock()
		return
	}

	newLog := newLog(request.FirstLogSlotNum)
	entrySlots := make([]LogSlotID, 0)
	for slotNum := request.FirstLogSlotNum; slotNum <= request.LastLogSlotNum; slotNum++ {
		// If the entry is greater than the last in the replica's log, request it.
		if entry := s.log.Get(slotNum); entry != nil {
			// If the entry is missing from the leader's log, request it. Otherwise add it to the new log.
			if noOpFilter.isMaybeNoOp(slotNum) {
				entrySlots = append(entrySlots, slotNum)
			} else {
				newLog.Set(entry)
			}
		} else if slotNum > s.log.LastSlot() {
			entrySlots = append(entrySlots, slotNum)
		}
	}

	// If the view's checkpoint is newer than the local checkpoint, request a checkpoint
	var checkpoint LogSlotID
	if request.FirstLogSlotNum > 1 && (s.currentCheckpoint == nil || request.FirstLogSlotNum > s.currentCheckpoint.SlotNum+1) {
		checkpoint = request.FirstLogSlotNum - 1
	}

	// If any entries need to be requested from the leader, request them. Otherwise, send a SyncReply
	var outMsg *ReplicaMessage
	var outLabel string
	var outTarget MemberID
	var sequencerSlots []LogSlotID
	if len(entrySlots) > 0 || checkpoint > 0 {
		leader := s.getLeader(s.viewID)
		syncRepair := &SyncRepair{
			Sender:     s.cluster.Member(),
			ViewID:     s.viewID,
			Checkpoint: checkpoint,
			SlotNums:   entrySlots,
		}
		outMsg = &ReplicaMessage{Message: &ReplicaMessage_SyncRepair{SyncRepair: syncRepair}}
		outLabel = "SyncRepair"
		outTarget = leader
	} else {
		s.sessionMessageNum = s.sessionMessageNum + MessageID(newLog.LastSlot()-s.log.LastSlot())
		s.log = newLog
		syncReply := &SyncReply{Sender: s.cluster.Member(), ViewID: s.viewID, SlotNum: s.log.LastSlot()}
		outMsg = &ReplicaMessage{Message: &ReplicaMessage_SyncReply{SyncReply: syncReply}}
		outLabel = "SyncReply"
		outTarget = request.Sender
		// 準備要回推給 sequencer 的 slot（鎖外送）
		for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
			if entry := s.log.Get(slotNum); entry != nil {
				sequencerSlots = append(sequencerSlots, slotNum)
			}
		}
	}
	s.mu.Unlock()
	// 修改：鎖外進行發送
	if outMsg != nil {
		switch outLabel {
		case "SyncRepair":
			s.logger.SendTo(outLabel, outMsg.GetSyncRepair(), outTarget)
			go s.send(outMsg, outTarget)
		case "SyncReply":
			s.logger.SendTo(outLabel, outMsg.GetSyncReply(), outTarget)
			go s.send(outMsg, outTarget)
		}
	}
	if outLabel == "SyncReply" {
		sequencer := s.sequencer
		if sequencer != nil {
			for _, slotNum := range sequencerSlots {
				entry := s.log.Get(slotNum)
				if entry != nil {
					_ = sequencer.Send(&ClientMessage{Message: &ClientMessage_CommandReply{CommandReply: &CommandReply{MessageNum: entry.MessageNum, Sender: s.cluster.Member(), ViewID: s.viewID, SlotNum: slotNum}}})
				}
			}
		}
	}
}

func (s *NOPaxos) handleSyncRepair(request *SyncRepair) {
	fmt.Println("=====handleSyncRepair=====")
	s.mu.RLock()
	// 注意：此函式使用讀鎖（RLock）讀取 s.log 與 s.currentCheckpoint 後，
	// 直接在持鎖期間進行 I/O 回覆（SendTo/send）。若其他地方需要升級為寫鎖將出現阻塞。

	// If the request views do not match, ignore the request
	if s.viewID.SessionNum != request.ViewID.SessionNum || s.viewID.LeaderNum != request.ViewID.LeaderNum {
		return
	}

	// Lookup entries for the requested slots
	entries := make([]*LogEntry, 0, len(request.SlotNums))
	for _, slotNum := range request.SlotNums {
		if entry := s.log.Get(slotNum); entry != nil {
			entries = append(entries, entry.LogEntry)
		}
	}

	// If a checkpoint was requested, return the checkpoint
	var checkpointSlotNum LogSlotID
	var checkpointData []byte
	if request.Checkpoint > 0 && s.currentCheckpoint != nil && request.Checkpoint <= s.currentCheckpoint.SlotNum {
		checkpointSlotNum = s.currentCheckpoint.SlotNum
		checkpointData = s.currentCheckpoint.Data
	}

	// Send non-nil entries back to the sender
	syncRepairReply := &SyncRepairReply{
		Sender:            s.cluster.Member(),
		ViewID:            s.viewID,
		CheckpointSlotNum: checkpointSlotNum,
		Checkpoint:        checkpointData,
		Entries:           entries,
	}
	message := &ReplicaMessage{Message: &ReplicaMessage_SyncRepairReply{SyncRepairReply: syncRepairReply}}
	sender := request.Sender
	s.mu.RUnlock()
	// 修改：鎖外發送回覆
	s.logger.SendTo("SyncRepairReply", syncRepairReply, sender)
	go s.send(message, sender)
}

func (s *NOPaxos) handleSyncRepairReply(reply *SyncRepairReply) {
	fmt.Println("=====handleSyncRepairReply=====")
	s.mu.Lock()
	// If the request views do not match, ignore the reply
	if s.viewID.SessionNum != reply.ViewID.SessionNum || s.viewID.LeaderNum != reply.ViewID.LeaderNum {
		s.mu.Unlock()
		return
	}

	// If no sync repair request is stored, ignore the reply
	request := s.syncRepair
	if request == nil || s.syncLog == nil {
		s.mu.Unlock()
		return
	}

	// If a checkpoint was returned and the checkpoint is newer than the local checkpoint, store the checkpoint
	if reply.CheckpointSlotNum > 0 && (s.currentCheckpoint == nil || reply.CheckpointSlotNum > s.currentCheckpoint.SlotNum) {
		s.currentCheckpoint = newCheckpoint(reply.CheckpointSlotNum)
		s.currentCheckpoint.Data = reply.Checkpoint
	}

	// Create a map of log entries
	entries := make(map[LogSlotID]*LogEntry)
	for _, entry := range reply.Entries {
		entries[entry.SlotNum] = entry
	}

	// For each requested slot, store the entry if one was returned. Otherwise, remove the entry
	for _, slotNum := range request.SlotNums {
		if entry := entries[slotNum]; entry != nil {
			s.syncLog.Set(
				&NewLogEntry{
					entry,
					0,
				},
			)
		} else {
			s.syncLog.Delete(slotNum)
		}
	}

	// Once the repair is complete, send a SyncReply
	s.sessionMessageNum = s.sessionMessageNum + MessageID(s.syncLog.LastSlot()-s.log.LastSlot())
	s.log = s.syncLog
	s.syncLog = nil

	// Send a SyncReply back to the leader
	syncReply := &SyncReply{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: s.log.LastSlot(),
	}
	message := &ReplicaMessage{Message: &ReplicaMessage_SyncReply{SyncReply: syncReply}}
	sender := reply.Sender
	s.mu.Unlock()
	// 修改：鎖外發送 SyncReply
	s.logger.SendTo("SyncReply", syncReply, sender)
	go s.send(message, sender)

	// Send a RequestReply for all entries in the new log
	sequencer := s.sequencer

	if sequencer != nil {
		for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
			entry := s.log.Get(slotNum)
			if entry != nil {
				_ = sequencer.Send(&ClientMessage{Message: &ClientMessage_CommandReply{CommandReply: &CommandReply{MessageNum: entry.MessageNum, Sender: s.cluster.Member(), ViewID: s.viewID, SlotNum: slotNum}}})
			}
		}
	}
}

func (s *NOPaxos) handleSyncReply(reply *SyncReply) {
	fmt.Println("=====handleSyncReply=====")
	s.logger.ReceiveFrom("SyncReply", reply, reply.Sender)

	s.mu.Lock()
	// 修改：改用寫鎖以保護對 s.syncReps 的寫入，並將 I/O 移到鎖外

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != reply.ViewID.LeaderNum || s.viewID.SessionNum != reply.ViewID.SessionNum {
		s.mu.Unlock()
		return
	}

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		s.mu.Unlock()
		return
	}

	// Add the reply to the set of sync replies
	s.syncReps[reply.Sender] = reply // 寫入共享 map，需寫鎖保護

	localSynced := false
	syncReps := make([]*SyncReply, 0, len(s.syncReps))
	for _, syncRep := range s.syncReps {
		if syncRep.ViewID.LeaderNum == s.viewID.LeaderNum && syncRep.ViewID.SessionNum == s.viewID.SessionNum && syncRep.SlotNum == s.tentativeSync {
			syncReps = append(syncReps, syncRep)
			if syncRep.Sender == s.cluster.Member() {
				localSynced = true
			}
		}
	}

	sessionMessageNum := s.sessionMessageNum - MessageID(s.log.LastSlot()-s.tentativeSync)

	var commitMsg *ReplicaMessage
	var targets []MemberID
	if localSynced && len(syncReps) >= s.cluster.QuorumSize() {
		commit := &SyncCommit{Sender: s.cluster.Member(), ViewID: s.viewID, MessageNum: sessionMessageNum, SyncPoint: s.tentativeSync}
		commitMsg = &ReplicaMessage{Message: &ReplicaMessage_SyncCommit{SyncCommit: commit}}
		for _, member := range s.cluster.Members() {
			if member != s.cluster.Member() {
				targets = append(targets, member)
			}
		}
	}
	s.mu.Unlock()
	if commitMsg != nil {
		for _, m := range targets {
			s.logger.SendTo("SyncCommit", commitMsg.GetSyncCommit(), m)
			go s.send(commitMsg, m)
		}
	}
}

func (s *NOPaxos) handleSyncCommit(request *SyncCommit) {
	fmt.Println("=====handleSyncCommit=====")
	s.logger.ReceiveFrom("SyncCommit", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()
	// 修改：改為寫鎖，因為會更新 s.applied

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the sender is not the leader for the current view, ignore the request
	if request.Sender != s.getLeader(request.ViewID) {
		return
	}

	// If a checkpoint exists and is less than the sync point, restore the checkpoint
	if s.currentCheckpoint != nil && s.currentCheckpoint.SlotNum <= request.SyncPoint && s.currentCheckpoint.SlotNum > s.applied {
		s.applied = s.currentCheckpoint.SlotNum // 寫共享狀態，需寫鎖
	}

	for slotNum := s.applied + 1; slotNum <= request.SyncPoint; slotNum++ {
		s.applied = slotNum // 寫共享狀態，需寫鎖
	}
}
