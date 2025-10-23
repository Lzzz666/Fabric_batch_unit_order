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

// handleMultipleGapsWithoutLock 為多個連續的 gap slots 發送 GapCommit 請求
// 注意：調用此函數前必須已經持有 s.mu 鎖，且 log 已經通過 Extend 創建了 gaps
// startSlot: 第一個 gap 的 slot number（通常等於第一個缺失的 message number）
// numGaps: gap 的數量
func (s *NOPaxos) handleMultipleGapsWithoutLock(startSlot LogSlotID, numGaps int) {
	fmt.Printf("=====handleMultipleGapsWithoutLock: %d gaps starting from slot %d=====\n", numGaps, startSlot)

	// If this replica is not the leader, skip the commit
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	if numGaps == 0 {
		return
	}

	// 設置狀態為 GapCommit
	s.setStatus(StatusGapCommit)

	// 記錄當前 gap 的範圍（使用最後一個 gap slot）
	endSlot := startSlot + LogSlotID(numGaps) - 1
	s.currentGapSlot = endSlot

	// 準備所有要發送的訊息（在持鎖期間）
	type gapCommitMsg struct {
		message *ReplicaMessage
		request *GapCommitRequest
	}
	messages := make([]gapCommitMsg, 0, numGaps)
	members := s.cluster.Members()

	// 為每個 gap slot 創建 GapCommit 請求
	for i := 0; i < numGaps; i++ {
		slotID := startSlot + LogSlotID(i)
		gapCommit := &GapCommitRequest{
			Sender:  s.cluster.Member(),
			ViewID:  s.viewID,
			SlotNum: slotID,
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_GapCommit{
				GapCommit: gapCommit,
			},
		}
		messages = append(messages, gapCommitMsg{message: message, request: gapCommit})
		fmt.Printf("[GapCommit] Prepared GapCommit for slot %d\n", slotID)
	}

	// 發送所有 GapCommit 訊息
	for _, msg := range messages {
		for _, member := range members {
			s.logger.SendTo("GapCommit", msg.request, member)
			go s.send(msg.message, member)
		}
	}
}

func (s *NOPaxos) sendGapCommitWithoutLock() {
	fmt.Println("=====sendGapCommit (single gap)=====")

	// If this replica is not the leader, skip the commit
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	fmt.Println("[Debug by lz] sendGapCommit status", s.status)
	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	// Add a no-op entry to the log
	s.log.Extend(s.log.LastSlot() + 1)
	slotID := s.log.LastSlot()

	s.sessionMessageNum++
	// Set the replica's status to GapCommit
	s.setStatus(StatusGapCommit)

	// Set the current gap slot
	s.currentGapSlot = slotID

	gapCommit := &GapCommitRequest{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: slotID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_GapCommit{
			GapCommit: gapCommit,
		},
	}

	// Send a GapCommit to each replica
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("GapCommit", gapCommit, member)
		go s.send(message, member)
	}
}

func (s *NOPaxos) sendGapCommit() {
	fmt.Println("=====sendGapCommit=====")
	s.mu.Lock()
	defer s.mu.Unlock()

	// If this replica is not the leader, skip the commit
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	// Add a no-op entry to the log
	// TODO: 這裡不應該只加 1，應該要算發生多少個 drop
	s.log.Extend(s.log.LastSlot() + 1)
	slotID := s.log.LastSlot()

	// Set the replica's status to GapCommit
	s.setStatus(StatusGapCommit)

	// Set the current gap slot
	s.currentGapSlot = slotID

	gapCommit := &GapCommitRequest{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: slotID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_GapCommit{
			GapCommit: gapCommit,
		},
	}

	// Send a GapCommit to each replica
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("GapCommit", gapCommit, member)
		go s.send(message, member)
	}
}

func (s *NOPaxos) handleGapCommit(request *GapCommitRequest) {
	fmt.Println("=====handleGapCommit=====")
	fmt.Println("[handleGapCommit] requestSender: ", request.Sender)
	fmt.Println("[handleGapCommit] SlotNum: ", request.SlotNum)
	s.logger.ReceiveFrom("GapCommitRequest", request, request.Sender)

	s.mu.Lock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		s.mu.Unlock()
		return
	}

	// If the replica's status is not Normal or GapCommit, skip the message
	if s.status != StatusNormal && s.status != StatusGapCommit {
		s.mu.Unlock()
		return
	}

	lastSlotID := s.log.LastSlot()

	// 處理三種情況：
	// 1. request.SlotNum == lastSlotID + 1: 這是預期的下一個 gap slot
	// 2. request.SlotNum <= lastSlotID: 這個 gap slot 已經在我們的範圍內（可能已有資料或為 gap）
	// 3. request.SlotNum > lastSlotID + 1: slot 不連續，需要先擴展 log

	if request.SlotNum > lastSlotID+1 {
		// 需要先擴展 log 到 request.SlotNum
		s.log.Extend(request.SlotNum)
	} else if request.SlotNum <= lastSlotID {
		// Slot 已在範圍內，確保它被標記為 gap（刪除任何現有資料）
		s.log.Delete(request.SlotNum)
	} else {
		// request.SlotNum == lastSlotID + 1，這是正常的下一個 slot
		s.log.Extend(request.SlotNum)
		// A no-op entry is represented as a missing entry，所以不需要 Set，只需要 Delete 確保它是空的
		s.log.Delete(request.SlotNum)
	}

	// Increment the session message ID if necessary
	// 只有當這個 gap slot 擴展了 log 時才增加
	if request.SlotNum > lastSlotID {
		s.sessionMessageNum++
	}

	// 準備回覆訊息（在持鎖期間）
	gapCommitReply := &GapCommitReply{
		Sender:  s.cluster.Member(),
		ViewID:  s.viewID,
		SlotNum: request.SlotNum,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_GapCommitReply{
			GapCommitReply: gapCommitReply,
		},
	}
	sender := request.Sender

	s.mu.Unlock()

	// 釋放鎖後再進行網路 I/O
	s.logger.SendTo("GapCommitReply", gapCommitReply, sender)
	go s.send(message, sender)
}

func (s *NOPaxos) handleGapCommitReply(reply *GapCommitReply) {
	fmt.Println("=====handleGapCommitReply=====")
	fmt.Println("[handleGapCommitReply] replySender: ", reply.Sender)
	fmt.Println("[handleGapCommitReply] reply: ", reply)
	s.logger.ReceiveFrom("GapCommitReply", reply, reply.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != reply.ViewID.LeaderNum || s.viewID.SessionNum != reply.ViewID.SessionNum {
		return
	}

	// If this replica is not the leader, skip the message
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal or GapCommit, skip the message
	if s.status != StatusGapCommit {
		return
	}

	// If the gap commit slot does not match the current gap slot, skip the message
	if reply.SlotNum != s.currentGapSlot {
		return
	}

	s.gapCommitReps[reply.Sender] = reply

	// Get the set of gap commit replies for the current slot
	gapCommits := make([]*GapCommitReply, 0, len(s.gapCommitReps))
	for _, gapCommit := range s.gapCommitReps {
		if gapCommit.ViewID.SessionNum == s.viewID.SessionNum && gapCommit.ViewID.LeaderNum == s.viewID.LeaderNum && gapCommit.SlotNum == s.currentGapSlot {
			gapCommits = append(gapCommits, gapCommit)
		}
	}

	// If a quorum of gap commits has been received for the slot, return the status to normal
	if len(gapCommits) >= s.cluster.QuorumSize() {
		s.setStatus(StatusNormal)
	}
}

func (s *NOPaxos) handleSlotLookup(request *SlotLookup) {
	fmt.Println("=====handleSlotLookup=====")
	fmt.Println("[handleSlotLookup] requestSender: ", request.Sender)
	fmt.Println("[handleSlotLookup] LastMessageNum: ", request.LastMessageNum, "MessageNum: ", request.MessageNum)
	s.logger.ReceiveFrom("SlotLookup", request, request.Sender)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the view ID does not match the sender's view ID, skip the message
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If this replica is not the leader, skip the message
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	// If the replica's status is not Normal, skip the message
	if s.status != StatusNormal {
		return
	}

	// 計算請求者缺少的 slot 範圍
	// 請求者提供了：
	// - LastMessageNum: 最後成功收到的 message number
	// - MessageNum: 剛收到的 message number（跳過了一些）
	// 我們需要發送 (LastMessageNum, MessageNum) 之間所有的 slots

	var startSlot, endSlot LogSlotID

	if request.LastMessageNum > 0 {
		// 使用提供的 LastMessageNum 計算起始 slot
		// startSlot = 對應 LastMessageNum + 1 的 slot
		startSlot = s.log.LastSlot() + 1 - LogSlotID(s.sessionMessageNum-request.LastMessageNum) - 1
		if startSlot < s.log.FirstSlot() {
			startSlot = s.log.FirstSlot()
		}
	} else {
		// 向後兼容：如果沒有提供 LastMessageNum，使用舊的計算方式
		startSlot = s.log.LastSlot() + 1 - LogSlotID(s.sessionMessageNum-request.MessageNum)
	}

	// 結束 slot 是對應當前 MessageNum 的 slot 的前一個
	endSlot = s.log.LastSlot() + 1 - LogSlotID(s.sessionMessageNum-request.MessageNum)

	fmt.Printf("[handleSlotLookup] Computed slot range: [%d, %d)\n", startSlot, endSlot)
	fmt.Printf("[handleSlotLookup] Leader log range: [%d, %d]\n", s.log.FirstSlot(), s.log.LastSlot())

	// 情況1：請求的 slots 在當前日誌範圍內
	if startSlot <= s.log.LastSlot() && endSlot > startSlot {
		// 發送所有 missing slots（包括非空的 entries）
		sentCount := 0
		for i := startSlot; i < endSlot && i <= s.log.LastSlot(); i++ {
			entry := s.log.Get(i)
			if entry != nil {
				commandRequest := &CommandRequest{
					SessionNum: s.viewID.SessionNum,
					MessageNum: entry.MessageNum,
					Value:      entry.Value,
				}
				message := &ReplicaMessage{
					Message: &ReplicaMessage_Command{
						Command: commandRequest,
					},
				}
				s.logger.SendTo("CommandRequest", commandRequest, request.Sender)
				go s.send(message, request.Sender)
				sentCount++
			}
		}
		fmt.Printf("[handleSlotLookup] Sent %d entries to %s\n", sentCount, request.Sender)
	} else if endSlot <= s.log.LastSlot()+1 {
		// 情況2：請求者基本上是同步的，但可能需要等待當前的 gap commit
		fmt.Println("===== Requester is mostly synced, waiting for gap commit =====")
		// 不需要額外操作，gap commit 會自動處理
	} else {
		// 情況3：請求者遠遠落後，啟動完整同步 （不行同步 leader 永遠最大）
		fmt.Printf("===== Requester is far behind (endSlot=%d > lastSlot+1=%d), starting sync =====\n",
			endSlot, s.log.LastSlot()+1)
		go s.startSync()
	}
}
