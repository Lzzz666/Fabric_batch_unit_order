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

// ======================================================================
// 簡化版 View Change 實作
// ======================================================================
// 這個版本的 view change 不進行 log repair，僅負責安全地切換 leader
// 依賴上層（chain.go）的 peer sync 機制來同步區塊狀態
// ======================================================================

// handleViewChangeRequestSimplified 簡化版：處理 ViewChangeRequest
// 只需要切換到 ViewChange 狀態，並向新 leader 發送 ViewChange 消息
func (s *NOPaxos) handleViewChangeRequestSimplified(request *ViewChangeRequest) {
	s.logger.ReceiveFrom("ViewChangeRequest [Simplified]", request, request.Sender)

	s.mu.Lock()

	// 如果正在 recovering，忽略
	if s.status == StatusRecovering {
		s.mu.Unlock()
		return
	}

	var newViewID *ViewId
	if compareViewID(request.ViewID, s.viewID) > 0 {
		newViewID = request.ViewID
	} else {
		newViewID = s.viewID
	}

	// 如果已經在這個 view，忽略
	if s.viewID.LeaderNum == newViewID.LeaderNum && s.viewID.SessionNum == newViewID.SessionNum {
		s.logger.Debug("Dropping ViewChangeRequest: Already in the requested view")
		s.mu.Unlock()
		return
	}

	previousStatus := s.status

	// 切換到 ViewChange 狀態
	s.setStatus(StatusViewChange)
	s.viewID = newViewID

	// 重置 view changes
	s.viewChanges = make(map[MemberID]*ViewChange)

	// 準備 ViewChange 消息（簡化版：不包含 log 信息）
	leader := s.getLeader(newViewID)
	viewChange := &ViewChange{
		Sender:     s.cluster.Member(),
		ViewID:     newViewID,
		LastNormal: s.lastNormView,
		MessageNum: s.sessionMessageNum,
		// 簡化版：不需要 NoOpFilter 和 log 範圍信息
		NoOpFilter:      nil,
		FirstLogSlotNum: 0,
		LastLogSlotNum:  0,
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

	s.mu.Unlock()

	// 發送 ViewChange 給 leader
	fmt.Println("===== [Simplified] Sending ViewChange to leader =====")
	s.logger.SendTo("ViewChange [Simplified]", viewChange, leader)
	go s.send(viewChangeMessage, leader)

	// 只有首次進入 view change 時才廣播 ViewChangeRequest
	if previousStatus != StatusViewChange {
		for _, member := range members {
			if member != myMember {
				s.logger.SendTo("ViewChangeRequest [Simplified]", viewChangeRequest, member)
				go s.send(viewChangeRequestMessage, member)
			}
		}
	}

	go s.resetTimeout()
}

// handleViewChangeSimplified 簡化版：處理 ViewChange
// Leader 收集 quorum 的 ViewChange 後，直接發送 StartView（不進行 log repair）
func (s *NOPaxos) handleViewChangeSimplified(request *ViewChange) {
	s.logger.ReceiveFrom("ViewChange [Simplified]", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// 檢查 view ID 是否匹配
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		s.logger.Debug("Dropping ViewChange: Views do not match")
		return
	}

	// 檢查狀態是否為 ViewChange
	if s.status != StatusViewChange {
		s.logger.Debug("Dropping ViewChange: Replica status is not ViewChange")
		return
	}

	// 檢查是否為 leader
	if s.getLeader(request.ViewID) != s.cluster.Member() {
		s.logger.Debug("Dropping ViewChange: Replica is not the leader of the requested view")
		fmt.Println("===== [Simplified] DROPPING ViewChange: I am not the leader =====")
		return
	}

	fmt.Println("===== [Simplified] I AM THE LEADER, processing ViewChange =====")

	// 收集 ViewChange
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

	fmt.Printf("===== [Simplified] ViewChange progress: %d/%d (quorum: %d) =====\n",
		len(viewChanges), len(s.cluster.Members()), s.cluster.QuorumSize())

	// 如果達到 quorum，發送 StartView
	if localViewChanged && len(viewChanges) >= s.cluster.QuorumSize() {
		fmt.Println("===== [Simplified] QUORUM REACHED! Sending StartView =====")

		// 找出最大的 lastNormal view
		lastNormal := viewChanges[0].LastNormal
		for _, viewChange := range viewChanges[1:] {
			if viewChange.LastNormal.SessionNum > lastNormal.SessionNum ||
				(viewChange.LastNormal.SessionNum == lastNormal.SessionNum &&
					viewChange.LastNormal.LeaderNum > lastNormal.LeaderNum) {
				lastNormal = viewChange.LastNormal
			}
		}

		// 計算新的 messageID（使用最大的 messageNum）
		var newMessageID MessageID
		for _, viewChange := range viewChanges {
			if viewChange.LastNormal.SessionNum == lastNormal.SessionNum &&
				viewChange.LastNormal.LeaderNum == lastNormal.LeaderNum {
				if viewChange.MessageNum > newMessageID {
					newMessageID = viewChange.MessageNum
				}
			}
		}

		// 簡化版：直接發送 StartView，不進行 log repair
		s.sendStartViewSimplified(newMessageID)
	} else {
		fmt.Println("===== [Simplified] NOT starting new view: quorum not reached =====")
	}
}

// sendStartViewSimplified 簡化版：發送 StartView
// 不包含 log 信息，僅通知切換到新 view
func (s *NOPaxos) sendStartViewSimplified(newMessageID MessageID) {
	// 簡化版：不需要 no-op filter，直接發送空的 StartView
	startView := &StartView{
		Sender:          s.cluster.Member(),
		ViewID:          s.viewID,
		MessageNum:      newMessageID,
		NoOpFilter:      nil, // 簡化版不需要
		FirstLogSlotNum: 0,   // 簡化版不需要
		LastLogSlotNum:  0,   // 簡化版不需要
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_StartView{
			StartView: startView,
		},
	}

	fmt.Println("===== [Simplified] SENDING StartView to all replicas =====")
	fmt.Printf("===== [Simplified] New View: Session=%d, Leader=%d =====\n",
		s.viewID.SessionNum, s.viewID.LeaderNum)

	// 發送給所有節點
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("StartView [Simplified]", startView, member)
		fmt.Printf("===== [Simplified] Sending StartView to %v =====\n", member)
		go s.send(message, member)
	}
}

// handleStartViewSimplified 簡化版：處理 StartView
// 直接切換到 Normal 狀態，不進行 log repair
func (s *NOPaxos) handleStartViewSimplified(request *StartView) {
	fmt.Println("===== [Simplified] handleStartView =====")
	s.logger.ReceiveFrom("StartView [Simplified]", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果本地 view 比請求的 view 新，忽略
	if s.viewID.SessionNum > request.ViewID.SessionNum || s.viewID.LeaderNum > request.ViewID.LeaderNum {
		fmt.Println("===== [Simplified] Dropping StartView: Local view is newer =====")
		return
	}

	// 如果 view 匹配且已經是 Normal 狀態，忽略
	if s.viewID.SessionNum == request.ViewID.SessionNum &&
		s.viewID.LeaderNum == request.ViewID.LeaderNum &&
		s.status != StatusViewChange {
		fmt.Println("===== [Simplified] Dropping StartView: Already in Normal status =====")
		return
	}

	// 簡化版：直接切換到 Normal 狀態
	s.sessionMessageNum = request.MessageNum
	s.setStatus(StatusNormal)
	s.viewID = request.ViewID
	s.lastNormView = request.ViewID

	fmt.Printf("===== [Simplified] View Change Complete! New View: Session=%d, Leader=%d =====\n",
		s.viewID.SessionNum, s.viewID.LeaderNum)

	// 如果本節點是新 leader
	if s.getLeader(s.viewID) == s.cluster.Member() {
		fmt.Println("===== [Simplified] I am the new leader! =====")
		fmt.Println("===== [Simplified] Will sync from peer (handled by chain.go) =====")
	} else {
		fmt.Println("===== [Simplified] I am a follower in the new view =====")
	}

	go s.resetTimeout()
}

// handleViewChangeRepairSimplified 簡化版：不需要處理 repair（佔位函數）
func (s *NOPaxos) handleViewChangeRepairSimplified(request *ViewChangeRepair) {
	// 簡化版不需要 repair，這個函數不應該被調用
	fmt.Println("===== [Simplified] ViewChangeRepair not used in simplified mode =====")
}

// handleViewChangeRepairReplySimplified 簡化版：不需要處理 repair reply（佔位函數）
func (s *NOPaxos) handleViewChangeRepairReplySimplified(reply *ViewChangeRepairReply) {
	// 簡化版不需要 repair，這個函數不應該被調用
	fmt.Println("===== [Simplified] ViewChangeRepairReply not used in simplified mode =====")
}

// handleViewRepairSimplified 簡化版：不需要處理 view repair（佔位函數）
func (s *NOPaxos) handleViewRepairSimplified(request *ViewRepair) {
	// 簡化版不需要 repair，這個函數不應該被調用
	fmt.Println("===== [Simplified] ViewRepair not used in simplified mode =====")
}

// handleViewRepairReplySimplified 簡化版：不需要處理 view repair reply（佔位函數）
func (s *NOPaxos) handleViewRepairReplySimplified(reply *ViewRepairReply) {
	// 簡化版不需要 repair，這個函數不應該被調用
	fmt.Println("===== [Simplified] ViewRepairReply not used in simplified mode =====")
}
