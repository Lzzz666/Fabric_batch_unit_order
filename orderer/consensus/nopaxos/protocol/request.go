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
)

// Result is a stream result
type Result struct {
	Value interface{}
	Error error
}

type NewCommandRequest struct {
	*CommandRequest
	ConfigSeq uint64
}

// Failed returns a boolean indicating whether the operation failed
func (r Result) Failed() bool {
	return r.Error != nil
}

// Succeeded returns a boolean indicating whether the operation was successful
func (r Result) Succeeded() bool {
	return !r.Failed()
}

func (s *NOPaxos) Command(request *NewCommandRequest, stream ClientService_ClientStreamServer) {
	s.logger.Receive("CommandRequest", request)
	fmt.Printf("[NOPaxos] 📨 收到 CommandRequest (messageNum: %d)\n", request.MessageNum)
	fmt.Printf("[NOPaxos] 🔒 嘗試獲取 s.mu 鎖...\n")

	s.mu.Lock()

	fmt.Printf("[NOPaxos] ✓ 已獲取 s.mu 鎖 (messageNum: %d)\n", request.MessageNum)
	defer func() {
		s.mu.Unlock()
		fmt.Printf("[NOPaxos] 🔓 已釋放 s.mu 鎖 (messageNum: %d)\n", request.MessageNum)
	}()

	// 這裡要去處理 status != normal 的情況，不能讓這時候收到的 txn 被 commit，不然會造成 consensus 錯誤
	if s.status != StatusNormal {
		fmt.Printf("[NOPaxos] ⚠️ 丟棄 CommandRequest: 狀態不是 Normal (messageNum: %d, status: %s)\n", request.MessageNum, s.status)
		s.logger.Trace("Dropping CommandRequest: Replica status is not Normal")
		return
	}

	if request.SessionNum == s.viewID.SessionNum && request.MessageNum == s.sessionMessageNum {
		// === Case 1: 正常順序的消息 ===
		s.processNormalMessage(request)

	} else if request.SessionNum > s.viewID.SessionNum {
		// NOTE: 先 skip 這個情況
		// 這裡是 session terminated 的情況，應該是 sequencer 結束的時候，這時候不需要換 leader，只需要更新新的 session number，不過也先 skip 這個情況
		fmt.Println("=====CommandRequest Receive in the session terminated case (request.SessionNum > s.viewID.SessionNum )=====")
		s.logger.Info("Session %d terminated", s.viewID.SessionNum)
		s.logger.Info("Requesting view change for session %d", request.SessionNum)

		// Command received in the session terminated case
		newViewID := &ViewId{
			SessionNum: request.SessionNum,
			LeaderNum:  s.viewID.LeaderNum,
		}
		fmt.Println("=====viewChangeRequest in the session terminated case=====")
		viewChangeRequest := &ViewChangeRequest{
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
	} else if request.SessionNum == s.viewID.SessionNum && request.MessageNum > s.sessionMessageNum {
		// === Case 3: Drop case（收到未來的消息，可能是亂序）===
		gap := request.MessageNum - s.sessionMessageNum
		fmt.Println("===== drop case =====")
		fmt.Printf("❌ [GAP DETECTED] request.MessageNum: %d, s.sessionMessageNum: %d, GAP: %d\n",
			request.MessageNum, s.sessionMessageNum, gap)
		s.logger.Debug("Received drop notification for %d", s.sessionMessageNum)

		//先將 gap commit 邏輯拿掉
		// ✅ 原有的 drop 處理邏輯（如果 buffer 未啟用或失敗）
		// Drop notification. If leader commit a gap, otherwise ask the leader for the slot
		// if s.getLeader(s.viewID) == s.cluster.Member() {
		// 	// 計算實際丟失的訊息數量
		// 	// ex: sessionMessageNum=1, request.MessageNum=3 => 丟失了 2 (message 1 和 2)
		// 	numDropped := int(request.MessageNum - s.sessionMessageNum)

		// 	fmt.Printf("===== Leader handling dropped messages =====\n")
		// 	fmt.Printf("[Leader] Current: lastSlot=%d, sessionMessageNum=%d\n",
		// 		s.log.LastSlot(), s.sessionMessageNum)
		// 	fmt.Printf("[Leader] Received: messageNum=%d, numDropped=%d\n", request.MessageNum, numDropped)

		// 	// 在 NOPaxos 中，slot number 應該等於 message number
		// 	// 所以 message N 應該寫入 slot N
		// 	targetSlot := LogSlotID(request.MessageNum)

		// 	// 確保 log 擴展到 targetSlot
		// 	if targetSlot > s.log.LastSlot() {
		// 		fmt.Printf("[Leader] Extending log from %d to %d\n", s.log.LastSlot(), targetSlot)
		// 		s.log.Extend(targetSlot)
		// 		// 中間的 slots 保持為 gaps（nil entries）
		// 	}

		// 	if numDropped > 0 {
		// 		// 發送 gap commit 給所有 replicas
		// 		// gap slots 從 sessionMessageNum 開始，到 request.MessageNum - 1
		// 		firstGapSlot := LogSlotID(s.sessionMessageNum)
		// 		s.handleMultipleGapsWithoutLock(firstGapSlot, numDropped)
		// 	}

		// 	// 寫入當前收到的訊息到對應的 slot
		// 	entry := &NewLogEntry{
		// 		&LogEntry{
		// 			SlotNum:    targetSlot,
		// 			Timestamp:  request.Timestamp,
		// 			MessageNum: request.MessageNum,
		// 			Value:      request.Value,
		// 		},
		// 		request.ConfigSeq,
		// 	}
		// 	s.log.Set(entry)
		// 	fmt.Printf("[Leader] Set message %d at slot %d\n", request.MessageNum, targetSlot)
		// 	// s.canCommit = true
		// 	s.sessionMessageNum = request.MessageNum + 1
		// } else {
		// 	// Follower 處理 drop：確保 log 擴展到當前 message 的位置
		// 	fmt.Printf("===== Follower handling dropped messages =====\n")
		// 	fmt.Printf("[Follower] Current: lastSlot=%d, sessionMessageNum=%d\n",
		// 		s.log.LastSlot(), s.sessionMessageNum)
		// 	fmt.Printf("[Follower] Received: messageNum=%d\n", request.MessageNum)

		// 	// 在 NOPaxos 中，slot number 應該等於 message number
		// 	// 所以 message N 應該寫入 slot N
		// 	targetSlot := LogSlotID(request.MessageNum)

		// 	// 確保 log 擴展到 targetSlot
		// 	if targetSlot > s.log.LastSlot() {
		// 		fmt.Printf("[Follower] Extending log from %d to %d\n", s.log.LastSlot(), targetSlot)
		// 		s.log.Extend(targetSlot)
		// 		// 中間的 slots 保持為 gaps（nil entries）
		// 	}

		// 	// 寫入當前收到的訊息到對應的 slot
		// 	entry := &NewLogEntry{
		// 		&LogEntry{
		// 			SlotNum:    targetSlot,
		// 			Timestamp:  request.Timestamp,
		// 			MessageNum: request.MessageNum,
		// 			Value:      request.Value,
		// 		},
		// 		request.ConfigSeq,
		// 	}
		// 	s.log.Set(entry)
		// 	fmt.Printf("[Follower] Set message %d at slot %d\n", request.MessageNum, targetSlot)

		// 	// 計算有多少 gaps（從 sessionMessageNum 到 request.MessageNum）
		// 	numGaps := int(request.MessageNum - s.sessionMessageNum)
		// 	fmt.Printf("[Follower] Created %d gaps for messages %d-%d\n",
		// 		numGaps, s.sessionMessageNum, request.MessageNum-1)

		// 	// 更新 sessionMessageNum
		// 	s.sessionMessageNum = request.MessageNum + 1

		// 	// 發送 SlotLookup 請求給 leader，嘗試填充 gaps
		// 	leader := s.getLeader(s.viewID)
		// 	slotLookup := &SlotLookup{
		// 		Sender:         s.cluster.Member(),
		// 		ViewID:         s.viewID,
		// 		MessageNum:     request.MessageNum,
		// 		LastMessageNum: request.MessageNum - MessageID(numGaps) - 1, // 最後成功收到的 message num
		// 	}
		// 	message := &ReplicaMessage{
		// 		Message: &ReplicaMessage_SlotLookup{
		// 			SlotLookup: slotLookup,
		// 		},
		// 	}
		// 	s.logger.SendTo("SlotLookup", slotLookup, leader)
		// 	go s.send(message, leader)
		// }
	}
	//暫時丟棄
	s.log.PrintLog("AFTER_SET_ENTRY")
}

// processNormalMessage 處理正常順序的消息（從原 Command 函數提取）
func (s *NOPaxos) processNormalMessage(request *NewCommandRequest) {
	// * 先暫時拿掉，先不要寫 log
	fmt.Printf("[lz debug] processNormalMessage: %d\n", request.MessageNum)
	// slotNum := s.log.LastSlot() + 1
	// entry := &NewLogEntry{
	// 	&LogEntry{
	// 		SlotNum:    slotNum,
	// 		Timestamp:  request.Timestamp,
	// 		MessageNum: request.MessageNum,
	// 		Value:      request.Value,
	// 	},
	// 	request.ConfigSeq,
	// }
	// s.log.Set(entry)

	s.sessionMessageNum++ // 因爲是 fast path，所以直接加一
}


func (s *NOPaxos) query(request *QueryRequest, stream ClientService_ClientStreamServer) {
	fmt.Println("=====QueryRequest Receive=====")
	s.logger.Receive("QueryRequest", request)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the replica's status is not Normal, skip the commit
	if s.status != StatusNormal {
		return
	}

	if request.SessionNum == s.viewID.SessionNum && stream != nil && s.getLeader(s.viewID) == s.cluster.Member() {
		ch := make(chan Result)
		go func() {
			for result := range ch {
				// TODO: Send state machine errors
				queryReply := &QueryReply{
					MessageNum: request.MessageNum,
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					Value:      result.Value.([]byte),
				}
				message := &ClientMessage{
					Message: &ClientMessage_QueryReply{
						QueryReply: queryReply,
					},
				}
				s.logger.Send("QueryReply", queryReply)
				if err := stream.Send(message); err != nil {
					s.logger.Error("Failed to send QueryReply")
				}
			}

			queryClose := &QueryClose{
				MessageNum: request.MessageNum,
				ViewID:     s.viewID,
			}
			message := &ClientMessage{
				Message: &ClientMessage_QueryClose{
					QueryClose: queryClose,
				},
			}
			s.logger.Send("QueryClose", queryClose)
			if err := stream.Send(message); err != nil {
				s.logger.Error("Failed to send QueryClose")
			}
		}()
	}
}

func (s *NOPaxos) handleSlot(request *NewCommandRequest) {
	fmt.Println("=====handleSlot=====")
	s.Command(request, nil)
}
