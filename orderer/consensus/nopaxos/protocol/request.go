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

	"github.com/gogo/protobuf/proto"
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
	fmt.Println("=====CommandRequest Receive=====")

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the replica's status is not Normal, skip the commit
	// 這裡要去處理 status != normal 的情況，不能讓這時候收到的 txn 被 commit，不然會造成 consensus 錯誤
	if s.status != StatusNormal {
		s.logger.Trace("Dropping CommandRequest: Replica status is not Normal")
		return
	}

	if request.SessionNum == s.viewID.SessionNum && request.MessageNum == s.sessionMessageNum {
		fmt.Println("=====CommandRequest Receive in the normal case=====")
		// Command received in the normal case
		slotNum := s.log.LastSlot() + 1
		entry := &NewLogEntry{
			&LogEntry{
				SlotNum:    slotNum,
				Timestamp:  request.Timestamp,
				MessageNum: request.MessageNum,
				Value:      request.Value,
			},
			request.ConfigSeq,
		}
		s.log.Set(entry)

		if s.getLeader(s.viewID) == s.cluster.Member() {
			s.canCommit = true
		}

		// Apply the command to the state machine before responding if leader

		if stream != nil {
			// 這裡的 stream 因為 command 的設定都是 nil ，所以不會有任何操作
			// 這裡的目的：回傳 response 給 client (但我這裡不需要)
			if s.getLeader(s.viewID) == s.cluster.Member() {
				ch := make(chan Result)
				viewID := s.viewID
				go func() {
					for result := range ch {
						indexed := &Indexed{}
						if err := proto.Unmarshal(result.Value.([]byte), indexed); err != nil {
							continue
						}
						commandReply := &CommandReply{
							MessageNum: request.MessageNum,
							Sender:     s.cluster.Member(),
							ViewID:     viewID,
							SlotNum:    LogSlotID(indexed.Index),
							Value:      indexed.Value,
						}
						message := &ClientMessage{
							Message: &ClientMessage_CommandReply{
								CommandReply: commandReply,
							},
						}
						// TODO: Send state machine errors
						s.logger.Send("CommandReply", commandReply)
						if err := stream.Send(message); err != nil {
							s.logger.Error("Failed to send CommandReply")
						}
					}

					commandClose := &CommandClose{
						MessageNum: request.MessageNum,
						ViewID:     s.viewID,
					}
					message := &ClientMessage{
						Message: &ClientMessage_CommandClose{
							CommandClose: commandClose,
						},
					}
					s.logger.Send("CommandClose", commandClose)
					if err := stream.Send(message); err != nil {
						s.logger.Error("Failed to send CommandClose")
					}
				}()
			} else {
				commandReply := &CommandReply{
					MessageNum: request.MessageNum,
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					SlotNum:    slotNum,
				}
				message := &ClientMessage{
					Message: &ClientMessage_CommandReply{
						CommandReply: commandReply,
					},
				}
				s.logger.Send("CommandReply", commandReply)
				if err := stream.Send(message); err != nil {
					s.logger.Error("Failed to send CommandReply")
				}
			}
		}
		s.sessionMessageNum++ // 因爲是 fast path，所以直接加一
	} else if request.SessionNum > s.viewID.SessionNum {
		// 這裡是 session terminated 的情況，應該是 sequencer 結束的時候，這時候有需要 viewchange??
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
		// 這裡是發生 drop 的情況
		fmt.Println("===== drop case =====")
		fmt.Println("request.MessageNum: ", request.MessageNum)
		fmt.Println("s.sessionMessageNum: ", s.sessionMessageNum)
		s.logger.Debug("Received drop notification for %d", s.sessionMessageNum)

		// Drop notification. If leader commit a gap, otherwise ask the leader for the slot
		if s.getLeader(s.viewID) == s.cluster.Member() {
			// 計算實際丟失的訊息數量
			// ex: sessionMessageNum=1, request.MessageNum=3 => 丟失了 2 (message 1 和 2)
			numDropped := int(request.MessageNum - s.sessionMessageNum)

			fmt.Printf("===== Leader handling dropped messages =====\n")
			fmt.Printf("[Leader] Current: lastSlot=%d, sessionMessageNum=%d\n",
				s.log.LastSlot(), s.sessionMessageNum)
			fmt.Printf("[Leader] Received: messageNum=%d, numDropped=%d\n", request.MessageNum, numDropped)

			// 在 NOPaxos 中，slot number 應該等於 message number
			// 所以 message N 應該寫入 slot N
			targetSlot := LogSlotID(request.MessageNum)

			// 確保 log 擴展到 targetSlot
			if targetSlot > s.log.LastSlot() {
				fmt.Printf("[Leader] Extending log from %d to %d\n", s.log.LastSlot(), targetSlot)
				s.log.Extend(targetSlot)
				// 中間的 slots 保持為 gaps（nil entries）
			}

			if numDropped > 0 {
				// 發送 gap commit 給所有 replicas
				// gap slots 從 sessionMessageNum 開始，到 request.MessageNum - 1
				firstGapSlot := LogSlotID(s.sessionMessageNum)
				s.handleMultipleGapsWithoutLock(firstGapSlot, numDropped)
			}

			// 寫入當前收到的訊息到對應的 slot
			entry := &NewLogEntry{
				&LogEntry{
					SlotNum:    targetSlot,
					Timestamp:  request.Timestamp,
					MessageNum: request.MessageNum,
					Value:      request.Value,
				},
				request.ConfigSeq,
			}
			s.log.Set(entry)
			fmt.Printf("[Leader] Set message %d at slot %d\n", request.MessageNum, targetSlot)
			s.canCommit = true
			s.sessionMessageNum = request.MessageNum + 1
		} else {
			// Follower 處理 drop：確保 log 擴展到當前 message 的位置
			fmt.Printf("===== Follower handling dropped messages =====\n")
			fmt.Printf("[Follower] Current: lastSlot=%d, sessionMessageNum=%d\n",
				s.log.LastSlot(), s.sessionMessageNum)
			fmt.Printf("[Follower] Received: messageNum=%d\n", request.MessageNum)

			// 在 NOPaxos 中，slot number 應該等於 message number
			// 所以 message N 應該寫入 slot N
			targetSlot := LogSlotID(request.MessageNum)

			// 確保 log 擴展到 targetSlot
			if targetSlot > s.log.LastSlot() {
				fmt.Printf("[Follower] Extending log from %d to %d\n", s.log.LastSlot(), targetSlot)
				s.log.Extend(targetSlot)
				// 中間的 slots 保持為 gaps（nil entries）
			}

			// 寫入當前收到的訊息到對應的 slot
			entry := &NewLogEntry{
				&LogEntry{
					SlotNum:    targetSlot,
					Timestamp:  request.Timestamp,
					MessageNum: request.MessageNum,
					Value:      request.Value,
				},
				request.ConfigSeq,
			}
			s.log.Set(entry)
			fmt.Printf("[Follower] Set message %d at slot %d\n", request.MessageNum, targetSlot)

			// 計算有多少 gaps（從 sessionMessageNum 到 request.MessageNum）
			numGaps := int(request.MessageNum - s.sessionMessageNum)
			fmt.Printf("[Follower] Created %d gaps for messages %d-%d\n",
				numGaps, s.sessionMessageNum, request.MessageNum-1)

			// 更新 sessionMessageNum
			s.sessionMessageNum = request.MessageNum + 1

			// 發送 SlotLookup 請求給 leader，嘗試填充 gaps
			leader := s.getLeader(s.viewID)
			slotLookup := &SlotLookup{
				Sender:         s.cluster.Member(),
				ViewID:         s.viewID,
				MessageNum:     request.MessageNum,
				LastMessageNum: request.MessageNum - MessageID(numGaps) - 1, // 最後成功收到的 message num
			}
			message := &ReplicaMessage{
				Message: &ReplicaMessage_SlotLookup{
					SlotLookup: slotLookup,
				},
			}
			s.logger.SendTo("SlotLookup", slotLookup, leader)
			go s.send(message, leader)
		}
	}
	// else if request.SessionNum == s.viewID.SessionNum && request.MessageNum < s.sessionMessageNum {
	// 	// 情況 4：重傳的舊訊息（從 leader 重新發送的缺失訊息）
	// 	fmt.Println("===== retransmission case =====")
	// 	fmt.Printf("[Retransmission] request.MessageNum=%d, s.sessionMessageNum=%d\n",
	// 		request.MessageNum, s.sessionMessageNum)

	// 	// 在 NOPaxos 中，slot number = message number
	// 	// 所以 message N 應該在 slot N
	// 	slotNum := LogSlotID(request.MessageNum)

	// 	fmt.Printf("[Retransmission] Message %d should be at slot %d (lastSlot=%d)\n",
	// 		request.MessageNum, slotNum, s.log.LastSlot())

	// 	// 檢查這個 slot 是否在有效範圍內且是空的（gap）
	// 	if slotNum >= s.log.FirstSlot() && slotNum <= s.log.LastSlot() {
	// 		existingEntry := s.log.Get(slotNum)
	// 		if existingEntry == nil {
	// 			// 這個 slot 是空的（gap），填充它
	// 			entry := &NewLogEntry{
	// 				&LogEntry{
	// 					SlotNum:    slotNum,
	// 					Timestamp:  request.Timestamp,
	// 					MessageNum: request.MessageNum,
	// 					Value:      request.Value,
	// 				},
	// 				request.ConfigSeq,
	// 			}
	// 			s.log.Set(entry)
	// 			fmt.Printf("[Retransmission] ✓ Filled gap at slot %d with message %d\n",
	// 				slotNum, request.MessageNum)
	// 		} else {
	// 			fmt.Printf("[Retransmission] Slot %d already has message %d, skipping\n",
	// 				slotNum, existingEntry.MessageNum)
	// 		}
	// 	} else {
	// 		fmt.Printf("[Retransmission] ✗ Slot %d out of range [%d, %d], ignoring\n",
	// 			slotNum, s.log.FirstSlot(), s.log.LastSlot())
	// 	}
	// 	// 不更新 sessionMessageNum，因為這是舊訊息
	// }
	//暫時丟棄
	s.log.PrintLog("AFTER_SET_ENTRY")
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
