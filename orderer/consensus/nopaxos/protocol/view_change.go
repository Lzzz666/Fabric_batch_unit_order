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
	"math"

	"github.com/willf/bloom"
)

func (s *NOPaxos) startLeaderChange() {
	fmt.Println("====================startLeaderChange====================")
	s.mu.RLock()
	newViewID := &ViewId{
		SessionNum: s.viewID.SessionNum,
		LeaderNum:  s.viewID.LeaderNum + 1,
	}
	s.mu.RUnlock()

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
		fmt.Println("=====send ViewChangeRequest to", member, "=====")
		fmt.Println("=====message=====")
		fmt.Println(message)
		fmt.Println("========================================================")
		go s.send(message, member)
	}

	go s.resetTimeout()
}

func (s *NOPaxos) handleViewChangeRequest(request *ViewChangeRequest) {
	fmt.Println("=====handleViewChangeRequest=====")
	fmt.Println("=====request.Sender=====")
	fmt.Println(request.Sender)
	fmt.Println("=====request.ViewID=====")
	fmt.Println(request.ViewID)
	fmt.Println("========================================================")
	fmt.Println("=====s.viewID=====")
	fmt.Println(s.viewID)
	fmt.Println("========================================================")
	s.logger.ReceiveFrom("ViewChangeRequest", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the replica is recovering, ignore the view change
	if s.status == StatusRecovering {
		fmt.Println("=====s.status == StatusRecovering, return=====")
		return
	}

	fmt.Println("=====s.status != StatusRecovering, continue=====")
	newLeaderID := LeaderID(math.Max(float64(s.viewID.LeaderNum), float64(request.ViewID.LeaderNum)))
	newSessionID := SessionID(math.Max(float64(s.viewID.SessionNum), float64(request.ViewID.SessionNum)))
	newViewID := &ViewId{
		LeaderNum:  newLeaderID,
		SessionNum: newSessionID,
	}

	fmt.Println("=====newViewID=====")
	fmt.Println(newViewID)
	fmt.Println("========================================================")

	// If the view IDs match, ignore the request
	if s.viewID.LeaderNum == newViewID.LeaderNum && s.viewID.SessionNum == newViewID.SessionNum {
		s.logger.Debug("Dropping ViewChangeRequest: Already in the requested view")
		return
	}
	fmt.Println("=====s.viewID.LeaderNum == newViewID.LeaderNum && s.viewID.SessionNum == newViewID.SessionNum, return=====")
	// Set the replica's status to ViewChange
	s.setStatus(StatusViewChange)
	fmt.Println("=====s.setStatus(StatusViewChange)=====")
	// Set the replica's view ID to the new view ID
	s.viewID = newViewID

	fmt.Println("=====s.viewID = newViewID=====")
	fmt.Println(s.viewID)
	fmt.Println("========================================================")

	// Reset the view changes
	s.viewChanges = make(map[MemberID]*ViewChange)
	fmt.Println("=====s.viewChanges = make(map[MemberID]*ViewChange)=====")
	fmt.Println(s.viewChanges)
	fmt.Println("========================================================")
	// Create a bloom filter of the log and add non-empty entries
	noOpFilter := bloom.New(uint(s.log.LastSlot()-s.log.FirstSlot()+1), bloomFilterHashFunctions)
	for slotNum := s.log.FirstSlot(); slotNum <= s.log.LastSlot(); slotNum++ {
		if entry := s.log.Get(slotNum); entry == nil {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(slotNum))
			noOpFilter.Add(key)
		}
	}

	fmt.Println("=====noOpFilter=====")
	fmt.Println(noOpFilter)
	fmt.Println("========================================================")

	// Marshall the bloom filter to bytes
	noOpFilterBytes, err := json.Marshal(noOpFilter)
	if err != nil {
		s.logger.Error("Failed to encode bloom filter", err)
		return
	}

	// Send a ViewChange message to the leader
	leader := s.getLeader(newViewID)
	viewChange := &ViewChange{
		Sender:          s.cluster.Member(),
		ViewID:          newViewID,
		LastNormal:      s.lastNormView,
		MessageNum:      s.sessionMessageNum,
		NoOpFilter:      noOpFilterBytes,
		FirstLogSlotNum: s.log.FirstSlot(),
		LastLogSlotNum:  s.log.LastSlot(),
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_ViewChange{
			ViewChange: viewChange,
		},
	}
	s.logger.SendTo("ViewChange", viewChange, leader)
	go s.send(message, leader)
	fmt.Println("=====send ViewChange to leader=====")
	fmt.Println("=====leader=====")
	fmt.Println(leader)
	fmt.Println("=====message=====")
	fmt.Println(message)
	fmt.Println("========================================================")
	// Send a ViewChangeRequest to all other replicas
	viewChangeRequest := &ViewChangeRequest{
		Sender: s.cluster.Member(),
		ViewID: newViewID,
	}
	message = &ReplicaMessage{
		Message: &ReplicaMessage_ViewChangeRequest{
			ViewChangeRequest: viewChangeRequest,
		},
	}
	fmt.Println("=====viewChangeRequest=====")
	fmt.Println(viewChangeRequest)
	fmt.Println("========================================================")
	// Send a view change request to all replicas other than the leader
	for _, member := range s.cluster.Members() {
		s.logger.SendTo("ViewChangeRequest", viewChangeRequest, member)
		go s.send(message, member)
	}
}

func (s *NOPaxos) handleViewChange(request *ViewChange) {
	fmt.Println("=====handleViewChange=====")
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
		return
	}

	// Add the view change to the set of view changes
	fmt.Println("=====s.viewChanges[request.Sender] = request=====")
	fmt.Println(request)
	fmt.Println("========================================================")
	s.viewChanges[request.Sender] = request

	// Aggregate the view changes for the current view
	fmt.Println("=====s.viewChanges=====")
	fmt.Println(s.viewChanges)
	fmt.Println("========================================================")
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
	if localViewChanged && len(viewChanges) >= s.cluster.QuorumSize() {
		// Create the state for the new view
		var lastNormal *ViewId
		for _, viewChange1 := range viewChanges {
			normal := true
			for _, viewChange2 := range viewChanges {
				if viewChange2.LastNormal.SessionNum > viewChange1.LastNormal.SessionNum || viewChange2.LastNormal.LeaderNum > viewChange1.LastNormal.LeaderNum {
					normal = false
					break
				}
			}
			if normal {
				lastNormal = viewChange1.LastNormal
			}
		}

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

		newLog := newLog(minSlotNum)
		noOpSlots := make(map[MemberID][]LogSlotID)
		for slotNum := minSlotNum; slotNum <= maxSlotNum; slotNum++ {
			// If the entry is missing from the local log, it's a no-op.
			// If the entry is present, check no-op filters to determine whether to request
			// a repair from peers.
			if entry := s.log.Get(slotNum); entry != nil {
				newLog.Set(entry)

				// For each member, if the no-op is present in the member's filter request a repair.
				for member, noOpFilter := range noOpFilters {
					if noOpFilter.isMaybeNoOp(slotNum) {
						slots := noOpSlots[member]
						if slots == nil {
							slots = make([]LogSlotID, 0)
						}
						noOpSlots[member] = append(slots, slotNum)
					}
				}
			}
		}

		// Set the view change log. Note this log is maintained separate from the primary log until the view is started.
		s.viewLog = newLog

		// If there are any missing slots in the log, store the new log and send LogRepair requests to peers to
		// determine whether a no-op entry should be written to the log. Otherwise, send a StartView.
		if len(noOpSlots) > 0 || maxCheckpoint > 0 {
			s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
			for member, slots := range noOpSlots {
				repair := &ViewChangeRepair{
					Sender:     s.cluster.Member(),
					ViewID:     s.viewID,
					MessageNum: newMessageID,
					Checkpoint: maxCheckpoint,
					SlotNums:   slots,
				}
				message := &ReplicaMessage{
					Message: &ReplicaMessage_ViewChangeRepair{
						ViewChangeRepair: repair,
					},
				}
				s.logger.SendTo("ViewChangeRepair", repair, member)
				go s.send(message, member)
			}
		} else {
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
			for _, member := range s.cluster.Members() {
				s.logger.SendTo("StartView", startView, member)
				go s.send(message, member)
			}
		}
	}
}

func (s *NOPaxos) handleViewChangeRepair(request *ViewChangeRepair) {
	fmt.Println("=====handleViewChangeRepair=====")
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the request views do not match, ignore the request
	if s.viewID.SessionNum != request.ViewID.SessionNum || s.viewID.LeaderNum != request.ViewID.LeaderNum {
		s.logger.Debug("Dropping ViewChangeRepair: Views do not match")
		return
	}

	// Lookup entries for the requested slots. For any entry that's present in the log,
	// append the slot num.
	slots := make([]LogSlotID, 0, len(request.SlotNums))
	for _, slotNum := range request.SlotNums {
		if entry := s.log.Get(slotNum); entry != nil {
			slots = append(slots, slotNum)
		}
	}

	// If this replica's checkpoint was requested, send the checkpoint
	var checkpointSlotNum LogSlotID
	var checkpointData []byte
	if request.Checkpoint > 0 && s.currentCheckpoint != nil && request.Checkpoint <= s.currentCheckpoint.SlotNum {
		checkpointSlotNum = s.currentCheckpoint.SlotNum
		checkpointData = s.currentCheckpoint.Data
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
	go s.send(message, request.Sender)
}

func (s *NOPaxos) handleViewChangeRepairReply(reply *ViewChangeRepairReply) {
	fmt.Println("=====handleViewChangeRepairReply=====")
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
	}

	// If all view repairs have been responded to, remove entries where any slot is empty
	// and populate slots where all entries have been returned
	if len(s.viewChangeRepairs) == len(s.viewChangeRepairReps) {
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

		// For each slot, remove entries where the replies do not equal the requests
		for slotNum, slot := range slots {
			if slot.requests != slot.replies {
				s.viewLog.Delete(slotNum)
			}
		}

		// Create a new no-op filter and add no-op entries
		noOpFilter := newNoOpFilter(s.viewLog.FirstSlot(), s.viewLog.LastSlot())
		for slotNum := s.viewLog.FirstSlot(); slotNum <= s.viewLog.LastSlot(); slotNum++ {
			if entry := s.viewLog.Get(slotNum); entry == nil {
				noOpFilter.add(slotNum)
			}
		}

		// Marshal the no-op filter to JSON
		noOpFilterJson, err := noOpFilter.marshal()
		if err != nil {
			s.logger.Error("Failed to marshal no-op filter", err)
			return
		}

		// Create and send a StartView message to each replica with the no-op filter
		startView := &StartView{
			Sender:          s.cluster.Member(),
			ViewID:          s.viewID,
			MessageNum:      reply.MessageNum,
			NoOpFilter:      noOpFilterJson,
			FirstLogSlotNum: s.viewLog.FirstSlot(),
			LastLogSlotNum:  s.viewLog.LastSlot(),
		}
		message := &ReplicaMessage{
			Message: &ReplicaMessage_StartView{
				StartView: startView,
			},
		}

		// Send a StartView to each replica
		for _, member := range s.cluster.Members() {
			s.logger.SendTo("StartView", startView, member)
			go s.send(message, member)
		}

		// Unset repair fields
		s.viewChangeRepairs = make(map[MemberID]*ViewChangeRepair)
		s.viewChangeRepairReps = make(map[MemberID]*ViewChangeRepairReply)
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
	return json.Marshal(newNoOpFilter)
}

func (f *noOpFilter) unmarshal(bytes []byte) error {
	f.filter = &bloom.BloomFilter{}
	if err := json.Unmarshal(bytes, f.filter); err != nil {
		return err
	}
	return nil
}
