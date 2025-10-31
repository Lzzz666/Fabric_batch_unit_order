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

func (s *NOPaxos) sendPing() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// If the replica is recovering, do not send the ping
	if s.status == StatusRecovering || s.status == StatusViewChange {
		return
	}

	// If this replica is not the leader of the view, do not send the ping
	if s.getLeader(s.viewID) != s.cluster.Member() {
		return
	}

	ping := &Ping{
		Sender: s.cluster.Member(),
		ViewID: s.viewID,
	}
	message := &ReplicaMessage{
		Message: &ReplicaMessage_Ping{
			Ping: ping,
		},
	}

	for _, member := range s.cluster.Members() {
		if member != s.cluster.Member() {
			s.logger.SendTo("Ping", ping, member)
			go s.send(message, member)
		}
	}
}

func (s *NOPaxos) handlePing(request *Ping) {
	s.logger.ReceiveFrom("Ping", request, request.Sender)

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the view IDs do not match, ignore the request
	if s.viewID.LeaderNum != request.ViewID.LeaderNum || s.viewID.SessionNum != request.ViewID.SessionNum {
		return
	}

	// If the replica's status is not Normal, ignore the request
	if s.status != StatusNormal {
		return
	}

	go s.resetTimeout()
}

func (s *NOPaxos) Timeout() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.status == StatusRecovering {
		go s.startRecovery()
		go s.resetTimeout()
	} else if s.status == StatusViewChange {
		// If still in ViewChange state, the previous view change may have failed
		// Start a new view change to try the next leader
		s.logger.Debug("ViewChange timed out, starting new view change")
		fmt.Println("====================ViewChange timed out, startLeaderChange====================")
		go s.startLeaderChange()
		go s.resetTimeout()
	} else if s.getLeader(s.viewID) != s.cluster.Member() {
		s.logger.Debug("Leader ping timed out")
		fmt.Println("====================Leader ping timed out, startLeaderChange====================")
		go s.startLeaderChange()
		go s.resetTimeout()
	} else {
		go s.resetTimeout()
	}
}
