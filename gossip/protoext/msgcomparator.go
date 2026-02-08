/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoext

import (
	"bytes"

	"github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/common"
)

// NewGossipMessageComparator creates a MessageReplacingPolicy given a maximum number of blocks to hold
func NewGossipMessageComparator(dataBlockStorageSize int) common.MessageReplacingPolicy {
	return (&msgComparator{dataBlockStorageSize: dataBlockStorageSize}).getMsgReplacingPolicy()
}

type msgComparator struct {
	dataBlockStorageSize int
}

func (mc *msgComparator) getMsgReplacingPolicy() common.MessageReplacingPolicy {
	return func(this interface{}, that interface{}) common.InvalidationResult {
		return mc.invalidationPolicy(this, that)
	}
}

func (mc *msgComparator) invalidationPolicy(this interface{}, that interface{}) common.InvalidationResult {
	thisMsg := this.(*SignedGossipMessage)
	thatMsg := that.(*SignedGossipMessage)

	if IsAliveMsg(thisMsg.GossipMessage) && IsAliveMsg(thatMsg.GossipMessage) {
		return aliveInvalidationPolicy(thisMsg.GetAliveMsg(), thatMsg.GetAliveMsg())
	}

	if IsDataMsg(thisMsg.GossipMessage) && IsDataMsg(thatMsg.GossipMessage) {
		return mc.dataInvalidationPolicy(thisMsg.GetDataMsg(), thatMsg.GetDataMsg())
	}

	if IsStateInfoMsg(thisMsg.GossipMessage) && IsStateInfoMsg(thatMsg.GossipMessage) {
		return mc.stateInvalidationPolicy(thisMsg.GetStateInfo(), thatMsg.GetStateInfo())
	}

	if IsIdentityMsg(thisMsg.GossipMessage) && IsIdentityMsg(thatMsg.GossipMessage) {
		return mc.identityInvalidationPolicy(thisMsg.GetPeerIdentity(), thatMsg.GetPeerIdentity())
	}

	if IsLeadershipMsg(thisMsg.GossipMessage) && IsLeadershipMsg(thatMsg.GossipMessage) {
		return leaderInvalidationPolicy(thisMsg.GetLeadershipMsg(), thatMsg.GetLeadershipMsg())
	}

	return common.MessageNoAction
}

func (mc *msgComparator) stateInvalidationPolicy(thisStateMsg *gossip.StateInfo, thatStateMsg *gossip.StateInfo) common.InvalidationResult {
	if !bytes.Equal(thisStateMsg.PkiId, thatStateMsg.PkiId) {
		return common.MessageNoAction
	}
	return compareTimestamps(thisStateMsg.Timestamp, thatStateMsg.Timestamp)
}

func (mc *msgComparator) identityInvalidationPolicy(thisIdentityMsg *gossip.PeerIdentity, thatIdentityMsg *gossip.PeerIdentity) common.InvalidationResult {
	if bytes.Equal(thisIdentityMsg.PkiId, thatIdentityMsg.PkiId) {
		return common.MessageInvalidated
	}

	return common.MessageNoAction
}

// TxnBroadcastMagic is the magic number for transaction broadcast messages
// 0x54584E45 = "TXNE" (Transaction Envelope)
const TxnBroadcastMagic uint32 = 0x54584E45

// isTxnBroadcastMsg checks if a DataMessage is a transaction broadcast message
// by checking the magic number prefix in the payload data
func isTxnBroadcastMsg(dataMsg *gossip.DataMessage) bool {
	if dataMsg == nil || dataMsg.Payload == nil || len(dataMsg.Payload.Data) < 4 {
		return false
	}
	data := dataMsg.Payload.Data
	magic := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	return magic == TxnBroadcastMagic
}

func (mc *msgComparator) dataInvalidationPolicy(thisDataMsg *gossip.DataMessage, thatDataMsg *gossip.DataMessage) common.InvalidationResult {
	// 🔥 Skip comparison for transaction broadcast messages
	// They use special SeqNum that doesn't follow block sequence
	thisIsTxnBroadcast := isTxnBroadcastMsg(thisDataMsg)
	thatIsTxnBroadcast := isTxnBroadcastMsg(thatDataMsg)

	// If one is a txn broadcast and the other is a block, don't invalidate either
	if thisIsTxnBroadcast != thatIsTxnBroadcast {
		return common.MessageNoAction
	}

	// If both are txn broadcasts with same SeqNum (same hash prefix), skip duplicate
	if thisIsTxnBroadcast && thatIsTxnBroadcast {
		if thisDataMsg.Payload.SeqNum == thatDataMsg.Payload.SeqNum {
			return common.MessageInvalidated
		}
		return common.MessageNoAction
	}

	// Both are blocks - original logic
	if thisDataMsg.Payload.SeqNum == thatDataMsg.Payload.SeqNum {
		return common.MessageInvalidated
	}

	diff := abs(thisDataMsg.Payload.SeqNum, thatDataMsg.Payload.SeqNum)
	if diff <= uint64(mc.dataBlockStorageSize) {
		return common.MessageNoAction
	}

	if thisDataMsg.Payload.SeqNum > thatDataMsg.Payload.SeqNum {
		return common.MessageInvalidates
	}
	return common.MessageInvalidated
}

func aliveInvalidationPolicy(thisMsg *gossip.AliveMessage, thatMsg *gossip.AliveMessage) common.InvalidationResult {
	if !bytes.Equal(thisMsg.Membership.PkiId, thatMsg.Membership.PkiId) {
		return common.MessageNoAction
	}

	return compareTimestamps(thisMsg.Timestamp, thatMsg.Timestamp)
}

func leaderInvalidationPolicy(thisMsg *gossip.LeadershipMessage, thatMsg *gossip.LeadershipMessage) common.InvalidationResult {
	if !bytes.Equal(thisMsg.PkiId, thatMsg.PkiId) {
		return common.MessageNoAction
	}

	return compareTimestamps(thisMsg.Timestamp, thatMsg.Timestamp)
}

func compareTimestamps(thisTS *gossip.PeerTime, thatTS *gossip.PeerTime) common.InvalidationResult {
	if thisTS.IncNum == thatTS.IncNum {
		if thisTS.SeqNum > thatTS.SeqNum {
			return common.MessageInvalidates
		}

		return common.MessageInvalidated
	}
	if thisTS.IncNum < thatTS.IncNum {
		return common.MessageInvalidated
	}
	return common.MessageInvalidates
}

// abs returns abs(a-b)
func abs(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
