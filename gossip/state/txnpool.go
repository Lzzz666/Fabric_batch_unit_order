/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/gossip/util"
	"google.golang.org/protobuf/proto"
)

// TxnPool defines the interface for transaction pool operations
type TxnPool interface {
	// Put stores an envelope with its hash and TTL
	Put(hash []byte, envelope *common.Envelope, ttl time.Duration) error
	// Get retrieves an envelope by its hash
	Get(hash []byte) (*common.Envelope, bool)
	// GetBatch retrieves multiple envelopes by their hashes
	// Returns found envelopes and missing hashes
	GetBatch(hashes [][]byte) ([]*common.Envelope, [][]byte)
	// Size returns the number of transactions in the pool
	Size() int
	// Stop stops the cleanup goroutine
	Stop()
}

// txnEntry represents a transaction entry with expiration time
type txnEntry struct {
	envelope  *common.Envelope
	expiresAt time.Time
}

// TxnPoolImpl implements TxnPool interface
type TxnPoolImpl struct {
	logger       util.Logger
	mu           sync.RWMutex
	pool         map[string]*txnEntry // hash (hex) -> entry
	defaultTTL   time.Duration
	cleanupTick  time.Duration
	stopCh       chan struct{}
	once         sync.Once
}

// DefaultTxnPoolTTL is the default TTL for transactions in the pool
const DefaultTxnPoolTTL = 5 * time.Minute

// DefaultCleanupInterval is the default interval for cleanup goroutine
const DefaultCleanupInterval = 30 * time.Second

// Global TxnPool registry for sharing between Gateway and GossipStateProvider
var (
	globalTxnPoolMu   sync.RWMutex
	globalTxnPoolMap  = make(map[string]*TxnPoolImpl) // channelID -> TxnPool
)

// GetGlobalTxnPool returns the global TxnPool for a channel, creating one if necessary
func GetGlobalTxnPool(channelID string, logger util.Logger) *TxnPoolImpl {
	globalTxnPoolMu.Lock()
	defer globalTxnPoolMu.Unlock()

	if pool, exists := globalTxnPoolMap[channelID]; exists {
		fmt.Printf("🔵 [GlobalTxnPool] 返回已存在的 TxnPool (channel=%s, pool_size=%d)\n",
			channelID, pool.Size())
		return pool
	}

	pool := NewTxnPoolWithConfig(logger, DefaultTxnPoolTTL, DefaultCleanupInterval)
	globalTxnPoolMap[channelID] = pool
	fmt.Printf("🟢 [GlobalTxnPool] 創建新的 TxnPool (channel=%s)\n", channelID)
	return pool
}

// GetGlobalTxnPoolIfExists returns the global TxnPool for a channel if it exists
func GetGlobalTxnPoolIfExists(channelID string) (*TxnPoolImpl, bool) {
	globalTxnPoolMu.RLock()
	defer globalTxnPoolMu.RUnlock()
	pool, exists := globalTxnPoolMap[channelID]
	if exists {
		fmt.Printf("🔵 [GlobalTxnPool] IfExists: channel=%s, found=true, pool_size=%d\n",
			channelID, pool.Size())
	} else {
		fmt.Printf("🔴 [GlobalTxnPool] IfExists: channel=%s, found=false\n", channelID)
	}
	return pool, exists
}

// NewTxnPool creates a new TxnPool instance
func NewTxnPool(logger util.Logger) *TxnPoolImpl {
	return NewTxnPoolWithConfig(logger, DefaultTxnPoolTTL, DefaultCleanupInterval)
}

// NewTxnPoolWithConfig creates a new TxnPool instance with custom configuration
func NewTxnPoolWithConfig(logger util.Logger, defaultTTL, cleanupInterval time.Duration) *TxnPoolImpl {
	tp := &TxnPoolImpl{
		logger:      logger,
		pool:        make(map[string]*txnEntry),
		defaultTTL:  defaultTTL,
		cleanupTick: cleanupInterval,
		stopCh:      make(chan struct{}),
	}

	// Start cleanup goroutine
	go tp.cleanupLoop()

	logger.Infof("TxnPool initialized with TTL=%v, cleanup interval=%v", defaultTTL, cleanupInterval)
	return tp
}

// Put stores an envelope with its hash and TTL
// Returns nil if stored successfully, skips if already exists (deduplication)
func (tp *TxnPoolImpl) Put(hash []byte, envelope *common.Envelope, ttl time.Duration) error {
	if ttl == 0 {
		ttl = tp.defaultTTL
	}

	hashKey := hex.EncodeToString(hash)

	tp.mu.Lock()
	defer tp.mu.Unlock()

	// 🔥 去重：如果已存在且未過期，跳過
	if entry, exists := tp.pool[hashKey]; exists {
		if time.Now().Before(entry.expiresAt) {
			// 已存在且未過期，跳過
			return nil
		}
		// 已過期，刪除後重新添加
	}

	tp.pool[hashKey] = &txnEntry{
		envelope:  envelope,
		expiresAt: time.Now().Add(ttl),
	}

	return nil
}

// Get retrieves an envelope by its hash and removes it from the pool (consume once)
func (tp *TxnPoolImpl) Get(hash []byte) (*common.Envelope, bool) {
	hashKey := hex.EncodeToString(hash)

	tp.mu.Lock() // 使用寫鎖，因為 HIT 後要刪除
	defer tp.mu.Unlock()

	entry, found := tp.pool[hashKey]
	if !found {
		// 🔥 Debug: 打印未找到信息
		fmt.Printf("🔴 [TxnPool] GET MISS: hash=%s..., pool_size=%d\n",
			hashKey[:minInt(16, len(hashKey))], len(tp.pool))
		return nil, false
	}

	// Check if expired
	if time.Now().After(entry.expiresAt) {
		fmt.Printf("🟡 [TxnPool] GET EXPIRED: hash=%s...\n", hashKey[:16])
		delete(tp.pool, hashKey) // 刪除過期的
		return nil, false
	}

	// 🔥 HIT 後立即刪除，釋放內存
	env := entry.envelope
	delete(tp.pool, hashKey)

	// 🔥 Debug: 打印找到信息
	fmt.Printf("🟢 [TxnPool] GET HIT & REMOVED: hash=%s..., pool_size=%d\n",
		hashKey[:16], len(tp.pool))
	return env, true
}

// GetBatch retrieves multiple envelopes by their hashes and removes found ones from the pool
// Returns found envelopes (in order, with nil for missing) and list of missing hashes
func (tp *TxnPoolImpl) GetBatch(hashes [][]byte) ([]*common.Envelope, [][]byte) {
	tp.mu.Lock() // 使用寫鎖，因為 HIT 後要刪除
	defer tp.mu.Unlock()

	now := time.Now()
	envelopes := make([]*common.Envelope, len(hashes))
	var missingHashes [][]byte

	for i, hash := range hashes {
		hashKey := hex.EncodeToString(hash)
		entry, found := tp.pool[hashKey]

		if !found || now.After(entry.expiresAt) {
			missingHashes = append(missingHashes, hash)
			envelopes[i] = nil
			// 刪除過期的
			if found && now.After(entry.expiresAt) {
				delete(tp.pool, hashKey)
			}
		} else {
			envelopes[i] = entry.envelope
			// 🔥 HIT 後立即刪除
			delete(tp.pool, hashKey)
		}
	}

	tp.logger.Debugf("TxnPool: batch get %d hashes, found %d, missing %d, pool_size=%d",
		len(hashes), len(hashes)-len(missingHashes), len(missingHashes), len(tp.pool))

	return envelopes, missingHashes
}

// Size returns the number of transactions in the pool
func (tp *TxnPoolImpl) Size() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return len(tp.pool)
}

// Stop stops the cleanup goroutine
func (tp *TxnPoolImpl) Stop() {
	tp.once.Do(func() {
		close(tp.stopCh)
		tp.logger.Info("TxnPool stopped")
	})
}

// cleanupLoop periodically removes expired entries
func (tp *TxnPoolImpl) cleanupLoop() {
	ticker := time.NewTicker(tp.cleanupTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tp.cleanup()
		case <-tp.stopCh:
			return
		}
	}
}

// cleanup removes expired entries from the pool
func (tp *TxnPoolImpl) cleanup() {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	for hashKey, entry := range tp.pool {
		if now.After(entry.expiresAt) {
			delete(tp.pool, hashKey)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		tp.logger.Debugf("TxnPool: cleanup removed %d expired entries, remaining=%d", expiredCount, len(tp.pool))
	}
}

// ComputeEnvelopeHash computes SHA256 hash of a serialized envelope
func ComputeEnvelopeHash(env *common.Envelope) ([]byte, error) {
	data, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return hash[:], nil
}

// HashOnlyBlockFlag is the flag value used to identify hash-only blocks
const HashOnlyBlockFlag = byte(0x01)

// HashOnlyBlockMetadataIndex is the metadata index for hash-only block marker
const HashOnlyBlockMetadataIndex = 4 // Using index 4 (after SIGNATURES, LAST_CONFIG, TRANSACTIONS_FILTER, ORDERER)

// IsHashOnlyBlock checks if a block contains only transaction hashes
func IsHashOnlyBlock(block *common.Block) bool {
	if block == nil || block.Metadata == nil {
		fmt.Printf("🔍 [IsHashOnlyBlock] block or metadata is nil\n")
		return false
	}

	// Check metadata for hash-only marker
	metadataLen := len(block.Metadata.Metadata)
	fmt.Printf("🔍 [IsHashOnlyBlock] Block #%d: metadata_count=%d, need_index=%d\n",
		block.Header.Number, metadataLen, HashOnlyBlockMetadataIndex)

	if metadataLen > HashOnlyBlockMetadataIndex {
		marker := block.Metadata.Metadata[HashOnlyBlockMetadataIndex]
		fmt.Printf("🔍 [IsHashOnlyBlock] Block #%d: marker_len=%d, marker=%v\n",
			block.Header.Number, len(marker), marker)
		if len(marker) > 0 && marker[0] == HashOnlyBlockFlag {
			fmt.Printf("✅ [IsHashOnlyBlock] Block #%d IS hash-only (flag=0x%02X)\n",
				block.Header.Number, HashOnlyBlockFlag)
			return true
		}
	}

	fmt.Printf("❌ [IsHashOnlyBlock] Block #%d is NOT hash-only\n", block.Header.Number)
	return false
}

// MarkBlockAsHashOnly marks a block as containing only transaction hashes
func MarkBlockAsHashOnly(block *common.Block) {
	if block == nil {
		return
	}

	// Ensure metadata slice is large enough
	for len(block.Metadata.Metadata) <= HashOnlyBlockMetadataIndex {
		block.Metadata.Metadata = append(block.Metadata.Metadata, nil)
	}

	// Set the hash-only marker
	block.Metadata.Metadata[HashOnlyBlockMetadataIndex] = []byte{HashOnlyBlockFlag}
}

// minInt returns the smaller of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
