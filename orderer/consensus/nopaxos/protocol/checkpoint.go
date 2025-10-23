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
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
)

func (s *NOPaxos) checkpoint() {
	fmt.Println("=====checkpoint=====")
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only take checkpoints when in Normal status
	if s.status != StatusNormal {
		return
	}

	// Only take checkpoints once the log has exceeded the configured max length
	if s.log.Len() < s.config.GetMaxLogLengthOrDefault() {
		return
	}

	// Checkpoints can only be created for consistent portions of the log, so a checkpoint can only be taken up
	// to the sync point
	if s.applied == 0 || (s.currentCheckpoint != nil && s.currentCheckpoint.SlotNum >= s.applied) {
		return
	}

	// Record the checkpoint
	s.logger.Info("Checkpointing system state")
	checkpoint := newCheckpoint(s.applied)
	s.currentCheckpoint = checkpoint

	// Once the checkpoint has been recorded, remove checkpointed entries from the log
	s.log.Truncate(checkpoint.SlotNum + 1)
}

// newCheckpoint creates a new checkpoint
func newCheckpoint(slotNum LogSlotID) *Checkpoint {
	return &Checkpoint{
		SlotNum: slotNum,
		Data:    make([]byte, 0),
	}
}

// Checkpoint is a checkpoint of the state machine
type Checkpoint struct {
	SlotNum  LogSlotID
	Data     []byte
	Checksum []byte
}

func (s *Checkpoint) Writer() io.WriteCloser {
	return &checkpointWriteCloser{checkpoint: s}
}

func (s *Checkpoint) Reader() io.ReadCloser {
	return &checkpointReadCloser{reader: bytes.NewBuffer(s.Data)}
}

type checkpointWriteCloser struct {
	checkpoint *Checkpoint
}

func (w *checkpointWriteCloser) Write(p []byte) (n int, err error) {
	w.checkpoint.Data = append(w.checkpoint.Data, p...)
	return len(p), nil
}

func (w *checkpointWriteCloser) Close() error {
	checksum := sha256.Sum256([]byte(w.checkpoint.Data))
	w.checkpoint.Checksum = checksum[:]
	return nil
}

type checkpointReadCloser struct {
	reader io.Reader
}

func (r *checkpointReadCloser) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *checkpointReadCloser) Close() error {
	return nil
}
