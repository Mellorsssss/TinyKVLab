// Copyright 2015 The etcd Authors
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

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("newLog: storage is nil")
	}

	hardState, _, _ := storage.InitialState()
	// corner case: filter the dummy logs
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	ents, _ := storage.Entries(lo, hi+1)
	snapshot, _ := storage.Snapshot()
	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         lo - 1, // apply from the first possible log
		stabled:         hi,     // todo: is this wrong?
		entries:         ents,
		pendingSnapshot: &snapshot,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}

	// filter all the dummy entries
	for offset, ent := range l.entries {
		if ent.Term == 0 {
			continue
		}
		ents = append(ents, l.entries[offset:]...)
		break
	}

	return ents
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}

	if len(l.entries) == 0 {
		return ents
	}

	offset := l.stabled - l.entries[0].Index + 1
	if offset >= uint64(len(l.entries)) {
		return ents
	}

	ents = append(ents, l.entries[offset:]...)
	log.Infof("stable: %v, ents: %v", l.stabled, ents)
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied == l.committed {
		return ents
	}

	if l.applied > l.committed {
		panic("applied should be less than committed")
	}

	// invariant: len(l.entries) > 0
	assert(len(l.entries) > 0, "should have at least one entry")
	appliedOffset := l.applied - l.entries[0].Index
	committedOffset := l.committed - l.entries[0].Index

	ents = append(ents, l.entries[appliedOffset+1:committedOffset+1]...)

	log.Infof("next ents: %v", ents)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// todo: add support for snapshot
	if len(l.entries) == 0 { // no entries, just return 0
		return 0
	}

	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// todo: add support for snapshot

	// when acquire the non-existed log, return term 0
	if i == 0 {
		return 0, nil
	}

	if len(l.entries) == 0 || i < l.entries[0].Index || i > l.LastIndex() {
		return 0, errors.New("fail to find matching index in log")
	}

	offset := i - l.entries[0].Index
	// jump to the position of index i
	return l.entries[offset].Term, nil
}
