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
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	ents, _ := storage.Entries(lo, hi+1)
	snapshot, _ := storage.Snapshot()
	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         0,
		stabled:         hi, // todo: is this wrong?
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

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := []pb.Entry{}
	for _, ent := range l.entries {
		if ent.Index <= l.stabled { // skip stable entries
			continue
		}

		ents = append(ents, ent)
	}
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

	for _, ent := range l.entries {
		if ent.Index <= l.applied {
			continue
		}

		if ent.Index > l.committed {
			break
		}
		ents = append(ents, ent)
	}

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

	if i == 0 { // corner case, no logs at beginning
		return 0, nil
	}

	for _, ent := range l.entries {
		if ent.Index == i {
			return ent.Term, nil
		}
	}
	return 0, errors.New("no logs match index")
}
