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
	lo, err := storage.FirstIndex()
	if err != nil {
		log.Panicf(err.Error())
	}

	hi, err := storage.LastIndex()
	if err != nil {
		log.Panicf(err.Error())
	}

	ents, err := storage.Entries(lo, hi+1)
	if err != nil {
		log.Panicf(err.Error())
	}

	var stabled uint64
	if len(ents) > 0 {
		stabled = ents[len(ents)-1].Index
	} else {
		stabled = hi
	}

	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   lo - 1,  // apply from the first possible log
		stabled:   stabled, // todo: is this wrong?
		entries:   ents,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

	truncatedIndex, err := l.storage.FirstIndex()
	if err != nil {
		log.Errorf("fail to get the first index")
		return
	}
	truncatedIndex -= 1

	l.TruncateEntries(truncatedIndex)

	// snapshot only contains the committed entries(and should be applied)
	l.UpdateApplied(truncatedIndex)
	l.UpdateCommited(truncatedIndex)
	l.UpdateStabled(truncatedIndex)
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
		log.Panicf("applied %v should be less than committed %v", l.applied, l.committed)
	}

	// invariant: len(l.entries) > 0
	assert(len(l.entries) > 0, "should have at least one entry")
	appliedOffset := l.applied - l.entries[0].Index
	committedOffset := l.committed - l.entries[0].Index

	ents = append(ents, l.entries[appliedOffset+1:committedOffset+1]...)

	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// todo: add support for snapshot
	if len(l.entries) == 0 {
		if !IsEmptySnap(l.pendingSnapshot) {
			return l.pendingSnapshot.Metadata.Index
		} else if lastIndex, err := l.storage.LastIndex(); err != nil {
			panic(err.Error())
		} else {
			return lastIndex
		}
	}

	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// todo: add support for snapshot

	if len(l.entries) == 0 || i < l.entries[0].Index {
		if !IsEmptySnap(l.pendingSnapshot) {
			if i == l.pendingSnapshot.Metadata.Index {
				return l.pendingSnapshot.Metadata.Term, nil
			} else {
				return 0, errors.New("fail to find matching index in log")
			}
		} else {
			return l.storage.Term(i)
		}
	}

	if i > l.LastIndex() {
		return 0, errors.New("fail to find matching index in log")
	}

	offset := i - l.entries[0].Index
	// jump to the position of index i
	return l.entries[offset].Term, nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	// todo: add support for snapshot
	if len(l.entries) == 0 { // no entries, just return 0
		if !IsEmptySnap(l.pendingSnapshot) {
			return l.pendingSnapshot.Metadata.Index + 1
		} else if firstIndex, err := l.storage.FirstIndex(); err != nil {
			panic(err.Error())
		} else {
			return firstIndex
		}
	}

	return l.entries[0].Index
}

func (l *RaftLog) UpdateCommited(committed uint64) bool {
	if committed < l.committed {
		return false
	}
	l.committed = committed
	return true
}

func (l *RaftLog) UpdateApplied(applied uint64) bool {
	if applied < l.applied {
		return false
	}
	l.applied = applied
	return true
}

func (l *RaftLog) UpdateStabled(stabled uint64) bool {
	if stabled < l.stabled {
		return false
	}
	l.stabled = stabled
	return true
}

// Why is stabled able to be modified directly?
// stabled may roll back to a smaller one due to
// AppendEntries truncate the stable entries (previous stable entries should be dropped)
func (l *RaftLog) SetStabled(stabled uint64) {
	l.stabled = stabled
}

func (l *RaftLog) TruncateEntries(truncatedIndex uint64) {
	/*
		if there is an incoming snapshot, several situations could occur:

		1. snapshot's last applied index >= LastIndex.
		Just drop all the logs(data in storage should already up-to-date)

		2. snapshot's last applied index < LastIndex.
			2.1 snapshot's last applied index < FirstIndex. Ignore it.
			2.2 snapshot's last applied idnex >= FirstIndex. Truncate logs.
	*/
	if len(l.entries) == 0 {
		return
	}

	if l.LastIndex() <= truncatedIndex {
		// logs are covered by incoming snapshot, drop entire logs
		l.entries = l.entries[:0]
		log.Infof("succed to truncate all the logs(%v -> %v)", l.FirstIndex(), truncatedIndex)
	} else if truncatedIndex >= l.entries[0].Index {
		offset := truncatedIndex - l.entries[0].Index
		// retain all the logs following the last applied log from pending snapshot
		log.Infof("succed to truncate part of the logs(%v -> %v)", l.entries[0].Index, truncatedIndex)
		l.entries = l.entries[offset+1:]
	}
}
