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
	"fmt"
	"math/rand"
	"sort"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes    map[uint64]bool
	hasVoted map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	electionRandTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	// if peers is empty, we will use confState
	var peers []uint64
	if len(c.peers) == 0 {
		peers = confState.GetNodes()
	} else {
		peers = c.peers
	}
	log.Infof("the conf state from storage %v is %v", c.ID, confState)

	return &Raft{
		id:                  c.ID,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		RaftLog:             newLog(c.Storage),
		Prs:                 makePrs(peers),
		State:               StateFollower,
		votes:               makeVotes(peers),
		hasVoted:            makeVotes(peers),
		msgs:                []pb.Message{},
		Lead:                0, // 0 is invalid for Lead
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		electionRandTimeout: c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
		leadTransferee:      0,
		PendingConfIndex:    0,
	}
}

func (r *Raft) sendSnap(to uint64) {
	for {
		if snap, err := r.RaftLog.storage.Snapshot(); err == ErrSnapshotTemporarilyUnavailable {
			if err == ErrSnapshotTemporarilyUnavailable {
				continue // snapshot is generating
			} else {
				log.Panicf("fail to generate snapshot when %v trying to send logs to %v", r.ToString(), to)
			}
		} else {
			r.msgs = append(r.msgs, pb.Message{
				MsgType:  pb.MessageType_MsgSnapshot,
				Term:     r.Term,
				From:     r.id,
				To:       to,
				Commit:   r.RaftLog.committed,
				Snapshot: &snap,
			})
			r.Prs[to].Match = max(r.Prs[to].Match, snap.Metadata.GetIndex())
			r.Prs[to].Next = max(r.Prs[to].Next, snap.Metadata.GetIndex()+1)
			return
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	if r.State != StateLeader { // only the leader could sent AppendEntries
		return false
	}

	logTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil { // log not found
		// poll for a snapshot
		r.sendSnap(to)
		return true
	}

	log.Infof("%v send to %v, prevIndex: %v, pervTerm: %v", r.id, to, r.Prs[to].Next-1, logTerm)

	logOffset := r.Prs[to].Next - r.RaftLog.entries[0].Index

	ents := []*pb.Entry{}
	if int(logOffset) < len(r.RaftLog.entries) {
		for _, ent := range r.RaftLog.entries[logOffset:] {
			nent := ent
			ents = append(ents, &nent)
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Index:   r.Prs[to].Next - 1,
		LogTerm: logTerm,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return
	}

	log.Infof("leader %v send heartbeat to %v", r.id, to)
	logTerm, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	// todo: send logic
	if err != nil { // log not found
		// poll for a snapshot
		r.sendSnap(to)
		return
	}
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, From: r.id, To: to, Term: r.Term, LogTerm: logTerm, Index: r.Prs[to].Next - 1, Commit: r.RaftLog.committed})
}

func (r *Raft) bcastHeartbeat() {
	if r.State != StateLeader {
		log.Infof("%v is not leader but try to send heartbeat", r.id)
		return
	}

	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

func (r *Raft) bcastAppend() {
	if r.State != StateLeader {
		log.Infof("%v is not leader but try to send append", r.id)
		return
	}

	for peer := range r.Prs {
		if peer != r.id {
			log.Infof("%v sends to %v", r.id, peer)
			succ := r.sendAppend(peer)
			assert(succ, "sendAppend should succ")
		}
	}
}

// sendRequestVote sends a requestvote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: to, Term: r.Term, LogTerm: logTerm, Index: r.RaftLog.LastIndex()})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		// invariant: electionElapsed is in [0, electionTimeout)
		r.electionElapsed++
		if r.electionElapsed == r.electionRandTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})

			r.electionElapsed = 0
			r.electionRandTimeout = rand.Int()%r.electionTimeout + r.electionTimeout // in range [et, 2et)
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			// should send heartbeat
			r.heartbeatElapsed = 0
			r.bcastHeartbeat()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Infof("%v becomes follower with term %v and lead %v", r.id, term, lead)
	if term > r.Term {
		r.Vote = 0 // since term increase, Vote should be reset
	}
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Infof("%v becomes candiate with term %v", r.id, r.Term+1)
	r.Term++
	r.Vote = r.id
	r.Lead = 0
	r.State = StateCandidate

	// clear the vote status
	for id := range r.votes {
		r.votes[id] = false
		r.hasVoted[id] = false
	}

	// vote for self
	r.votes[r.id] = true
	r.hasVoted[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("%v becomes leader in term %v", r.id, r.Term)
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Lead = r.id

	// initialize Progress information
	lastLogIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id].Next = lastLogIndex + 1
		r.Prs[id].Match = 0
	}

	// specifically, leader's progress info is newst
	r.Prs[r.id].Match = lastLogIndex

	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: nil}}}) // noop entry
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		return nil // ignore
	case pb.MessageType_MsgHup: // start new election
		r.becomeCandidate()
		log.Infof("%v get msg hup and begin a new election", r.id)

		if len(r.votes) == 1 {
			log.Infof("%v get %v votes and become leader of term %v", r.id, 1, r.Term)
			r.becomeLeader()
		} else {
			// send requestvote to all the peers
			for id := range r.votes {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		}
	case pb.MessageType_MsgPropose:
		// forwarded to leader
		log.Infof("%v recv propose, forward it to %v", r.id, r.Lead)
		m.To = r.Lead // is this legal?
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		return nil
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		return nil // ignore
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		return nil // ignore this
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	default:
		log.Fatalf("%v get unknown msg %v", r.id, m)
	}

	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // start new election
		r.becomeCandidate()
		log.Infof("%v get msg hup and begin a new election", r.id)
		if len(r.votes) == 1 {
			log.Infof("%v get %v votes and become leader of term %v", r.id, 1, r.Term)
			r.becomeLeader()
		} else {
			// send requestvote to all the peers
			for id := range r.votes {
				if id != r.id {
					r.sendRequestVote(id)
				}
			}
		}
	case pb.MessageType_MsgBeat:
		return nil // ignore
	case pb.MessageType_MsgPropose:
		return nil // ignore
	case pb.MessageType_MsgRequestVote:
		fallthrough
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		return nil // ignore
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		fallthrough
	case pb.MessageType_MsgSnapshot:
		return nil
	default:
		log.Fatalf("%v get unknown msg %v", r.id, m)
	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return nil // just ignore
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		return nil
	case pb.MessageType_MsgAppend:
		fallthrough
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		fallthrough
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		return nil
	default:
		log.Fatalf("leader %v get unknown msg", r.id)
	}

	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)

		// reject the vote if:
		// 1. smaller term
		// 2. log is not as-new-as its
		// 3. has voted to another one
		if m.Term < r.Term {
			// log.Infof("[%v,%v] reject requestvote from [%v,%v]", r.id, r.Term, m.From, m.Term)
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term})
			return
		}

		log.Infof("[%v,%v] get requestvote from [%v,%v]", r.id, r.Term, m.From, m.Term)
		if m.Term > r.Term {
			r.becomeFollower(m.Term, 0)
		}

		if m.LogTerm < lastTerm || (m.LogTerm == lastTerm && m.Index < lastIndex) || (r.Vote != 0 && r.Vote != m.From) {
			log.Infof("[%v,%v] reject requestvote from [%v,%v]", r.id, r.Term, m.From, m.Term)
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: true, Term: r.Term})
			return
		}

		// grant the vote
		if r.Vote == 0 && r.State == StateFollower { // follower first grant the vote in this term
			r.Vote = m.From
			r.electionElapsed = 0 // since grant a vote to another candiate, reset the ticker
		}

		log.Debugf("[%v,%v] grant requestvote from [%v,%v]", r.id, r.Term, m.From, m.Term)
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Reject: false, Term: r.Term})

	case pb.MessageType_MsgRequestVoteResponse:

		if m.Term != r.Term || r.hasVoted[m.From] { // reduant replies
			// log.Infof("[%v, %v] get redudant grant from [%v, %v]", r.id, r.Term, m.From, m.Term)
			return
		}

		r.hasVoted[m.From] = true

		if !m.Reject { // vote granted
			log.Infof("[%v, %v] get grant from [%v, %v]", r.id, r.Term, m.From, m.Term)
			r.votes[m.From] = true

			minVotes := (len(r.votes))/2 + 1 // at least minVotes to become leader
			voteCnt := 0
			for _, granted := range r.votes {
				if granted {
					voteCnt++
				}
			}

			if voteCnt >= minVotes {
				log.Infof("%v get %v votes and become leader of term %v with commit %v", r.id, voteCnt, r.Term, r.RaftLog.committed)
				r.becomeLeader()
			}
		} else {
			if m.Term > r.Term {
				r.becomeFollower(m.Term, 0)
			}

			minVotes := (len(r.votes))/2 + 1 // at least minVotes to become follower
			voteCnt := 0
			for id, granted := range r.votes {
				if !granted && r.hasVoted[id] {
					voteCnt++
				}
			}

			if voteCnt >= minVotes {
				r.becomeFollower(r.Term, 0)
			}
		}
	default:
		panic("handleRequestVote: get wrong message")
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		log.Infof("[%v, %v] get append from [%v, %v]", r.id, r.Term, m.From, m.Term)
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
			return
		}

		if m.Term > r.Term || (r.Lead != m.From && r.State != StateLeader) { // optimize: if the peer is follower in the term, there couldn't be another leader
			r.becomeFollower(m.Term, m.From)
		}

		y.Assert(r.State == StateFollower)
		r.electionElapsed = 0 // since recive AppendEntries from current leder, reset the ticker

		term, err := r.RaftLog.Term(m.Index)
		if err != nil || term != m.LogTerm { // no log found with m.Index in local logs or mismatch
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
			return
		}

		// tackle with the corner case:
		// m.Entries[0] could be the truncted log, if so, we truncate the m.Entries
		if len(m.Entries) != 0 && len(r.RaftLog.entries) != 0 && m.Entries[0].Index == r.RaftLog.entries[0].Index-1 {
			m.Entries = m.Entries[1:]
		}

		// check log consistency and apply new logs
		if m.Entries != nil && len(m.Entries) > 0 {
			loglen := len(r.RaftLog.entries)
			var logoffset int // the begin index of local logs
			if loglen == 0 {
				logoffset = 0
			} else {
				logoffset = int(m.Entries[0].Index) - int(r.RaftLog.entries[0].Index)

				assert(logoffset == loglen || r.RaftLog.entries[logoffset].Index == m.Entries[0].Index, "begin index should be the same")
			}

			if logoffset > loglen || logoffset < 0 {
				panic("offset should no larger than loglen and le than 0")
			}

			for ind, ent := range m.Entries {
				if logoffset == loglen { // reach the end of local logs, just append
					log.Infof("%v just append the logs", r.id)

					// assert(r.RaftLog.stabled == uint64(logoffset), "all the previous logs are persisted")
					for _, ent := range m.Entries[ind:] {
						r.RaftLog.entries = append(r.RaftLog.entries, *ent)
					}
					break
				}

				// todo: remove the check
				if r.RaftLog.entries[logoffset].Index != ent.Index {
					panic("index should equal")
				}

				term := r.RaftLog.entries[logoffset].Term // check term equality
				if term != ent.Term {                     // drop following local logs and just append
					offset := ent.Index - r.RaftLog.entries[0].Index
					r.RaftLog.entries = append([]pb.Entry{}, r.RaftLog.entries[:offset]...)

					// reset the stabled to drop the inconsistent entries(and these log entries will be
					// removed from storage soon)
					var stabled uint64
					if len(r.RaftLog.entries) > 0 {
						stabled = min(r.RaftLog.stabled, r.RaftLog.LastIndex())
					} else {
						stabled = r.RaftLog.FirstIndex() - 1 // all the entries are not stabled
					}
					log.Infof("%v change stable from %v to %v", r.id, r.RaftLog.stabled, stabled)
					r.RaftLog.SetStabled(stabled)

					for _, ent := range m.Entries[ind:] {
						r.RaftLog.entries = append(r.RaftLog.entries, *ent)
					}
					break
				}

				logoffset++
			}
		}

		// local logs: [1, 2, 3, 4, 5]
		// incoming logs: [3, 4, 5] / [3, 4] / [4, 5, 6]

		// update commit index
		lastNewIndex := m.Index
		if m.Entries != nil && len(m.Entries) > 0 {
			lastNewIndex = m.Entries[len(m.Entries)-1].Index
		}

		if m.Commit > r.RaftLog.committed {
			if r.RaftLog.UpdateCommited(min(m.Commit, lastNewIndex)) {
				log.Infof("%v change commit to %v", r.ToString(), r.RaftLog.committed)
			}
		}

		if r.RaftLog.committed > r.RaftLog.LastIndex() {
			log.Errorf("[%v, %v] commit %v larger than last index %v, %v, %v, and logs %v", r.id, r.Term, r.RaftLog.committed, r.RaftLog.LastIndex(), m.Commit, lastNewIndex, r.RaftLog.allEntries())
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  false,
			Index:   lastNewIndex})

	case pb.MessageType_MsgAppendResponse:
		// maintain the Prs
		if m.Reject {
			log.Infof("[%v, %v] get append reject response from [%v, %v]", r.id, r.Term, m.From, m.Term)
			if m.Term > r.Term {
				r.becomeFollower(m.Term, 0)
			}
			// todo: snapshot support
			if r.Prs[m.From].Next != r.RaftLog.FirstIndex()-1 {
				r.Prs[m.From].Next--
			}

			r.sendAppend(m.From)
		} else {
			r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
			r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
			log.Infof("[%v, %v] get append ok response from [%v, %v], Next: %v, Match: %v now", r.id, r.Term, m.From, m.Term, r.Prs[m.From].Next, r.Prs[m.From].Match)
			r.tryUpdateCommit()
		}
	default:
		panic("handleAppendEntries: get wrong message")
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		log.Infof("[%v, %v] get heartbeat from [%v, %v]", r.id, r.Term, m.From, m.Term)
		if m.Term < r.Term {
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
			return
		}

		if m.Term > r.Term || (r.Lead != m.From && r.State != StateLeader) { // optimize: if the peer is follower in the term, there couldn't be another leader
			r.becomeFollower(m.Term, m.From)
		}

		y.Assert(r.State == StateFollower)
		r.electionElapsed = 0
		term, _ := r.RaftLog.Term(m.Index)
		if term == 0 { // no log at m.Index
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Reject: true})
			return
		}

		// update committed
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.UpdateCommited(min(m.Commit, m.Index))
		}

		if r.RaftLog.committed > r.RaftLog.LastIndex() {
			log.Errorf("[%v, %v] commit %v larger than last index %v, %v, and logs %v", r.id, r.Term, r.RaftLog.committed, r.RaftLog.LastIndex(), m.Commit, r.RaftLog.allEntries())
		}

		// return the last matching index too
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From, Term: r.Term, Index: m.Index, Reject: false})

	case pb.MessageType_MsgHeartbeatResponse:
		log.Infof("[%v, %v] get heartbeat response from [%v, %v]", r.id, r.Term, m.From, m.Term)
		// maintain the Prs
		if m.Reject {
			// todo: snapshot support
			log.Infof("%v get heart beat rej from %v", r.id, m.From)

			if m.Term > r.Term { // degrade to Follower
				r.becomeFollower(m.Term, 0)
			} else if r.Prs[m.From].Next != r.RaftLog.FirstIndex()-1 { // update the Next
				r.Prs[m.From].Next--
			}
		} else {
			r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		}

		if r.Prs[m.From].Next <= r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	default:

		panic("handleHeartbeat: get wrong message")
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	assert(r.State == StateLeader, "only leader could get propose")

	// 1. append logs to local logs
	if m.Entries != nil && len(m.Entries) > 0 {
		log.Infof("%v begins to append entries", r.id)
		r.appendEntry(m.Entries)
	}

	// 2. broadcast to all peers
	r.bcastAppend()
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	log.Infof("%v handle the snapshot %v from %v", r.ToString(), m.Snapshot.Metadata, m.From)
	if m.Term < r.Term {
		return
	}

	if m.Term > r.Term || (r.Lead != m.From && r.State != StateLeader) { // optimize: if the peer is follower in the term, there couldn't be another leader
		r.becomeFollower(m.Term, m.From)
	}

	y.Assert(r.State == StateFollower)
	r.electionElapsed = 0 // since recive AppendEntries from current leder, reset the ticker

	if IsEmptySnap(m.Snapshot) {
		log.Errorf("%v expect a snapshot while get a nil", r.ToString())
		return
	}

	// stale snapshot, just ignore it
	if m.Snapshot.GetMetadata().GetIndex() < r.RaftLog.LastIndex() {
		return
	}

	r.RaftLog.TruncateEntries(m.Snapshot.GetMetadata().GetIndex())

	// restore the data from the snapshot
	confstate := m.Snapshot.Metadata.GetConfState()

	if confstate != nil {
		peers := confstate.GetNodes()
		r.Prs = makePrs(peers)
		r.votes = makeVotes(peers)
		r.hasVoted = makeVotes(peers)
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.UpdateApplied(m.Snapshot.GetMetadata().GetIndex())
	r.RaftLog.UpdateCommited(m.Snapshot.GetMetadata().GetIndex())
	r.RaftLog.UpdateStabled(m.Snapshot.GetMetadata().GetIndex())

	// todo: handle the conf change(3A)

}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) appendEntry(ents []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	log.Infof("%v append %v to logs, lastIndex: %v", r.id, ents, lastIndex)

	// assert(r.RaftLog.stabled == lastIndex, "logs before append should be persisted")
	for _, ent := range ents {
		lastIndex++
		nent := *ent
		nent.Term = r.Term
		nent.Index = lastIndex
		// todo: is this copy neccassary ?
		if ent.Data != nil {
			copy(nent.Data, ent.Data)
		}
		r.RaftLog.entries = append(r.RaftLog.entries, nent)
	}

	if len(r.Prs) == 1 { // corner case: single node, just commit
		r.RaftLog.UpdateCommited(lastIndex)
	}

	r.Prs[r.id].Match = lastIndex
	r.Prs[r.id].Next = lastIndex + 1
}

// tryUpdateCommit is called only when peer's Match is updated
// according to last rule of leader
func (r *Raft) tryUpdateCommit() bool {
	// todo: single node?
	th := len(r.Prs)/2 + 1 // at least th peers should persist the log

	matchs := []uint64{}
	for _, pr := range r.Prs {
		matchs = append(matchs, pr.Match)
	}

	sort.Slice(matchs, func(i, j int) bool { return matchs[i] > matchs[j] })
	log.Infof("after sort: %v", matchs)

	if matchs[th-1] > r.RaftLog.committed {
		term, err := r.RaftLog.Term(matchs[th-1])
		assert(err == nil, "tryUpdateCommit: term must exist")

		if term != r.Term {
			return false
		}
		r.RaftLog.UpdateCommited(matchs[th-1])

		// when advance commit index, broad cast append entries
		// todo: when?
		r.bcastAppend()
		return true
	}

	return false
}

func (r *Raft) ToString() string {
	return fmt.Sprintf("[id: %v, term: %v, commit:%v, lead:%v]", r.id, r.Term, r.RaftLog.committed, r.Lead)
}

// Compact calls maybeCompact to compact the logs
func (r *Raft) Compact() {
	r.RaftLog.maybeCompact()
}
