package raft

import (
	"math/rand"
	"sync"
	"time"
)

type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Log         []LogEntry
}

type VolatileState struct {
	CommitIndex LogIndex
	LastApplied LogIndex
}

type LeaderState struct {
	NextIndex  map[NodeID]LogIndex
	MatchIndex map[NodeID]LogIndex
}

type Node struct {
	mu sync.Mutex

	id     NodeID
	config Config

	state NodeState

	persist  PersistentState
	volatile VolatileState

	leaderState   *LeaderState
	currentLeader NodeID

	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker

	applyCh chan LogEntry
	stopCh  chan struct{}
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

func NewNode(config Config, applyCh chan LogEntry) *Node {
	n := &Node{
		id:      config.NodeID,
		config:  config,
		state:   Follower,
		applyCh: applyCh,
		stopCh:  make(chan struct{}),
		persist: PersistentState{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         make([]LogEntry, 0),
		},
		volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
		},
	}
	return n
}

func (n *Node) lastLogIndex() LogIndex {
	if len(n.persist.Log) == 0 {
		return 0
	}
	return n.persist.Log[len(n.persist.Log)-1].Index
}

func (n *Node) lastLogTerm() Term {
	if len(n.persist.Log) == 0 {
		return 0
	}
	return n.persist.Log[len(n.persist.Log)-1].Term
}

func (n *Node) getEntry(index LogIndex) (LogEntry, bool) {
	if index == 0 || int(index) > len(n.persist.Log) {
		return LogEntry{}, false
	}
	return n.persist.Log[index-1], true
}

func (n *Node) appendEntry(entry LogEntry) {
	n.persist.Log = append(n.persist.Log, entry)
}

func (n *Node) truncateFrom(index LogIndex) {
	if int(index) <= len(n.persist.Log) {
		n.persist.Log = n.persist.Log[:index-1]
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

func (n *Node) resetElectionTimer() {
	timeout := n.randomElectionTimeout()
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		// stop + drain before reset to prevent trigger immediately after reset
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(timeout)
	}
}

func (n *Node) start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.run()
}

func (n *Node) stop() {
	close(n.stopCh)
}

func (n *Node) run() {
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}

		select {
		case <-n.stopCh:
			return
		default:
		}
	}
}

func (n *Node) runFollower() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	select {
	case <-n.electionTimer.C:
		n.mu.Lock()
		n.becomeCandidate()
		n.mu.Unlock()
	case <-n.stopCh:
		return
	}
}

func (n *Node) runCandidate() {}
func (n *Node) runLeader()    {}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.persist.CurrentTerm++
	n.persist.VotedFor = n.id
	n.currentLeader = ""
}

func (n *Node) becomeFollower(term Term, leaderID NodeID) {
	n.state = Follower
	n.persist.CurrentTerm = term
	n.persist.VotedFor = ""
	n.currentLeader = leaderID
	n.leaderState = nil
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.currentLeader = n.id

	nextIdx := n.lastLogIndex() + 1
	leaderState := &LeaderState{
		NextIndex:  make(map[NodeID]LogIndex),
		MatchIndex: make(map[NodeID]LogIndex),
	}
	for _, peer := range n.config.Peers {
		leaderState.NextIndex[peer] = nextIdx
		leaderState.MatchIndex[peer] = 0
	}
	n.leaderState = leaderState
}


func (n *Node) HandleRequestVote(term Term, candidateID NodeID, lastLogIndex LogIndex, lastLogTerm Term) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := RequestVoteResponse{
		Term:        n.persist.CurrentTerm,
		VoteGranted: false,
	}

	// Rule 1: if candidate term is less than node term , reject (stale candidate)
	if term < n.persist.CurrentTerm {
		return resp
	}

	// Rule 2: if candidate term is greater than node term, become follower
	if term > n.persist.CurrentTerm {
		n.becomeFollower(term, "")
		resp.Term = n.persist.CurrentTerm
	}

	// Rule 3: if we have already voted for another candidate in this term reject (no double voting)
	if n.persist.VotedFor != "" && n.persist.VotedFor != candidateID {
		return resp
	}

	// Rule 4: if candidate's log is at least as up to date as nodes log. if yes grant vote
	if !n.isLogUpToDate(lastLogIndex, lastLogTerm) {
		return resp
	}

	n.persist.VotedFor = candidateID
	n.resetElectionTimer() 
	resp.VoteGranted = true

	return resp
}


func (n *Node) isLogUpToDate(lastIndex LogIndex, lastTerm Term) bool {
	myLastIndex := n.lastLogIndex()
	myLastTerm := n.lastLogTerm()

	if lastTerm > myLastTerm {
		return true
	}

	if lastTerm < myLastTerm {
		return false
	}

	// if both are on the same term then the one with more entries is more up to date
	return lastIndex >= myLastIndex
}
