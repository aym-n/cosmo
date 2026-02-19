package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/aym-n/cosmo/rpc/proto"
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

	transport Transport

	applyCh chan LogEntry
	stopCh  chan struct{}
}

type Transport interface {
	SendRequestVote(peerID NodeID, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	SendAppendEntries(peerID NodeID, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool

	ConflictIndex LogIndex
	ConflictTerm  Term
}

func NewNode(config Config, applyCh chan LogEntry, transport Transport) *Node {
	n := &Node{
		id:        config.NodeID,
		config:    config,
		state:     Follower,
		applyCh:   applyCh,
		stopCh:    make(chan struct{}),
		transport: transport,
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

func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.run()
}

func (n *Node) Stop() {
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

func (n *Node) runCandidate() {
	n.mu.Lock()

	term := n.persist.CurrentTerm
	lastLogIndex := n.lastLogIndex()
	lastLogTerm := n.lastLogTerm()
	n.resetElectionTimer()

	n.mu.Unlock()

	voteCh := make(chan bool, len(n.config.Peers))
	for _, peerID := range n.config.Peers {
		go func(peer NodeID) {
			granted := n.sendRequestVote(peer, term, lastLogIndex, lastLogTerm)
			voteCh <- granted
		}(peerID)
	}

	votes := 1
	majority := len(n.config.Peers)/2 + 1

	votesRecieved := 0
	for votesRecieved < len(n.config.Peers) {
		select {
		case granted := <-voteCh:
			votesRecieved++
			if granted {
				votes++
			}

			if votes >= majority {
				n.mu.Lock()
				if n.state == Candidate && n.persist.CurrentTerm == term {
					n.becomeLeader()
				}

				n.mu.Unlock()
				return
			}

		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state == Candidate {
				n.becomeCandidate()
			}
			n.mu.Unlock()
			return

		case <-n.stopCh:
			return

		}
	}

	select {
	case <-n.electionTimer.C:

		n.mu.Lock()
		if n.state == Candidate {
			n.becomeCandidate()
		}
		n.mu.Unlock()

	case <-n.stopCh:
		return
	}
}
func (n *Node) runLeader() {
	n.mu.Lock()
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	n.heartbeatTicker = time.NewTicker(n.config.HeartbeatTimeout)
	n.mu.Unlock()

	n.sendHeartbeats()

	for {
		select {
		case <-n.heartbeatTicker.C:
			n.sendHeartbeats()

		case <-n.stopCh:
			return
		}

		n.mu.Lock()
		stillLeader := (n.state == Leader)
		n.mu.Unlock()

		if !stillLeader {
			return
		}
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.persist.CurrentTerm++
	n.persist.VotedFor = n.id
	n.currentLeader = ""
	n.logInfo("Starting election for term %d", n.persist.CurrentTerm)
}

func (n *Node) becomeFollower(term Term, leaderID NodeID) {
	n.state = Follower
	n.persist.CurrentTerm = term
	n.persist.VotedFor = ""
	n.currentLeader = leaderID
	n.leaderState = nil
	n.resetElectionTimer()

	if leaderID != "" {
		n.logInfo("Became follower for term %d, leader=%s", term, leaderID) // ADD THIS
	} else {
		n.logInfo("Became follower for term %d", term) // ADD THIS
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.currentLeader = n.id
	n.logInfo("Became LEADER for term %d", n.persist.CurrentTerm)

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

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := AppendEntriesResponse{
		Term:    n.persist.CurrentTerm,
		Success: false,
	}

	// Rule 1: if term < currentTerm , reply flase (leader is stale)
	if req.Term < n.persist.CurrentTerm {
		return resp
	}

	// Rule 2: If we see a higher term, become follower
	if req.Term > n.persist.CurrentTerm {
		n.becomeFollower(req.Term, req.LeaderID)
		resp.Term = n.persist.CurrentTerm
	}

	// Rule 3: If we are a candidate and see a valid leader, become follower
	if n.state == Candidate {
		n.becomeFollower(req.Term, req.LeaderID)
	}

	n.currentLeader = req.LeaderID
	n.resetElectionTimer()

	// Heartbeat
	if len(req.Entries) == 0 {
		resp.Success = true
		n.logInfo("Received heartbeat from %s (term=%d)", req.LeaderID, req.Term)
		return resp
	}

	// TODO: Handle Log Replication

	resp.Success = true
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

func (n *Node) sendRequestVote(peerID NodeID, term Term, lastLogIndex LogIndex, lastLogTerm Term) bool {
	req := &pb.RequestVoteRequest{
		Term:         uint64(term),
		CandidateId:  string(n.id),
		LastLogIndex: uint64(lastLogIndex),
		LastLogTerm:  uint64(lastLogTerm),
	}

	resp, err := n.transport.SendRequestVote(peerID, req)
	if err != nil {
		// Network error, timeout, or peer is down
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// If we see a higher term in the response, step down immediately
	if Term(resp.Term) > n.persist.CurrentTerm {
		n.becomeFollower(Term(resp.Term), "")
		return false
	}

	return resp.VoteGranted
}

func (n *Node) logInfo(format string, args ...interface{}) {
	prefix := "[" + string(n.id) + "] "
	log.Printf(prefix+format, args...)
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.persist.CurrentTerm
	leaderID := n.id
	commitIndex := n.volatile.CommitIndex
	n.logInfo("Sending heartbeats to %d peers (term=%d)", len(n.config.Peers), term)
	n.mu.Unlock()

	for _, peerID := range n.config.Peers {
		go func(peer NodeID) {
			n.sendAppendEntries(peer, term, leaderID, commitIndex)
		}(peerID)
	}
}

func (n *Node) sendAppendEntries(peerID NodeID, term Term, leaderID NodeID, commitIndex LogIndex) {
	n.mu.Lock()

	prevLogIndex := n.lastLogIndex()
	prevLogTerm := n.lastLogTerm()

	n.mu.Unlock()

	req := &pb.AppendEntriesRequest{
		Term:         uint64(term),
		LeaderId:     string(leaderID),
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  uint64(prevLogTerm),
		Entries:      nil,
		LeaderCommit: uint64(commitIndex),
	}

	resp, err := n.transport.SendAppendEntries(peerID, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if Term(resp.Term) > n.persist.CurrentTerm {
		n.becomeFollower(Term(resp.Term), "")
		return
	}

	// TODO: Handle Log Replication
}
