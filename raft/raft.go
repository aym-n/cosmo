package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/aym-n/cosmo/internal/consensus"
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
	store     consensus.Persister

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

func NewNode(config Config, applyCh chan LogEntry, transport Transport, store consensus.Persister) (*Node, error) {
	n := &Node{
		id:        config.NodeID,
		config:    config,
		state:     Follower,
		applyCh:   applyCh,
		stopCh:    make(chan struct{}),
		transport: transport,
		store:     store,
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
	if store != nil {
		term, votedFor, err := store.LoadMetadata()
		if err == nil {
			n.persist.CurrentTerm = term
			n.persist.VotedFor = votedFor
		}
		entries, err := store.LoadLog()
		if err == nil && len(entries) > 0 {
			n.persist.Log = entries
		}
	}
	return n, nil
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
	if n.store != nil {
		_ = n.store.AppendLogEntry(entry)
	}
}

func (n *Node) truncateFrom(index LogIndex) {
	if int(index) <= len(n.persist.Log) {
		n.persist.Log = n.persist.Log[:index-1]
		if n.store != nil {
			_ = n.store.TruncateLog(index)
		}
	}
}

func (n *Node) persistMetadata() {
	if n.store != nil {
		_ = n.store.SaveMetadata(n.persist.CurrentTerm, n.persist.VotedFor)
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
	n.persistMetadata()
	n.logInfo("Starting election for term %d", n.persist.CurrentTerm)
}

func (n *Node) becomeFollower(term Term, leaderID NodeID) {
	n.state = Follower
	n.persist.CurrentTerm = term
	n.persist.VotedFor = ""
	n.persistMetadata()
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
	n.persistMetadata()
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

	// Rule 4: Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm 
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > n.lastLogIndex() {
			n.logInfo("Log too short: need index %d, have %d", req.PrevLogIndex, n.lastLogIndex())
			resp.ConflictIndex = n.lastLogIndex() + 1
			resp.ConflictTerm = 0
			return resp
		}

		entry, ok := n.getEntry(req.PrevLogIndex)
		if !ok || entry.Term != req.PrevLogTerm {
			n.logInfo("Log mismatch at index %d: expected term %d, have term %d", req.PrevLogIndex, req.PrevLogTerm, entry.Term)

			if ok {
				resp.ConflictIndex = req.PrevLogIndex
				resp.ConflictTerm = entry.Term

				for i := req.PrevLogIndex - 1; i >= 1; i-- {
					if e, exists := n.getEntry(i); exists && e.Term == resp.ConflictTerm {
						resp.ConflictIndex = i
					} else {
						break
					}
				}
			} else {
				resp.ConflictIndex = n.lastLogIndex() + 1
				resp.ConflictTerm = 0
			}

			return resp
		}
	}

	// Rule 5: If an existing entry conflicts with a new one - delete the existing entry and all that follow
	for _, entry := range req.Entries {
		index := entry.Index
		
		if existingEntry, ok := n.getEntry(index); ok {
			if existingEntry.Term != entry.Term {
				// Conflict — truncate log from this point
				n.logInfo("Truncating log from index %d (term conflict: %d vs %d)", 
					index, existingEntry.Term, entry.Term)
				n.truncateFrom(index)
			} else {
				// Entry already exists and matches — skip it
				continue
			}
		}
		
		// Rule 6: Append any new entries not already in the log
		n.appendEntry(entry)
		n.logInfo("Appended entry at index=%d, term=%d", entry.Index, entry.Term)
	}

	// Rule 7: If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > n.volatile.CommitIndex {
		oldCommitIndex := n.volatile.CommitIndex
		n.volatile.CommitIndex = min(req.LeaderCommit, n.lastLogIndex())
		n.logInfo("Advanced commitIndex from %d to %d", oldCommitIndex, n.volatile.CommitIndex)
		
		// Apply newly committed entries to state machine
		n.applyCommitted()
	}

	resp.Success = true
	return resp
}


func min(a, b LogIndex) LogIndex {
	if a < b {
		return a
	}
	return b
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

	if n.leaderState == nil {
		n.mu.Unlock()
		return
	}
	
	nextIndex := n.leaderState.NextIndex[peerID]
	prevLogIndex := nextIndex - 1

	var prevLogTerm Term
	if prevLogIndex == 0 {
		prevLogTerm = 0 
	} else if entry, ok := n.getEntry(prevLogIndex); ok {
		prevLogTerm = entry.Term
	} else {
		n.mu.Unlock()
		return
	}

	n.mu.Unlock()

	var entries []LogEntry
	lastLogIndex := n.lastLogIndex()
	if nextIndex <= lastLogIndex {
		for idx := nextIndex; idx <= lastLogIndex; idx++ {
			if entry, ok := n.getEntry(LogIndex(idx)); ok {
				entries = append(entries, entry)
			}
		}
	}

	pbEntries := make([]*pb.LogEntry, 0, len(entries))
	for _, e := range entries {
		pbEntries = append(pbEntries, &pb.LogEntry{
			Index:   uint64(e.Index),
			Term:    uint64(e.Term),
			Command: e.Command,
		})
	}
	req := &pb.AppendEntriesRequest{
		Term:         uint64(term),
		LeaderId:     string(leaderID),
		PrevLogIndex: uint64(prevLogIndex),
		PrevLogTerm:  uint64(prevLogTerm),
		Entries:      pbEntries,
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

	if n.state != Leader || n.leaderState == nil {
		return
	}

	
	if resp.Success {
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			n.leaderState.MatchIndex[peerID] = newMatchIndex
			n.leaderState.NextIndex[peerID] = newMatchIndex + 1
			
			n.logInfo("Replicated up to index=%d on peer %s", newMatchIndex, peerID)
			
			n.advanceCommitIndex()
		}
	} else {

		if n.leaderState.NextIndex[peerID] > 1 {
			n.leaderState.NextIndex[peerID]--
			n.logInfo("Log conflict with %s, decrementing nextIndex to %d", peerID, n.leaderState.NextIndex[peerID])
		}
	}
}

// Propose implements consensus.Proposer.
func (n *Node) Propose(command []byte) consensus.ProposeResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return consensus.ProposeResult{
			IsLeader: false,
		}
	}

	index := n.lastLogIndex() + 1
	entry := LogEntry{
		Index:   index,
		Term:    n.persist.CurrentTerm,
		Command: command,
	}

	n.appendEntry(entry)
	n.logInfo("Proposing command (index=%d, term=%d)", index, n.persist.CurrentTerm)

	return consensus.ProposeResult{
		Index:    index,
		Term:     n.persist.CurrentTerm,
		IsLeader: true,
	}
}

// GetLeader returns the current leader ID and whether this node is the leader.
// Caller can use leader ID to redirect clients (e.g. via a node-id-to-address map).
func (n *Node) GetLeader() (leaderID NodeID, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentLeader, n.state == Leader
}

func (n *Node) advanceCommitIndex() {
	if n.state != Leader || n.leaderState == nil {
		return
	}


	for index := n.volatile.CommitIndex + 1; index <= n.lastLogIndex(); index++ {
		replicaCount := 1
		
		for _, matchIndex := range n.leaderState.MatchIndex {
			if matchIndex >= index {
				replicaCount++
			}
		}
		
		majority := len(n.config.Peers)/2 + 1
		if replicaCount >= majority {
			if entry, ok := n.getEntry(index); ok && entry.Term == n.persist.CurrentTerm {
				oldCommit := n.volatile.CommitIndex
				n.volatile.CommitIndex = index
				n.logInfo("Advanced commitIndex from %d to %d (replicated to %d/%d nodes)", 
					oldCommit, index, replicaCount, len(n.config.Peers)+1)
				
				n.applyCommitted()
			}
		}
	}
}

func (n *Node) applyCommitted() {
	for n.volatile.LastApplied < n.volatile.CommitIndex {
		n.volatile.LastApplied++
		
		entry, ok := n.getEntry(n.volatile.LastApplied)
		if !ok {
			n.logInfo("ERROR: Missing entry at index %d", n.volatile.LastApplied)
			continue
		}
		
		n.logInfo("Applying entry index=%d, term=%d to state machine", entry.Index, entry.Term)
		
		select {
		case n.applyCh <- entry:
		default:
			n.logInfo("WARNING: applyCh full, dropping entry")
		}
	}
}
