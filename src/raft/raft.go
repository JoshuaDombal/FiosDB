package raft

import (
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"
)

const HeartbeatInterval = 50 * time.Millisecond
const ElectionTimeoutMin = 150 * time.Millisecond
const ElectionTimeoutMax = 300 * time.Millisecond

type Peer struct {
	ipAddr string
	port   int
}

type Entry struct {
	Command interface{} `json:"command"`
	Term    int         `json:"term"`
	Index   int         `json:"index"`
}

type AppendEntriesRequestWrapper struct {
	req AppendEntriesRequest
	w   http.ResponseWriter
}

type RequestVoteRequestWrapper struct {
	req RequestVoteRequest
	w   http.ResponseWriter
}

type AppendEntriesRequest struct {
	Id          int     `json:"id"`
	Term        int     `json:"term"`
	CommitIdx   int     `json:"commitIdx"`
	PrevLogIdx  int     `json:"prevLogIdx"`
	PrevLogTerm int     `json:"prevLogTerm"`
	Entries     []Entry `json:"entries"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type RequestVoteRequest struct {
	Term         int `json:"term"`
	Id           int `json:"id"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("unreachable")
	}
}

type Raft struct {
	// mutex to lock the state of this raft instance
	mu sync.Mutex

	// TODO: need to persist fields that should be persisted
	currentTerm int
	votedFor    int
	id          int
	peerIds     []int
	state       State

	commitIdx      int
	lastAppliedIdx int

	// contains the index at which we are certain the peer's log matches our log up to
	matchIdx map[int]int
	// next location where we think the peer matches. This and matchIndex are needed as when we become a leader
	// we set the matchIdx for each peer to -1 while nextIndex stays, this allows us to work backward from the end to
	// learn the match index
	nextIdx map[int]int

	log *Log
	rpc *RPC

	// when client sends a submit request and entry has been persisted in our log
	submitChan chan interface{}
	// when the term of this raft instance has changed
	termChangeChan chan interface{}
	// when the state of this raft instance has changed
	stateChangeChan chan interface{}
	// when we receive an AppendEntries message from the leader
	heardFromLeaderChan chan interface{}
	// when we have new commits that can be delivered to the client
	newCommitReadyChan chan interface{}
	// entries that can be delivered to the client
	commitChan chan Entry

	// client supplied channel to send command which have been committed
	committedCommands chan interface{}
}

func NewRaft(id int, peerIds []int, idToPeerMap map[int]Peer, committedCommands chan interface{}) *Raft {
	raft := &Raft{
		mu:                  sync.Mutex{},
		currentTerm:         0,
		votedFor:            -1,
		id:                  id,
		peerIds:             peerIds,
		state:               Follower,
		commitIdx:           -1,
		lastAppliedIdx:      -1,
		matchIdx:            make(map[int]int),
		nextIdx:             make(map[int]int),
		log:                 NewLog(),
		rpc:                 NewRPC(id, idToPeerMap),
		submitChan:          make(chan interface{}),
		termChangeChan:      make(chan interface{}),
		stateChangeChan:     make(chan interface{}),
		heardFromLeaderChan: make(chan interface{}),
		newCommitReadyChan:  make(chan interface{}),
		commitChan:          make(chan Entry),
	}

	go func () {
		for aeWrapper := range raft.rpc.aeChan {
			go raft.onAppendEntries(aeWrapper)
		}
	}()

	go func () {
		for rvWrapper := range raft.rpc.rvChan {
			go raft.onRequestVote(rvWrapper)
		}
	}()

	// TODO: problems: channels are not broadcast, they are one to one so all the channels we have above
	//   don't work like you might hope. Also we need to send successfully committed commands to the client commit chan
	//   so that they can apply the command to their state machine

	go raft.runElectionTimer()
	return raft
}

func (r *Raft) Submit(command interface{}) bool {
	// Only the leader can accept submit commands. Client will need to retry with another node
	if r.state != Leader {
		return false
	}

	r.mu.Lock()
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	// Append this entry to our log
	idx := r.log.Append(savedCurrentTerm, command)

	// Notify other threads that a new client command has been appended to the log and we should begin replicating
	r.submitChan <- struct{}{}

	// Block until entries have been committed up to the index at which we appended this client command. If the committed
	// command is the same client command that we just appended then we can return success to the caller, else this command
	// was not successfully committed
	for {
		select {
		case committedEntry := <-r.commitChan:
			if committedEntry.Index > idx {
				continue
			}

			return committedEntry.Index == idx && committedEntry.Term == savedCurrentTerm
		}
	}
}

func (r *Raft) onAppendEntries(aeWrapper AppendEntriesRequestWrapper) {
	request := aeWrapper.req
	if request.Term > r.currentTerm || (request.Term == r.currentTerm && r.state != Follower) {
		r.becomeFollower(r.currentTerm)
	}

	success := false
	if request.Term == r.currentTerm {
		prevCommand, err := r.log.Get(request.PrevLogIdx)
		if err == nil && prevCommand.Term == request.Term {
			// Our logs do indeed match up to where the leader thinks they match
			r.log.BatchPut(request.Entries)
			if request.CommitIdx > r.commitIdx {
				r.commitIdx = request.CommitIdx
				r.newCommitReadyChan <- struct{}{}
			}

			success = true
		}

		// this needs to go outside the above if command. When a node becomes a leader it begins probing the peers to
		// bring the match index in sync so it may send many request where the prevCommand term does not match before
		// it finds the location where the logs match
		r.heardFromLeaderChan <- struct{}{}
	}

	r.rpc.SendAppendEntriesResponse(aeWrapper.w, AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: success,
	})
}

func (r *Raft) onRequestVote(rvWrapper RequestVoteRequestWrapper) {
	request := rvWrapper.req
	if request.Term > r.currentTerm {
		r.becomeFollower(request.Term)
	}

	voteGranted := false
	if request.Term == r.currentTerm {
		// if we already voted for this peer for this term
		if request.Id == r.votedFor {
			voteGranted = true
		}

		// if their log is at least as up to date as our log
		if request.LastLogTerm > r.log.GetLatestTerm() || (request.LastLogTerm == r.log.GetLatestTerm() && request.LastLogIndex >= r.log.GetLatestIndex()) {
			voteGranted = true
		}
	}

	r.rpc.SendRequestVoteResponse(rvWrapper.w, RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: voteGranted,
	})
}

func (r *Raft) commitChanSender() {
	for range r.newCommitReadyChan {
		r.mu.Lock()
		savedCommitIdx := r.commitIdx
		savedLastAppliedIdx := r.lastAppliedIdx
		entries := r.log.BatchGet(savedCommitIdx, savedLastAppliedIdx)
		r.lastAppliedIdx = r.commitIdx
		r.mu.Unlock()

		for _, entry := range entries {
			r.commitChan <- entry
		}
	}
}

func (r *Raft) startLeader() {
	r.state = Leader
	r.stateChangeChan <- struct{}{}

	// nodes who are not the leader do not know the state of the logs of the other nodes
	for _, peerId := range r.peerIds {
		r.nextIdx[peerId] = r.log.GetLatestIndex()
		r.matchIdx[peerId] = -1
	}

	timer := time.NewTimer(HeartbeatInterval)
	for {
		select {
		case state := <-r.stateChangeChan:
			if state != Leader {
				return
			}
		case <-r.submitChan:
		case <-timer.C:
			r.leaderSendHeartbeats()
			timer.Reset(HeartbeatInterval)
		}
	}
}

func (r *Raft) becomeFollower(term int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Follower {
		r.state = Follower
		r.stateChangeChan <- struct{}{}
	}
	if r.currentTerm != term {
		r.currentTerm = term
		r.termChangeChan <- struct{}{}
	}

	r.votedFor = -1

	r.runElectionTimer()
}

func (r *Raft) runElectionTimer() {
	timer := time.NewTimer(time.Duration(rand.Intn(int(ElectionTimeoutMax)-int(ElectionTimeoutMin))) + ElectionTimeoutMin)
	for {
		select {
		case <-timer.C:
			r.startElection()
			return
		case <-r.heardFromLeaderChan:
			timer.Reset(time.Duration(rand.Intn(int(ElectionTimeoutMax)-int(ElectionTimeoutMin))) + ElectionTimeoutMin)
		case <-r.termChangeChan:
			return
		case <-r.stateChangeChan:
			if r.state == Leader {
				return
			}
		}
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.stateChangeChan <- struct{}{}
	r.currentTerm++
	r.termChangeChan <- struct{}{}
	r.votedFor = r.id
	savedCurrentTerm := r.currentTerm
	savedLastLogIndex := r.log.GetLatestIndex()
	savedLastLopTerm := r.log.GetLatestTerm()
	r.mu.Unlock()

	votesReceived := 1
	for _, peerId := range r.peerIds {
		go func(peerId int) {
			response, err := r.rpc.SendRequestVoteRequest(peerId, RequestVoteRequest{
				Term:         savedCurrentTerm,
				Id:           r.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLopTerm,
			})

			r.mu.Lock()
			defer r.mu.Unlock()
			if err != nil || r.state != Candidate || r.currentTerm != savedCurrentTerm {
				// either this RPC failed or the state of this raft instance has advanced and this election
				// is no longer relevant
				return
			}

			if savedCurrentTerm < response.Term {
				r.becomeFollower(response.Term)
			}

			if response.VoteGranted == true {
				votesReceived++
				if votesReceived > len(r.peerIds)/2 {
					r.startLeader()
				}
			}
		}(peerId)
	}

	r.runElectionTimer()
}

func (r *Raft) leaderSendHeartbeats() {
	r.mu.Lock()
	savedCurrentTerm := r.currentTerm
	r.mu.Unlock()

	for _, peerId := range r.peerIds {
		go func(peerId int) {
			r.mu.Lock()
			savedCommitIdx := r.commitIdx
			nextIdx := r.nextIdx[peerId]
			speculativePrevLogIdx := nextIdx - 1
			speculativePrevLogTerm := r.log.GetTermForIndex(speculativePrevLogIdx)
			entries := r.log.BatchGet(nextIdx, -1)
			r.mu.Unlock()

			response, err := r.rpc.SendAppendEntriesRequest(peerId, AppendEntriesRequest{
				Id:          r.id,
				Term:        savedCurrentTerm,
				CommitIdx:   savedCommitIdx,
				PrevLogIdx:  speculativePrevLogIdx,
				PrevLogTerm: speculativePrevLogTerm,
				Entries:     entries,
			})

			r.mu.Lock()
			defer r.mu.Unlock()
			if err != nil || r.state != Leader {
				return
			}

			if response.Term > savedCurrentTerm {
				r.becomeFollower(response.Term)
			}

			if response.Success {
				r.nextIdx[peerId] = nextIdx + len(entries)
				r.matchIdx[peerId] = r.nextIdx[peerId] - 1

				// check if any log entries now have a majority
				matchIndices := make([]int, 0)
				for _, matchIdx := range r.matchIdx {
					matchIndices = append(matchIndices, matchIdx)
				}
				sort.Ints(matchIndices)
				largestIdxWithMajority := matchIndices[len(matchIndices)/2+1]
				if largestIdxWithMajority > r.commitIdx {
					r.newCommitReadyChan <- struct{}{}
					r.commitIdx = largestIdxWithMajority
				}
			} else {
				r.nextIdx[peerId] -= 1
			}
		}(peerId)
	}
}
