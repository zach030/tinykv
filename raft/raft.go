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
	"math/rand"

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
	Match, Next uint64 // 发送到服务器的下一条日志索引；已经复制到该服务器的最高日志索引
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
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomElectionTimeout int
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
	r := &Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	// 随机的选举过期值
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	hardSt, confSt, _ := r.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confSt.Nodes
	}
	r.Term, r.Vote, r.RaftLog.committed = hardSt.GetTerm(), hardSt.GetVote(), hardSt.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			// 是自身，记录next和match
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
			continue
		}
		r.Prs[peer] = &Progress{Next: lastIndex + 1}
	}
	r.becomeFollower(0, None)
	return r
}

// leader接收到来自client的请求，apply应用到状态机
// leader在接收到超半数的回复后再commit
// 更新server的match与next
// leader将自己维护的next索引数据发送给follower，follower根据自己的commitindex来回复接不接受，leader会将next依次递减发送
// 再向其他服务器发送commit请求，各个节点均commit

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// 获取之前发给此节点的日志索引
	prevAppendLogIdx := r.Prs[to].Next - 1
	// 获取之前发送的日志term
	logTerm, err := r.RaftLog.Term(prevAppendLogIdx)
	if err != nil {
		return false
	}
	// 将prevIdx+1--size 这部分日志发出去
	size := len(r.RaftLog.entries)
	ents := make([]*pb.Entry, 0)
	for i := int(prevAppendLogIdx - r.RaftLog.firstIndex + 1); i < size; i++ {
		ents = append(ents, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   prevAppendLogIdx,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.tickWithHeartbeat()
	case StateFollower, StateCandidate:
		r.tickWithElection()
	}
	// Your Code Here (2A).
}

// tickWithHeartbeat leader用于处理心跳时钟
func (r *Raft) tickWithHeartbeat() {
	r.heartbeatElapsed++
	// 定期发送心跳
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.resetHeartbeatTimer()
		// 发送本地beat消息，需要发心跳
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// tickWithElection follower和candidate用于处理选举时钟
func (r *Raft) tickWithElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		// 选举时钟超时：开始一轮新的选举
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.Vote = None
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	// term自增
	r.Term++
	// 修改vote为自身
	r.Vote = r.id
	r.votes[r.id] = true
	r.Lead = None
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.Lead = r.id
	r.State = StateLeader
	r.Vote = None
	r.resetElectionTimer()
	lastIdx := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastIdx + 2
			r.Prs[peer].Match = lastIdx + 1
			continue
		}
		r.Prs[peer].Next = lastIdx + 1
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType:            0,
		Term:                 r.Term,
		Index:                lastIdx + 1,
		Data:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
	// r.broadcastAppend()
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	r.doPrevCheck(m)
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) doPrevCheck(m pb.Message) {
	// 如果是发送消息，不做检查
	if m.From == r.id {
		return
	}
	// 如果是接收消息
	// 如果收到消息的term更大，则需要更新term
	if m.Term > r.Term {
		// 变为follow
		r.becomeFollower(m.Term, None)
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 需要leader发送广播心跳
		r.broadcastHeartBeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}
	return nil
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIdx := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIdx + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.broadcastAppend()
}

// startElection follower心跳超时成为候选人 or 候选人选举超时，发起选举
func (r *Raft) startElection() {
	// 成为候选人
	r.becomeCandidate()
	// 重置选举超时计时器
	r.resetElectionTimer()
	if len(r.Prs) < 2 {
		r.becomeLeader()
		return
	}
	// 发送请求投票rpc
	r.broadcastRequestVote()
}

// handleRequestVote 接收投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	// 判断对方的任期
	if m.Term != None && m.Term < r.Term {
		// 对方任期小，拒绝投票
		r.sendResponseVote(m.From, true)
		return
	}
	// 判断自己是否投过票
	if r.Vote != None && r.Vote != m.From {
		// 当前任期内已经给其他人投过票，拒绝此次投票
		r.sendResponseVote(m.From, true)
		return
	}
	// todo 判断日志索引
	r.Vote = m.From
	r.resetElectionTimer()
	// 成功投票给对方
	r.sendResponseVote(m.From, false)
}

// resetHeartbeatTimer 重置心跳超时计时器
func (r *Raft) resetHeartbeatTimer() {
	r.heartbeatElapsed = 0
}

// resetElectionTimer 重置选举超时计时器
func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) broadcastRequestVote() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer)
	}
}

// handleVoteResponse candidate用于处理其他人的投票回复
func (r *Raft) handleVoteResponse(m pb.Message) {
	// 判断对方任期
	r.votes[m.From] = !m.Reject
	agree := 0
	allVote := len(r.votes)
	for _, state := range r.votes {
		if state {
			agree++
		}
	}
	// 超半数 获选
	if agree > len(r.Prs)/2 {
		r.becomeLeader()
	}
	// 有其他candidate获选
	if allVote-agree > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) broadcastHeartBeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// sendRequestVote candidate send request for vote
func (r *Raft) sendRequestVote(peer uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      peer,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendResponseVote follower/candidate send response for vote-request
func (r *Raft) sendResponseVote(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	// todo handle append-response
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if r.Term != None && r.Term > m.Term {
		// 如果leader的term更小，则拒绝此次心跳
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	// 接收心跳信息，与leader同步
	r.Lead = m.From
	r.resetElectionTimer()
	r.resetHeartbeatTimer()
	r.sendHeartbeatResponse(m.From, false)
	// Your Code Here (2A).
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
