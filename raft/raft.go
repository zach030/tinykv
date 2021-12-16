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
	"sort"

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

	randomizedElectionTimeout int
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
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// 从raftlog中恢复存储的状态
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
	r.becomeFollower(r.Term, None)
	return r
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.resetElectionTimer()
	r.resetHeartbeatTimer()
	r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	log.Debugf("%x became follower at term %d", r.id, r.Term)
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	// 修改vote为自身
	r.Vote = r.id
	r.votes[r.id] = true
	log.Debugf("%x became candidate at term %d", r.id, r.Term)
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		panic("invalid transfer from follower to leader")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	lastIdx := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastIdx + 2
			r.Prs[peer].Match = lastIdx + 1
			continue
		}
		r.Prs[peer].Next = lastIdx + 1
		r.Prs[peer].Match = 0
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIdx + 1,
	})
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
		return
	}
	r.broadcastAppend()
	log.Debugf("%x became leader at term %d", r.id, r.Term)
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
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
	r.electionElapsed++
	// 定期发送心跳
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.resetHeartbeatTimer()
		// 发送本地beat消息，需要发心跳
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Errorf("error occurred during heartbeat: %v", err)
		}
	}
}

// tickWithElection follower和candidate用于处理选举时钟
func (r *Raft) tickWithElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.resetElectionTimer()
		// 选举时钟超时：开始一轮新的选举
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup}); err != nil {
			log.Errorf("error occurred during election: %v", err)
		}
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
		r.resetElectionTimer()
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.resetElectionTimer()
		r.Lead = m.From
		r.handleAppendEntries(m)
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
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
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
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	}
	return nil
}

func (r *Raft) broadcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
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
		panic(err)
	}
	// 将prevIdx+1--size 这部分日志发出去
	size := len(r.RaftLog.entries)
	ents := make([]*pb.Entry, 0)
	for i := int(prevAppendLogIdx + 1 - r.RaftLog.firstIndex); i < size; i++ {
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
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	}
	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIdx := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIdx + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entries[i])
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		// 如果只有一个leader节点，则不需要广播，直接commit
		r.RaftLog.committed = r.Prs[r.id].Match
		return
	}
	r.broadcastAppend()
}

// startElection follower心跳超时成为候选人 or 候选人选举超时，发起选举
func (r *Raft) startElection() {
	// 成为候选人
	r.becomeCandidate()
	r.resetHeartbeatTimer()
	// 重置选举超时计时器
	r.resetElectionTimer()
	if len(r.Prs) < 2 {
		r.becomeLeader()
		return
	}
	lastIdx := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		panic(err)
	}
	// 发送请求投票rpc
	r.broadcastRequestVote(lastIdx, lastLogTerm)
}

// handleRequestVote 接收投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	// 判断对方的任期
	if m.Term != None && m.Term < r.Term {
		// 对方任期小，拒绝投票
		r.sendVoteResponse(m.From, true)
		return
	}
	// 判断自己是否投过票
	if r.Vote != None && r.Vote != m.From {
		// 当前任期内已经给其他人投过票，拒绝此次投票
		r.sendVoteResponse(m.From, true)
		return
	}
	lastIdx := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIdx)
	if err != nil {
		panic(err)
	}
	if m.LogTerm < lastLogTerm || m.LogTerm == lastLogTerm && m.Index < lastIdx {
		r.sendVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.resetElectionTimer()
	// 成功投票给对方
	r.sendVoteResponse(m.From, false)
}

// resetHeartbeatTimer 重置心跳超时计时器
func (r *Raft) resetHeartbeatTimer() {
	r.heartbeatElapsed = 0
}

// resetElectionTimer 重置选举超时计时器
func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) broadcastRequestVote(index, term uint64) {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendVoteRequest(peer, index, term)
	}
}

// handleVoteResponse candidate用于处理其他人的投票回复
func (r *Raft) handleVoteResponse(m pb.Message) {
	// 判断对方任期
	if m.Term != None && m.Term < r.Term {
		return
	}
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

// sendVoteRequest candidate send request for vote
func (r *Raft) sendVoteRequest(to, index, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendVoteResponse follower/candidate send response for vote-request
func (r *Raft) sendVoteResponse(to uint64, reject bool) {
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
	if m.Term != None && m.Term < r.Term {
		// 如果leader发来的term更小，则拒绝
		r.sendAppendResponse(m.From, None, None, true)
		return
	}
	// 重置选举计时器
	r.resetElectionTimer()
	r.Lead = m.From
	logLastIdx := r.RaftLog.LastIndex()
	// log.Infof("node:%v ,log last index is:%v", r.id, logLastIdx)
	if m.Index > logLastIdx {
		// log.Infof("leader msg index:%v,my log index:%v", m.Index, logLastIdx)
		// 如果leader发来的消息索引大于我本地最大的消息，则修改期望发送的消息
		r.sendAppendResponse(m.From, logLastIdx+1, None, true)
		return
	}
	logFirstIdx := r.RaftLog.firstIndex
	// log.Infof("node:%v ,log first index is:%v", r.id, logFirstIdx)
	if m.Index >= logFirstIdx {
		// log.Infof("node:%v,recv msg index:%v bigger than first index:%v, need to find out conflict", r.id, m.Index, logFirstIdx)
		// 根据msg的index查到term
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil {
			panic(err)
		}
		// log.Infof("node:%v,entry term:%v,index:%v", r.id, logTerm, m.Index)
		// 如果不等，则说明有冲突
		if logTerm != m.LogTerm {
			offset := sort.Search(int(m.Index+1-r.RaftLog.firstIndex), func(i int) bool {
				return r.RaftLog.entries[i].Term == logTerm
			})
			index := uint64(offset) + r.RaftLog.firstIndex
			// log.Infof("node:%v,find conflict, recv msg entry term:%v , log entry term is:%v", r.id, m.LogTerm, logTerm)
			r.sendAppendResponse(m.From, index, logTerm, true)
			return
		}
	}
	for _, entry := range m.Entries {
		if entry.Index < logFirstIdx {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if logTerm != entry.Term {
				index := int(entry.Index - r.RaftLog.firstIndex)
				r.RaftLog.entries[index] = *entry
				// 将后续可能有冲突的entries清除
				r.RaftLog.entries = r.RaftLog.entries[:index+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			// 无冲突，将日志放入
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	// log.Infof("node:%v,state:%v,msg commit:%v,log commit:%v", r.id, r.State.String(), m.Commit, r.RaftLog.committed)
	if m.Commit > r.RaftLog.committed {
		// 取两者的较小值
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		// log.Infof("node:%v,state:%v,commit:%v", r.id, r.State.String(), r.RaftLog.committed)
	}
	r.sendAppendResponse(m.From, r.RaftLog.LastIndex(), None, false)
	// Your Code Here (2A).
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	// 如果其他节点拒绝接收
	if m.Reject {
		idx := m.Index
		if m.LogTerm > 0 {
			logTerm := m.LogTerm
			l := r.RaftLog
			sliceIndex := sort.Search(len(l.entries), func(i int) bool { return l.entries[i].Term >= logTerm })
			if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
				idx = uint64(sliceIndex) + l.firstIndex
				// log.Infof("leader change new idx for node:%v, next:%v", m.From, idx)
			}
		}
		// 更新next
		// log.Infof("leader change for node:%v, old next:%v,new next:%v", m.From, m.Index, idx)
		r.Prs[m.From].Next = idx
		// 继续发送append请求
		r.sendAppend(m.From)
		return
	}
	// 同意接收数据，判断server返回的index
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		// 判断是否超半数接收append，leader可提交
		r.leaderCommit()
	}
}

// leaderCommit 判断是否收到半数的append回复，更新commit
func (r *Raft) leaderCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, progress := range r.Prs {
		match[i] = progress.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			// 广播告知其他节点更新commit
			r.broadcastAppend()
		}
	}
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

func (r *Raft) sendAppendResponse(to, index, term uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
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
