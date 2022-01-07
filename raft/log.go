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
	// raft log的存储实现
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// 已提交日志的索引
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 上层已应用到状态机的索引
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 已持久化的索引
	stabled uint64

	// all entries that have not yet compact.
	// 内存中存储的log
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	FirstIdx uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	if storage == nil {
		panic("storage must not nil")
	}
	var (
		firstIdx uint64
		lastIdx  uint64
		err      error
		entries  []pb.Entry
	)
	if firstIdx, err = storage.FirstIndex(); err != nil {
		panic(err)
	}
	if lastIdx, err = storage.LastIndex(); err != nil {
		panic(err)
	}
	if entries, err = storage.Entries(firstIdx, lastIdx+1); err != nil {
		panic(err)
	}
	// Your Code Here (2A).
	return &RaftLog{
		storage:   storage,
		committed: firstIdx - 1,
		applied:   firstIdx - 1,
		stabled:   lastIdx,
		entries:   entries,
		FirstIdx:  firstIdx,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.FirstIdx {
		if len(l.entries) > 0 {
			entries := l.entries[first-l.FirstIdx:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.FirstIdx = first
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// 取stabled下标往后的log
		return l.entries[l.getRelativeIdx(l.stabled):]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	applyIdx := l.getRelativeIdx(l.applied)
	commitIdx := l.getRelativeIdx(l.committed)
	if len(l.entries) > 0 {
		return l.entries[applyIdx:commitIdx]
	}
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	var index uint64
	// 如果有还未持久化的快照，找到下标
	if !IsEmptySnap(l.pendingSnapshot) {
		index = l.pendingSnapshot.Metadata.Index
	}
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, index)
	}
	la, err := l.storage.LastIndex()
	if err != nil {
		return 0
	}
	return max(la, index)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && i >= l.FirstIdx {
		// log.Infof("entry size:%v,pos is:%v,fistidx:%v", len(l.entries), i, l.FirstIdx)
		return l.entries[i-l.FirstIdx].Term, nil
	}
	return 0, nil
}

func (l *RaftLog) getRelativeIdx(index uint64) uint64 {
	return index - l.FirstIdx + 1
}
