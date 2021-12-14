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
	// snapshot-----memory-storage-----------------------------------unstable-entries
	//              firstIdx----apply/commit---lastIdx,unstableOffset
	// 维护待快照的entry，定期进行snapshot，是stabled（稳定）数据
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// storage中稳定的数据中的最高位
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 上层已应用到状态机的index
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 持久存储与未持久存储的界限
	stabled uint64

	// all entries that have not yet compact.
	// 所有未进行快照的log（inmemory和unstable）
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	if storage == nil {
		panic("storage must not nil")
	}
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIdx, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		panic(err)
	}
	// Your Code Here (2A).
	return &RaftLog{
		storage:    storage,
		committed:  firstIdx - 1,
		applied:    firstIdx - 1,
		stabled:    lastIdx,
		entries:    entries,
		firstIndex: firstIdx,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if first > l.firstIndex {
		if len(l.entries) > 0 {
			entries := l.entries[first-l.firstIndex:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.firstIndex = first
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		// firstIdx------lastIdx(stable)-----apply----commit
		//            unstable:|------------------------------|
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	applyIdx := l.applied - l.firstIndex + 1
	commitIdx := l.committed - l.firstIndex + 1
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
	if len(l.entries) > 0 && i >= l.firstIndex {
		// log.Infof("entry size:%v,pos is:%v,fistidx:%v", len(l.entries), i, l.firstIndex)
		return l.entries[i-l.firstIndex].Term, nil
	}
	return 0, nil
}
