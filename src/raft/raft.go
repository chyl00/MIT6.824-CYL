package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateFollower = iota
	StateCandidate
	StateLeader
)

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 500
)

// raft -> stateMachine
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	Persister *Persister
	me        int
	dead      int32
	state     int
	applyCh   chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []LogEntry

	lastIncludedIndex int
	lastIncludedTerm  int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCond *sync.Cond // 用于触发 apply
}

// ==================== 工具函数 ====================

func randomElectionTimeout() time.Duration {
	ms := ElectionTimeoutMin + rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	return time.Duration(ms) * time.Millisecond
}

// 将全局 index 转为 rf.log 中的切片下标
func (rf *Raft) toSliceIndex(globalIndex int) int {
	return globalIndex - rf.lastIncludedIndex - 1
}

// 获取 rf.log 中最后一条日志的全局 index
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// 获取 rf.log 中最后一条日志的任期
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// 获取指定全局 index 的日志任期
func (rf *Raft) termAt(globalIndex int) int {
	if globalIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	if globalIndex < rf.lastIncludedIndex {
		return -1
	}
	sliceIdx := rf.toSliceIndex(globalIndex)
	if sliceIdx < 0 || sliceIdx >= len(rf.log) {
		return -1
	}
	return rf.log[sliceIdx].Term
}

// ==================== 状态获取 ====================

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == StateLeader
}

// ==================== 2C 持久化 ====================

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.Persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// ==================== 2D 快照 ====================

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果快照已经过时，拒绝
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// 截断日志
	if lastIncludedIndex <= rf.lastLogIndex() {
		sliceIdx := rf.toSliceIndex(lastIncludedIndex)
		rf.log = rf.log[sliceIdx+1:]
	} else {
		rf.log = []LogEntry{}
	}

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.Persister.SaveStateAndSnapshot(w.Bytes(), snapshot)

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	sliceIdx := rf.toSliceIndex(index)
	rf.lastIncludedTerm = rf.log[sliceIdx].Term
	rf.log = rf.log[sliceIdx+1:]
	rf.lastIncludedIndex = index

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.Persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
}

// ==================== 2A 选举相关 ====================

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// 日志是否比自己的更新（up-to-date）
func (rf *Raft) isMoreUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastTerm := rf.lastLogTerm()
	myLastIndex := rf.lastLogIndex()
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 拒绝旧任期的请求
	if args.Term < rf.currentTerm {
		return
	}

	// 发现更高任期，退回 follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// 检查是否可以投票 + 日志是否足够新
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if canVote && rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.state = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	votes := 1
	majority := len(rf.peers)/2 + 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = StateFollower
				rf.votedFor = -1
				rf.persist()
				return
			}

			// 确认仍处于同一任期的 Candidate
			if rf.state != StateCandidate || rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = StateLeader
	// 初始化 nextIndex 和 matchIndex
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	// 立即发送心跳
	rf.heartbeatTimer.Reset(0)
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(randomElectionTimeout())
}

// ==================== 2B AppendEntries ====================

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 快速回退优化字段
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	// 发现 leader，重置状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = StateFollower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// ---- 一致性检查 ----
	// prevLogIndex 在快照之前
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// prevLogIndex 超过当前日志长度
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	// prevLog 任期不匹配
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		conflictTerm := rf.termAt(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm
		// 找到该任期的第一个 index
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.termAt(idx-1) == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// ---- 追加日志 ----
	for i, entry := range args.Entries {
		globalIdx := args.PrevLogIndex + 1 + i
		if globalIdx <= rf.lastIncludedIndex {
			continue
		}
		sliceIdx := rf.toSliceIndex(globalIdx)
		if sliceIdx < len(rf.log) {
			// 冲突则截断
			if rf.log[sliceIdx].Term != entry.Term {
				rf.log = rf.log[:sliceIdx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()
	reply.Success = true

	// ---- 更新 commitIndex ----
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if rf.lastLogIndex() < newCommit {
			newCommit = rf.lastLogIndex()
		}
		rf.commitIndex = newCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ==================== 2D InstallSnapshot ====================

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = StateFollower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 通过 applyCh 发送快照给上层应用
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// ==================== 2B Leader 发送日志 ====================

func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()

	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peer]

	// 如果 follower 需要的日志已经在快照里，发送快照
	if nextIdx <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.Persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(peer, args, reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = StateFollower
			rf.votedFor = -1
			rf.persist()
			return
		}
		if rf.state != StateLeader || rf.currentTerm != args.Term {
			return
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
		}
		if args.LastIncludedIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := rf.termAt(prevLogIndex)

	// 复制 nextIdx 之后的所有日志
	entries := make([]LogEntry, 0)
	if nextIdx <= rf.lastLogIndex() {
		sliceStart := rf.toSliceIndex(nextIdx)
		entries = append(entries, rf.log[sliceStart:]...)
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.tryCommit()
	} else {
		// 快速回退
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			// 找到 leader 中 ConflictTerm 最后一条日志
			newNext := reply.ConflictIndex
			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.termAt(i) == reply.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			rf.nextIndex[peer] = newNext
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
	}
}

// leader 尝试推进 commitIndex
func (rf *Raft) tryCommit() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// ==================== 2B Start ====================

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != StateLeader {
		return -1, -1, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.currentTerm
	rf.matchIndex[rf.me] = index

	// 立即触发复制
	rf.heartbeatTimer.Reset(0)

	return index, term, true
}

// ==================== Apply 协程 ====================

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		// 收集需要提交的日志
		entries := make([]LogEntry, 0)
		startIdx := lastApplied + 1
		for i := startIdx; i <= commitIndex; i++ {
			if i <= rf.lastIncludedIndex {
				continue
			}
			si := rf.toSliceIndex(i)
			if si < 0 || si >= len(rf.log) {
				break
			}
			entries = append(entries, rf.log[si])
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			globalIdx := startIdx + i
			if globalIdx <= rf.lastIncludedIndex {
				continue
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: globalIdx,
			}
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

// ==================== ticker & heartbeat ====================

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != StateLeader {
				rf.startElection()
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				// 向所有 follower 发送心跳/日志
				for peer := range rf.peers {
					if peer == rf.me {
						continue
					}
					go rf.replicateTo(peer)
				}
				rf.heartbeatTimer.Reset(HeartbeatInterval)
			}
			rf.mu.Unlock()
		}
	}
}

// ==================== Make ====================

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)

	// 从持久化状态恢复
	rf.readPersist(persister.ReadRaftState())

	// 启动 ticker 协程
	go rf.ticker()
	// 启动 apply 协程
	go rf.applier()

	return rf
}
