package raft

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

// raft -> stateMachine
type ApplyMsg struct {
	// 正常日志消息
	CommandValid bool        // 是否是正常日志消息
	Command      interface{} // 正常日志消息内容
	CommandIndex int         // 正常日志消息索引

	// For 2D: 快照消息
	SnapshotValid bool   // 是否是快照消息
	Snapshot      []byte // 快照消息内容
	SnapshotTerm  int    // 快照消息任期
	SnapshotIndex int    // 快照消息索引
}

type LogEntry struct {
	Term    int         // 任期
	Command interface{} // 命令
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int                 // 角色字段 2 leader 1 candidator 0 follower
	applyCh   chan ApplyMsg       // 向上层提交日志

	// 2A 2B 2C params
	currentTerm       int        // 当前任期
	votedFor          int        // 投票对象
	log               []LogEntry // 日志
	lastIncludedIndex int        // 最后一个包含的日志索引
	lastIncludedTerm  int        // 最后一个包含的日志任期
	electionTimeout   int        // 选举超时时间
	lastHeardTime     int        // 上一次收到心跳的时间
	commitIndex       int        // 已提交的日志索引
	lastApplied       int        // 已应用的日志索引

	// leader params nextIndex和matchIndex
	nextIndex  []int // 下一个要复制的日志索引
	matchIndex []int // 已复制的日志索引

	// 动态计算字段
	// prevLogIndex = nextIndex[peer] - 1
	// prevLogTerm = termAt(prevLogIndex)
	// prevLogIndex int  上一个日志索引
	// prevLogTerm int  上一个日志任期
}

// TODO 2A
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == 0
}

// TODO 2C
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// TODO 2C
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// TODO 2D
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// TODO 2D
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// TODO 2A 2B
type RequestVoteArgs struct {
	Term         int // 任期
	CandidateId  int // 候选者ID
	LastLogIndex int // 最后的日志索引
	LastLogTerm  int // 最后日志任期
}

// TODO 2A
type RequestVoteReply struct {
	Term        int  // follower的任期
	VoteGranted bool // 是否投票给该候选者
}

// TODO 2A 2B
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.currentTerm = args.Term
		rf.state = 0
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

}

// TODO 2A
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// TODO 2B
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// TODO COMMON
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// TODO COMMON
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// TODO 2A
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// TODO 2A 2B 2C
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
