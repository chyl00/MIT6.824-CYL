package kvraft

import "6.824/labrpc"
import "sync/atomic"

var globalIndex int64

type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderId int
	clientId int64
	seqNum int64
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seqNum , 1)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = atomic.AddInt64(&globalIndex , 1)
	ck.seqNum = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	seq := ck.nextSeq()
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
 
	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
 
		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader, ErrTimeout:
				// 换一台服务器重试
			}
		}
		// RPC 失败或 ErrWrongLeader：轮转到下一台
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}
 
// ==================== PutAppend ====================
//
// 幂等性分析：
//   Put/Append 是写操作，网络丢包或 leader 切换可能导致 client 重发。
//   server 端维护 lastSeq[clientId]，若 SeqId <= lastSeq 则直接返回
//   缓存结果，不重复执行状态机，从而保证 exactly-once 语义。
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	seq := ck.nextSeq()
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
 
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
 
		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader, ErrTimeout:
				// 换一台服务器重试
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
