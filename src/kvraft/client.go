package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	clientId int64
	seqId    int64
	leaderId int
}

// ==================== clientId 生成器 ====================
//
// 64bit 布局：
//   [63:32] timestamp(32bit) — Unix 秒
//   [31:10] random(22bit)    — 每秒重新采样
//   [ 9: 0] counter(10bit)   — 同秒内 [0,1023] 回绕

var (
	clientIdMu      sync.Mutex
	clientIdLastSec int64
	clientIdRand    int64
	clientIdCounter int64
)

func genClientId() int64 {
	clientIdMu.Lock()
	defer clientIdMu.Unlock()

	now := time.Now().Unix()

	if now != clientIdLastSec {
		clientIdLastSec = now
		clientIdRand = randBits(22)
		clientIdCounter = 0
	} else {
		clientIdCounter = (clientIdCounter + 1) & 0x3FF
	}

	return (now << 32) | (clientIdRand << 10) | clientIdCounter
}

func randBits(bits uint) int64 {
	max := big.NewInt(1)
	max.Lsh(max, bits)
	n, _ := rand.Int(rand.Reader, max)
	return n.Int64()
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: genClientId(),
		seqId:    0,
		leaderId: 0,
	}
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seqId, 1)
}

// ==================== Get ====================

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
			case ErrWrongLeader:
				// 明确告知不是 leader，换下一台
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrTimeout:
				// Bug3 修复：超时不代表未 apply，用相同 SeqId 继续等待同一 server。
				// 若该 server 其实不是 leader，它会返回 ErrWrongLeader，届时再换。
			}
		} else {
			// RPC 彻底失败（网络断开），才轮转
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

// ==================== PutAppend ====================

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
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			case ErrTimeout:
				// 同上：超时用相同 SeqId 重试，dupTable 保证不重复执行
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
