package shardctrler

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)
// shardctrler 主要负责管理配置文件
// shardkv 主要是负责管理分片数据
// raft 主要是负责日志数据的同步
type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seqId    int64
	leaderId int
}

// ==================== clientId 生成器 ====================
// 64bit: timestamp(32) | random(22) | counter(10)

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

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seqId, 1)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: genClientId(),
		seqId:    0,
		leaderId: 0,
	}
}

// ==================== Query ====================
// Query 是只读操作，走 Raft log 保证 linearizability

func (ck *Clerk) Query(num int) Config {
	seq := ck.nextSeq()
	args := &QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
	for {
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return reply.Config
		}
		if ok && reply.Err == ErrTimeout {
			// 超时：可能已 apply，继续等同一台
			continue
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

// ==================== Join ====================
// 传入新group和对应的服务IP 只包含新group 旧group不在其中
func (ck *Clerk) Join(servers map[int][]string) {
	seq := ck.nextSeq()
	args := &JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
	for {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		if ok && reply.Err == ErrTimeout {
			continue
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

// ==================== Leave ====================

func (ck *Clerk) Leave(gids []int) {
	seq := ck.nextSeq()
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
	for {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		if ok && reply.Err == ErrTimeout {
			continue
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

// ==================== Move ====================

func (ck *Clerk) Move(shard int, gid int) {
	seq := ck.nextSeq()
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		SeqId:    seq,
	}
	for {
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		if ok && reply.Err == ErrTimeout {
			continue
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}
