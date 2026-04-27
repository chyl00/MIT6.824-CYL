package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
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

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	n, _ := rand.Int(rand.Reader, max)
	return n.Int64()
}

// ==================== Clerk ====================

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId  int64
	seqId     int64
	mu        sync.Mutex
	leaderIds map[int]int // gid -> 上次成功的 server index，减少轮询
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		clientId:  genClientId(),
		leaderIds: make(map[int]int),
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seqId, 1)
}

// ==================== Get ====================

func (ck *Clerk) Get(key string) string {
	seq := ck.nextSeq()
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    seq,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.mu.Lock()
			si := ck.leaderIds[gid]
			ck.mu.Unlock()

			for i := 0; i < len(servers); i++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.mu.Lock()
					ck.leaderIds[gid] = si
					ck.mu.Unlock()
					if reply.Err == ErrNoKey {
						return ""
					}
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break // 该 group 不负责此 shard，刷新 config
				}
				// ErrWrongLeader / ErrTimeout / RPC 失败：换下一台
				si = (si + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

// ==================== PutAppend ====================

func (ck *Clerk) PutAppend(key string, value string, op string) {
	seq := ck.nextSeq()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    seq,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.mu.Lock()
			si := ck.leaderIds[gid]
			ck.mu.Unlock()

			for i := 0; i < len(servers); i++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.mu.Lock()
					ck.leaderIds[gid] = si
					ck.mu.Unlock()
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				si = (si + 1) % len(servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}