package mr

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// -------- 雪花算法生成 WorkerID --------

const (
	// 序列号 12 位
	sequenceBits uint8 = 12
	// workerID 10 位
	workerBits uint8 = 10
	// 1ms中最多生成的序列号数量
	maxSequence int64 = -1 ^ (-1 << sequenceBits)
	// 时间戳偏移量
	timeShift uint8 = workerBits + sequenceBits
	// workerID偏移量
	workerShift = sequenceBits
	// 起始时间戳
	epoch int64 = 1700000000000 // 自定义起始时间戳 ms
)

type snowflake struct {
	mu        sync.Mutex
	lastStamp int64
	workerID  int64
	sequence  int64
}

// 单例模式
var sf = &snowflake{workerID: int64(os.Getpid()) & 0x3FF}

// 雪花UUID生成逻辑
func GenerateID() int64 {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	now := time.Now().UnixMilli()
	if now == sf.lastStamp {
		sf.sequence = (sf.sequence + 1) & maxSequence
		if sf.sequence == 0 {
			// 序列号溢出，等到下一毫秒
			for now <= sf.lastStamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		sf.sequence = 0
	}
	sf.lastStamp = now
	// 拼接 UUID
	return (now-epoch)<<timeShift | sf.workerID<<workerShift | sf.sequence
}

// -------- RPC 数据结构 --------

type Args struct {
	Addr string // worker 汇报完成的任务地址
}

type Reply struct {
	T        string // map / reduce / wait / exit
	N        int    // reduce 任务数量
	Addr     string // 任务标识
	Index    int    // reduce 任务索引
	MapIndex int    // map 任务索引
}

type HeartbeatArgs struct {
	WorkerID int64
}

type HeartbeatReply struct {
	Stop bool // true 表示所有任务已完成，worker 应停止心跳并退出
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
