package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// ==================== PutAppend ====================

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64 // 客户端唯一 ID（由 nrand() 生成）
	SeqId    int64 // 单调递增的请求序列号，server 用它去重
}

type PutAppendReply struct {
	Err Err
}

// ==================== Get ====================

type GetArgs struct {
	Key string
	ClientId int64
	SeqId    int64
}

type GetReply struct {
	Err   Err
	Value string
}