package shardkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady"
)

type Err string

// ==================== 客户端 KV ops ====================

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqId    int64
}

type GetReply struct {
	Err   Err
	Value string
}

// ==================== 去重表条目（跨 group 传输） ====================

// DupEntry 放 common.go，因为 PullShardReply 需要它
type DupEntry struct {
	SeqId int64
	Err   Err
	Value string
}

// ==================== Shard 迁移 RPC ====================

// PullShard：新 owner 向旧 owner 拉取 shard 数据
type PullShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type PullShardReply struct {
	Err      Err
	Shards   map[int]map[string]string // shardId -> kv
	DupTable map[int64]DupEntry        // 随 shard 一起迁移，保证幂等性
}

// GCShard：新 owner 通知旧 owner 删除已迁移的 shard 数据
type GCArgs struct {
	ConfigNum int
	ShardIds  []int
}

type GCReply struct {
	Err Err
}