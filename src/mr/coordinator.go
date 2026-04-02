package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type workerInfo struct {
	lastSeen time.Time
}

type Coordinator struct {
	// 输入文件列表（map 任务）
	mf []string
	// 任务状态 0 待处理 1 执行中 2 已完成
	book map[string]int
	// reduce 任务数量
	r int
	// map 完成数量
	mtnum int
	// reduce 完成数量
	rtnum int
	// 活跃 worker 列表 workerID -> 最后心跳时间
	workers map[int64]*workerInfo
	// 是否已完成所有任务
	done bool
}

// -------- 任务分配 --------

func (c *Coordinator) CoordinatorHandler(args *Args, reply *Reply) error {
	mu.Lock()
	defer mu.Unlock()
	if c.mtnum < len(c.mf) {
		return c.maptask(args, reply)
	}
	return c.reducetask(args, reply)
}

func (c *Coordinator) maptask(args *Args, reply *Reply) error {
	// 汇报已完成的任务
	if v, ok := c.book[args.Addr]; ok && v == 1 {
		c.book[args.Addr] = 2
		c.mtnum++
	}
	if c.mtnum == len(c.mf) {
		reply.T = "wait"
		return nil
	}
	for i, addr := range c.mf {
		if c.book[addr] == 0 {
			c.book[addr] = 1
			reply.T = "map"
			reply.N = c.r
			reply.Addr = addr
			reply.MapIndex = i
			go c.timeadd(addr)
			return nil
		}
	}
	reply.T = "wait"
	return nil
}

func (c *Coordinator) reducetask(args *Args, reply *Reply) error {
	// 汇报已完成的任务
	if v, ok := c.book[args.Addr]; ok && v == 1 {
		c.book[args.Addr] = 2
		c.rtnum++
	}
	if c.rtnum == c.r {
		reply.T = "exit"
		c.done = true
		return nil
	}
	for i := 0; i < c.r; i++ {
		key := fmt.Sprintf("reduce-%d", i)
		if c.book[key] == 0 {
			c.book[key] = 1
			reply.T = "reduce"
			reply.Index = i
			reply.Addr = key
			go c.timeadd(key)
			return nil
		}
	}
	reply.T = "wait"
	return nil
}

// -------- 超时重置 --------

func (c *Coordinator) timeadd(key string) {
	time.Sleep(10 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if c.book[key] == 1 {
		c.book[key] = 0
		fmt.Printf("任务超时，重新分配：%v\n", key)
	}
}

// -------- 心跳 --------

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	mu.Lock()
	defer mu.Unlock()
	if c.done {
		// 所有任务完成，通知 worker 停止
		reply.Stop = true
		delete(c.workers, args.WorkerID)
		return nil
	}
	// 更新最后心跳时间
	if _, ok := c.workers[args.WorkerID]; !ok {
		c.workers[args.WorkerID] = &workerInfo{}
	}
	c.workers[args.WorkerID].lastSeen = time.Now()
	reply.Stop = false
	return nil
}

// 后台协程：定期清理失联 worker（超过 15 秒无心跳视为已死亡）
func (c *Coordinator) watchWorkers() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		for id, info := range c.workers {
			if time.Since(info.lastSeen) > 15*time.Second {
				fmt.Printf("worker %d 失联，移除\n", id)
				delete(c.workers, id)
			}
		}
		// 所有任务完成且没有活跃 worker，清理中间文件
		if c.done && len(c.workers) == 0 {
			mu.Unlock()
			c.cleanup()
			return
		}
		mu.Unlock()
	}
}

// 清理所有中间文件
func (c *Coordinator) cleanup() {
	files, _ := filepath.Glob("map-result-*")
	for _, f := range files {
		os.Remove(f)
		fmt.Printf("清理中间文件：%v\n", f)
	}
	fmt.Println("所有中间文件已清理，coordinator 退出")
}

// -------- Done --------

func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	// 所有任务完成 且 没有活跃 worker
	return c.done && len(c.workers) == 0
}

// -------- 启动 --------

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mf = append(c.mf, files...)
	c.r = nReduce
	c.book = make(map[string]int)
	c.workers = make(map[int64]*workerInfo)

	for _, v := range c.mf {
		c.book[v] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.book[fmt.Sprintf("reduce-%d", i)] = 0
	}

	go c.watchWorkers()
	c.server()
	return &c
}
