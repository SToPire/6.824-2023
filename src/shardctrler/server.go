package shardctrler

import (
	"container/heap"
	"sort"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type clientData struct {
	RequestId int
	Config    Config // return value of Query
}

// message from apply goroutine to RPC handler
type ReturnData struct {
	err       Err
	config    Config
	requestID int
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clientMap map[int64]clientData      // indexed by client id
	chanMap   map[int64]chan ReturnData // store channel for each client

	configs []Config // indexed by config num
}

const (
	JoinOp  = 0
	LeaveOp = 1
	MoveOp  = 2
	QueryOp = 3
)

type Op struct {
	Type int

	// Join
	Servers map[int][]string

	// Leave
	GIDs []int

	// Move
	Shard int
	GID   int

	// Query
	Num int

	ClientId  int64
	RequestId int
}

// heap used in rebalance
type groupData struct {
	gid     int
	nShards int
}

type SGHeap []groupData

func (h SGHeap) Len() int {
	return len(h)
}

func (h SGHeap) Less(i, j int) bool {
	if h[i].nShards == h[j].nShards {
		return h[i].gid < h[j].gid
	}

	return h[i].nShards < h[j].nShards
}

func (h SGHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *SGHeap) Push(v interface{}) {
	*h = append(*h, v.(groupData))
}

func (h *SGHeap) Pop() interface{} {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

func (sc *ShardCtrler) rebalance(old *Config, new *Config) {
	if len(old.Groups) == 0 {
		/* old assignment is null, all it takes is an equal share */
		groups := make([]int, 0)
		for gid := range new.Groups {
			groups = append(groups, gid)
		}

		sort.Slice(groups, func(i, j int) bool {
			return groups[i] < groups[j]
		})

		for i := 0; i < NShards; i++ {
			new.Shards[i] = groups[i%len(groups)]
		}

		return
	}

	if len(new.Groups) == 0 {
		/* all groups are removed */
		for i := 0; i < NShards; i++ {
			new.Shards[i] = 0
		}

		return
	}

	if len(old.Groups) < len(new.Groups) {
		/* adding new group */
		/* mapping: gid -> []shards */
		shardsPerGroup := make(map[int][]int)
		for gid := range new.Groups {
			shardsPerGroup[gid] = make([]int, 0)
		}
		for i, gid := range old.Shards {
			shardsPerGroup[gid] = append(shardsPerGroup[gid], i)
		}

		groups := make([]int, 0)
		for gid := range new.Groups {
			groups = append(groups, gid)
		}

		/* sort groups by number of shards mapped to them */
		sort.Slice(groups, func(i, j int) bool {
			if len(shardsPerGroup[groups[i]]) == len(shardsPerGroup[groups[j]]) {
				return groups[i] < groups[j]
			}

			return len(shardsPerGroup[groups[i]]) > len(shardsPerGroup[groups[j]])
		})

		length := len(groups)
		if length > NShards {
			length = NShards
		}

		avg := NShards / length
		remain := NShards % length
		last := length - 1
		for i := 0; i < length; i++ {
			above := 0
			if i < remain {
				above = 1
			}

			/* move shards until the number of shards in the group is equal to avg+above */
			for len(shardsPerGroup[groups[i]]) > avg+above {
				shard := shardsPerGroup[groups[i]][0]
				shardsPerGroup[groups[i]] = shardsPerGroup[groups[i]][1:]
				shardsPerGroup[groups[last]] = append(shardsPerGroup[groups[last]], shard)

				new.Shards[shard] = groups[last]

				lastAbove := 0
				if last < remain {
					lastAbove = 1
				}
				if len(shardsPerGroup[groups[last]]) == avg+lastAbove {
					last--
				}
			}
		}
	} else if len(old.Groups) > len(new.Groups) {
		/* removing group */
		shardsPerGroup := make(map[int][]int)
		deleted := make([]int, 0)
		for i, gid := range old.Shards {
			shardsPerGroup[gid] = append(shardsPerGroup[gid], i)
			_, ok := new.Groups[gid]
			if !ok {
				deleted = append(deleted, gid)
			}
		}

		hp := &SGHeap{}
		heap.Init(hp)
		for gid := range new.Groups {
			heap.Push(hp, groupData{gid: gid, nShards: len(shardsPerGroup[gid])})
		}

		/* make the result deterministic */
		sort.Slice(deleted, func(i, j int) bool {
			if len(shardsPerGroup[deleted[i]]) == len(shardsPerGroup[deleted[j]]) {
				return deleted[i] < deleted[j]
			}

			return len(shardsPerGroup[deleted[i]]) > len(shardsPerGroup[deleted[j]])
		})

		for i := 0; i < len(deleted); i++ {
			deletedGid := deleted[i]

			/* move the shards from deleted group to the group with least shards */
			for len(shardsPerGroup[deletedGid]) > 0 {
				shard := shardsPerGroup[deletedGid][0]
				shardsPerGroup[deletedGid] = shardsPerGroup[deletedGid][1:]

				data := heap.Pop(hp).(groupData)
				new.Shards[shard] = data.gid
				data.nShards++
				heap.Push(hp, data)
			}
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()

	if args.RequestId <= sc.clientMap[args.ClientId].RequestId {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	op := Op{
		Type:      JoinOp,
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	c := make(chan ReturnData)
	sc.chanMap[args.ClientId] = c

	sc.mu.Unlock()

	for rdata := range c {
		if rdata.requestID == args.RequestId {
			reply.Err = rdata.err
			reply.WrongLeader = false
			return
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()

	if args.RequestId <= sc.clientMap[args.ClientId].RequestId {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	op := Op{
		Type:      LeaveOp,
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	c := make(chan ReturnData)
	sc.chanMap[args.ClientId] = c

	sc.mu.Unlock()

	for rdata := range c {
		if rdata.requestID == args.RequestId {
			reply.Err = rdata.err
			reply.WrongLeader = false
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()

	if args.RequestId <= sc.clientMap[args.ClientId].RequestId {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	op := Op{
		Type:      MoveOp,
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	c := make(chan ReturnData)
	sc.chanMap[args.ClientId] = c

	sc.mu.Unlock()

	for rdata := range c {
		if rdata.requestID == args.RequestId {
			reply.Err = rdata.err
			reply.WrongLeader = false
			return
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()

	if args.RequestId <= sc.clientMap[args.ClientId].RequestId {
		reply.Config = sc.clientMap[args.ClientId].Config
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}

	op := Op{
		Type:      QueryOp,
		Num:       args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	c := make(chan ReturnData)
	sc.chanMap[args.ClientId] = c

	sc.mu.Unlock()

	for rdata := range c {
		if rdata.requestID == args.RequestId {
			reply.Err = rdata.err
			reply.WrongLeader = false
			reply.Config = rdata.config
			return
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) apply() {
	for msg := range sc.applyCh {
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)
		clientId := op.ClientId
		requestId := op.RequestId

		sc.mu.Lock()

		// A request could be duplicated in log, so we need to check if it has been applied before
		if sc.clientMap[clientId].RequestId >= requestId {
			sc.mu.Unlock()
			continue
		}

		newClientData := clientData{
			RequestId: requestId,
		}

		var newConfig Config
		lastConfig := sc.configs[len(sc.configs)-1]
		newConfig.Num = lastConfig.Num + 1
		newConfig.Shards = lastConfig.Shards
		newConfig.Groups = make(map[int][]string)
		for k, v := range lastConfig.Groups {
			newConfig.Groups[k] = v
		}

		if op.Type == JoinOp || op.Type == LeaveOp {
			if op.Type == JoinOp {
				for k, v := range op.Servers {
					newConfig.Groups[k] = v
				}
			} else if op.Type == LeaveOp {
				for _, gid := range op.GIDs {
					delete(newConfig.Groups, gid)
				}
			}

			sc.rebalance(&lastConfig, &newConfig)

			sc.configs = append(sc.configs, newConfig)
		} else if op.Type == MoveOp {
			newConfig.Num++
			newConfig.Shards[op.Shard] = op.GID
			sc.configs = append(sc.configs, newConfig)
		} else if op.Type == QueryOp {
			if op.Num == -1 || op.Num >= len(sc.configs) {
				newClientData.Config = lastConfig
			} else {
				newClientData.Config = sc.configs[op.Num]
			}
		} else {
			panic("invalid op type")
		}

		sc.clientMap[clientId] = newClientData

		c, ok := sc.chanMap[clientId]
		if !ok {
			sc.mu.Unlock()
			continue
		}

		// prevent sending to an outdated channel
		delete(sc.chanMap, clientId)
		sc.mu.Unlock()

		c <- ReturnData{
			err:       OK,
			config:    newClientData.Config,
			requestID: requestId,
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientMap = make(map[int64]clientData)
	sc.chanMap = make(map[int64]chan ReturnData)

	go sc.apply()

	return sc
}
