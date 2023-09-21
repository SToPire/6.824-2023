package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOp = iota
	PutOp
	AppendOp
	ReconfOp
	RecvShardsOp
	PushShardsDoneOp
	NullOp
)

const (
	Unavailable = iota
	Available
	Pushing
	Pulling
)

type ShardState int

type KVFields struct {
	Key string
	// for Put and Append
	Value string

	ClientId  int64
	RequestId int
}

type ReconfFields struct {
	Config shardctrler.Config
}

type RecvShardsFields struct {
	KVMap     map[string]string
	ClientMap map[int64]clientData
	Shards    []int
}

type PushShardsDoneFields struct {
	ConfigId int
}

type Op struct {
	Type int

	Cmd interface{}
}

// message from apply goroutine to RPC handler
type returnData struct {
	err       Err
	value     string
	requestId int
}

type clientData struct {
	RequestId int    // last request ID
	Value     string // return value of Get
}

type PushShardsArgs struct {
	Shards    []int
	KVMap     map[string]string
	ClientMap map[int64]clientData
	ConfigId  int
}

type PushShardsReply struct {
	Err Err
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	dead     int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	lastApplied  int // for snapshot

	// Your definitions here.
	mck        *shardctrler.Clerk // clerk of shardctrler
	config     shardctrler.Config // current config
	lastConfig shardctrler.Config // last config

	kvMap     map[string]string    // store kv pairs
	clientMap map[int64]clientData // store last operation for each client

	chanMap map[int64]chan returnData // store channel for each client

	shardState [shardctrler.NShards]ShardState // state of each shard
}

func (kv *ShardKV) PushShards(args *PushShardsArgs, reply *PushShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	/* Incoming config is too old, ignore */
	if args.ConfigId < kv.config.Num {
		reply.Err = ErrWrongConfig
		return
	}

	/* Incoming config is too new, wait for local config to catch up */
	if args.ConfigId > kv.config.Num {
		reply.Err = ErrWrongRetry
		return
	}

	kv.rf.Start(Op{
		Type: RecvShardsOp,
		Cmd: RecvShardsFields{
			KVMap:     args.KVMap,
			ClientMap: args.ClientMap,
			Shards:    args.Shards,
		},
	})

	reply.Err = OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.shardState[key2shard(args.Key)] != Available {
		reply.Err = ErrWrongRetry
		kv.mu.Unlock()
		return
	}

	clientData, exist := kv.clientMap[args.ClientId]

	if exist && args.RequestId <= clientData.RequestId {
		reply.Err = OK
		reply.Value = clientData.Value
		kv.mu.Unlock()
		return
	}

	Op := Op{
		Type: GetOp,
		Cmd: KVFields{
			Key:       args.Key,
			ClientId:  args.ClientId,
			RequestId: args.RequestId,
		},
	}

	_, _, isLeader := kv.rf.Start(Op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	c := make(chan returnData)
	kv.chanMap[args.ClientId] = c

	kv.mu.Unlock()

	for rdata := range c {
		if rdata.requestId == args.RequestId {
			reply.Err = rdata.err
			reply.Value = rdata.value
			return
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("[Group %v Server %v] config=%v", kv.gid, kv.me, kv.config)
		kv.mu.Unlock()
		return
	}

	if kv.shardState[key2shard(args.Key)] != Available {
		reply.Err = ErrWrongRetry
		kv.mu.Unlock()
		return
	}

	clientData, exist := kv.clientMap[args.ClientId]

	if exist && args.RequestId <= clientData.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{
		Type: PutOp,
		Cmd: KVFields{
			Key:       args.Key,
			Value:     args.Value,
			ClientId:  args.ClientId,
			RequestId: args.RequestId,
		},
	}
	if args.Op == "Append" {
		op.Type = AppendOp
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	c := make(chan returnData)
	kv.chanMap[args.ClientId] = c

	kv.mu.Unlock()

	for rdata := range c {
		if rdata.requestId == args.RequestId {
			reply.Err = rdata.err
			DPrintf("[Group %v Server %v] PutAppend %v => %v", kv.gid, kv.me, args, reply)
			return
		}
	}
}

func (kv *ShardKV) getMovingShards() (map[int][]int, []int) {
	/* map: gid => shards */
	pushing := make(map[int][]int)
	/* list: shards */
	pulling := make([]int, 0)

	for i := 0; i < shardctrler.NShards; i++ {
		if kv.lastConfig.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
			pushing[kv.config.Shards[i]] = append(pushing[kv.config.Shards[i]], i)
		} else if kv.lastConfig.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid {
			pulling = append(pulling, i)
		}
	}

	return pushing, pulling
}

func (kv *ShardKV) applyReconfOp(op Op, idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reconfFields := op.Cmd.(ReconfFields)
	newConfig := reconfFields.Config

	kv.lastApplied = idx

	/* only continuous config update is allowed */
	if newConfig.Num != kv.config.Num+1 {
		return
	}

	if newConfig.Num == 1 {
		/* first config */
		kv.lastConfig = newConfig
		kv.config = newConfig

		for i := 0; i < shardctrler.NShards; i++ {
			if kv.config.Shards[i] == kv.gid {
				kv.shardState[i] = Available
			}
		}
	} else {
		kv.lastConfig = kv.config
		kv.config = newConfig

		pushing, pulling := kv.getMovingShards()

		/* alter the state of shards influenced by the config change */
		for _, shard := range pulling {
			kv.shardState[shard] = Pulling
		}

		for _, shards := range pushing {
			for _, shard := range shards {
				kv.shardState[shard] = Pushing
			}
		}

		DPrintf("[Group %v Server %v] oldconf=%v newconf=%v states=%v", kv.gid, kv.me, kv.lastConfig, kv.config, kv.shardState)
	}
}

func (kv *ShardKV) applyRecvShardsOp(op Op, idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[Group %v Server %v] [applyRecvShardsOp] op=%v", kv.gid, kv.me, op)

	kv.lastApplied = idx

	recvShardsFields := op.Cmd.(RecvShardsFields)
	kvMap := recvShardsFields.KVMap
	clientMap := recvShardsFields.ClientMap
	shards := make([]int, 0)

	for _, shard := range recvShardsFields.Shards {
		if kv.shardState[shard] == Pulling {
			kv.shardState[shard] = Available
			shards = append(shards, shard)
		} else {
			/* The shard is already available, so we don't need to apply the op */
			/* This may because Pushing side experience a leader change and both old and new leader sent the shard */
			DPrintf("[Group %v Server %v] [applyRecvShardsOp] shard=%v state=%v", kv.gid, kv.me, shard, kv.shardState[shard])
		}
	}

	/* update the kvMap for all Pulling shards */
	for k, v := range kvMap {
		for _, e := range shards {
			if e == key2shard(k) {
				kv.kvMap[k] = v
				break
			}
		}
	}

	/* update the clientMap for request deduplication */
	for k, v := range clientMap {
		data, exist := kv.clientMap[k]

		if !exist || data.RequestId < v.RequestId {
			kv.clientMap[k] = v
		}
	}
}

func (kv *ShardKV) applyPushShardsDoneOp(op Op, idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	pushShardsDoneFields := op.Cmd.(PushShardsDoneFields)
	configId := pushShardsDoneFields.ConfigId

	kv.lastApplied = idx

	if configId != kv.config.Num {
		return
	}

	for i := 0; i < shardctrler.NShards; i++ {
		if kv.shardState[i] == Pushing {
			kv.shardState[i] = Unavailable
		}
	}
}

func (kv *ShardKV) applyNullOp(op Op, idx int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastApplied = idx
}

func (kv *ShardKV) applyKVOp(op Op, idx int) {
	kv.mu.Lock()

	kv.lastApplied = idx

	kvfields := op.Cmd.(KVFields)

	clientId := kvfields.ClientId
	requestId := kvfields.RequestId

	/* request deduplication */
	if kv.clientMap[clientId].RequestId >= requestId {
		kv.mu.Unlock()
		return
	}

	newClientData := clientData{
		RequestId: requestId,
	}

	status := OK

	key := kvfields.Key
	value := kvfields.Value

	if kv.config.Shards[key2shard(key)] != kv.gid {
		/* ignore Op to wrong group */
		status = ErrWrongGroup
	} else {
		if op.Type == GetOp {
			var exist bool
			newClientData.Value, exist = kv.kvMap[key]
			if !exist {
				status = ErrNoKey
			}
		} else if op.Type == PutOp {
			kv.kvMap[key] = value
		} else if op.Type == AppendOp {
			kv.kvMap[key] += value
		} else {
			panic("invalid op type")
		}

		kv.clientMap[clientId] = newClientData
	}

	DPrintf("[Group %v Server %v] Op=%v key=%v value=%v newClientData=%v", kv.gid, kv.me, op.Type, key, value, newClientData)

	c, ok := kv.chanMap[clientId]
	if !ok {
		kv.mu.Unlock()
		return
	}
	delete(kv.chanMap, clientId)

	kv.mu.Unlock()

	c <- returnData{
		err:       Err(status),
		value:     newClientData.Value,
		requestId: requestId,
	}
}

func (kv *ShardKV) applyOp() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)
		idx := msg.CommandIndex

		if op.Type == ReconfOp {
			kv.applyReconfOp(op, idx)
		} else if op.Type == RecvShardsOp {
			kv.applyRecvShardsOp(op, idx)
		} else if op.Type == PushShardsDoneOp {
			kv.applyPushShardsDoneOp(op, idx)
		} else if op.Type == NullOp {
			kv.applyNullOp(op, idx)
		} else {
			kv.applyKVOp(op, idx)
		}
	}
}

func (kv *ShardKV) takeSnapshot() {
	for kv.maxraftstate != -1 && !kv.killed() {
		kv.mu.Lock()
		/* raft state size is close to the threshold */
		if kv.persister.RaftStateSize() > kv.maxraftstate/2 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvMap)
			e.Encode(kv.clientMap)
			e.Encode(kv.lastConfig)
			e.Encode(kv.config)
			e.Encode(kv.shardState)
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, data)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var clientMap map[int64]clientData
	var lastConfig shardctrler.Config
	var config shardctrler.Config
	var shardState [shardctrler.NShards]ShardState

	if d.Decode(&kvMap) != nil ||
		d.Decode(&clientMap) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&shardState) != nil {
		panic("readSnapshot error")
	} else {
		kv.kvMap = kvMap
		kv.clientMap = clientMap
		kv.lastConfig = lastConfig
		kv.config = config
		kv.shardState = shardState
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVFields{})
	labgob.Register(ReconfFields{})
	labgob.Register(RecvShardsFields{})
	labgob.Register(PushShardsDoneFields{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvMap = make(map[string]string)
	kv.clientMap = make(map[int64]clientData)
	kv.chanMap = make(map[int64]chan returnData)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardState[i] = Unavailable
	}

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.tryUpdateConfig()
	go kv.applyOp()
	go kv.pushShards()
	go kv.cleanShards()
	go kv.takeSnapshot()
	go kv.submitEmptyLog()

	return kv
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) tryUpdateConfig() {
	for {
		time.Sleep(100 * time.Millisecond)

		if kv.killed() {
			return
		}

		if !kv.isLeader() {
			continue
		}

		kv.mu.Lock()

		/* only update config if no shards in Pushing/Pulling */
		updateable := true
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardState[i] == Pushing || kv.shardState[i] == Pulling {
				updateable = false
			}
		}
		if !updateable {
			kv.mu.Unlock()
			continue
		}

		newConfig := kv.mck.Query(kv.config.Num + 1)

		if newConfig.Num == kv.config.Num+1 {
			op := Op{
				Type: ReconfOp,
				Cmd: ReconfFields{
					Config: newConfig,
				},
			}
			kv.rf.Start(op)
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) sendPushShards(servers []string, index int, args *PushShardsArgs, reply *PushShardsReply) bool {
	result := make(chan bool)

	go func() {
		srv := kv.make_end(servers[index])
		ok := srv.Call("ShardKV.PushShards", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *ShardKV) pushShards() {
	for {
		time.Sleep(100 * time.Millisecond)

		if kv.killed() {
			return
		}

		if !kv.isLeader() {
			continue
		}
		kv.mu.Lock()

		var wg sync.WaitGroup
		pushing, _ := kv.getMovingShards()

		copyKVMap := make(map[string]string)
		for k, v := range kv.kvMap {
			copyKVMap[k] = v
		}

		copyClientMap := make(map[int64]clientData)
		for k, v := range kv.clientMap {
			copyClientMap[k] = v
		}

		DPrintf("[Group %v Server %v] ConfigId=%v states=%v", kv.gid, kv.me, kv.config.Num, kv.shardState)

		/* for any group needs to be pushed */
		for gid, shards := range pushing {
			/* find shards that should be sent to the group and in Pushing state */
			pushingShards := make([]int, 0)
			for _, shard := range shards {
				if kv.shardState[shard] == Pushing {
					pushingShards = append(pushingShards, shard)
				}
			}

			if len(pushingShards) == 0 {
				continue
			}

			wg.Add(1)

			/* async send shards to the group */
			go func(gid int, shards []int, kvMap map[string]string, clientMap map[int64]clientData) {
				defer wg.Done()

				args := PushShardsArgs{
					Shards:    shards,
					KVMap:     kvMap,
					ClientMap: clientMap,
					ConfigId:  kv.config.Num,
				}

				var reply PushShardsReply

				svrs := kv.config.Groups[gid]
				for i := 0; ; {
					if !kv.isLeader() || kv.killed() {
						return
					}
					DPrintf("[Group %v Server %v] PushShards %v => [Group %v Server %v]", kv.gid, kv.me, args, gid, i)
					ok := kv.sendPushShards(svrs, i, &args, &reply)
					DPrintf("[Group %v Server %v] <= [Group %v Server %v] ok=%v reply=%v", kv.gid, kv.me, gid, i, ok, reply)
					if ok {
						if reply.Err == OK {
							break
						} else if reply.Err == ErrWrongConfig {
							/* this config is too old, stop sending */
							break
						} else if reply.Err == ErrWrongRetry {
							/* this config is too new for the receiver, wait for retry */
							time.Sleep(100 * time.Millisecond)
						}
					}
					i = (i + 1) % len(svrs)
				}

			}(gid, pushingShards, copyKVMap, copyClientMap)

			for _, shard := range pushingShards {
				kv.shardState[shard] = Unavailable
			}
		}

		wg.Wait()

		kv.rf.Start(Op{
			Type: PushShardsDoneOp,
			Cmd: PushShardsDoneFields{
				ConfigId: kv.config.Num,
			},
		})

		kv.mu.Unlock()
	}
}

/* for any shard in Unavailable state, (typically after pushing shards to other groups)
 * clean all kv pairs in that shard */
func (kv *ShardKV) cleanShards() {
	for {
		time.Sleep(100 * time.Millisecond)

		if kv.killed() {
			return
		}

		kv.mu.Lock()

		for i := 0; i < shardctrler.NShards; i++ {
			if kv.shardState[i] == Unavailable {
				for k := range kv.kvMap {
					if key2shard(k) == i {
						delete(kv.kvMap, k)
					}
				}
			}
		}

		kv.mu.Unlock()
	}
}

/* empty raft log to prevent livelock */
func (kv *ShardKV) submitEmptyLog() {
	for {
		time.Sleep(100 * time.Millisecond)

		if kv.killed() {
			return
		}

		if !kv.isLeader() {
			continue
		}

		if kv.rf.HaveLogInCurTerm() {
			kv.rf.Start(Op{
				Type: NullOp,
			})
		}
	}
}
