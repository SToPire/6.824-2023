package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOp    = 0
	PutOp    = 1
	AppendOp = 2
)

type Op struct {
	Type      int
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

// message from apply goroutine to RPC handler
type returnData struct {
	err       Err
	value     string
	requestId int
}

// metadata for each client
type clientData struct {
	RequestId int    // last request ID
	Value     string // return value of Get
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	lastApplied  int // for snapshot

	kvMap     map[string]string    // store kv pairs
	clientMap map[int64]clientData // store last operation for each client

	chanMap map[int64]chan returnData // store channel for each client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	// the request has been served before, fetch the previous result
	if args.RequestId <= kv.clientMap[args.ClientId].RequestId {
		reply.Err = OK
		reply.Value = kv.clientMap[args.ClientId].Value
		kv.mu.Unlock()
		return
	}

	op := Op{
		Type:      GetOp,
		Key:       args.Key,
		RequestId: args.RequestId,
		ClientId:  args.ClientId,
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// prepare channel for this request
	c := make(chan returnData)
	kv.chanMap[args.ClientId] = c

	kv.mu.Unlock()

	// wait until the request is applied
	for rdata := range c {
		// make sure the request is the one we are waiting for
		if rdata.requestId == args.RequestId {
			reply.Err = rdata.err
			reply.Value = rdata.value
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	// the request has been served before
	if args.RequestId <= kv.clientMap[args.ClientId].RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{
		Type:      PutOp,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
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

	// prepare channel for this request
	c := make(chan returnData)
	kv.chanMap[args.ClientId] = c

	kv.mu.Unlock()

	// wait until the request is applied
	for rdata := range c {
		// make sure the request is the one we are waiting for
		if rdata.requestId == args.RequestId {
			reply.Err = OK
			return
		}
	}
}

func (kv *KVServer) applyOp() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		/* Underlying raft receives a snapshot from other servers. */
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

		clientId := op.ClientId
		requestId := op.RequestId

		kv.mu.Lock()

		// A request could be duplicated in log, so we need to check if it has been applied before
		if kv.clientMap[clientId].RequestId >= requestId {
			kv.mu.Unlock()
			continue
		}
		DPrintf("[Server %d] apply op %+v", kv.me, op)

		newClientData := clientData{
			RequestId: requestId,
		}

		status := OK

		// update local state machine, including kvMap and clientMap
		if op.Type == GetOp {
			var exist bool
			newClientData.Value, exist = kv.kvMap[op.Key]
			if !exist {
				status = ErrNoKey
			} else {
				DPrintf("[Server %d] get %s = %s", kv.me, op.Key, newClientData.Value)
			}
		} else if op.Type == PutOp {
			kv.kvMap[op.Key] = op.Value
		} else if op.Type == AppendOp {
			kv.kvMap[op.Key] += op.Value
		} else {
			panic("invalid op type")
		}

		kv.lastApplied = msg.CommandIndex

		kv.clientMap[clientId] = newClientData

		// if some RPC handler is waiting for this request, send the result to it
		c, ok := kv.chanMap[clientId]
		if !ok {
			kv.mu.Unlock()
			continue
		}

		// prevent sending to a outdated channel
		delete(kv.chanMap, clientId)
		kv.mu.Unlock()

		c <- returnData{
			err:       Err(status),
			value:     newClientData.Value,
			requestId: requestId,
		}
	}

}

/* background goroutine to take snapshot periodically */
func (kv *KVServer) takeSnapshot() {
	for kv.maxraftstate != -1 && !kv.killed() {
		kv.mu.Lock()
		/* raft state size is close to the threshold */
		if kv.persister.RaftStateSize() > kv.maxraftstate/2 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvMap)
			e.Encode(kv.clientMap)
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, data)
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvMap map[string]string
	var clientMap map[int64]clientData

	if d.Decode(&kvMap) != nil ||
		d.Decode(&clientMap) != nil {
		panic("failed to read snapshot")
	} else {
		kv.kvMap = kvMap
		kv.clientMap = clientMap
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.clientMap = make(map[int64]clientData)
	kv.chanMap = make(map[int64]chan returnData)

	// initialize from state persisted before a crash
	kv.readSnapshot(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applyOp()
	go kv.takeSnapshot()

	return kv
}
