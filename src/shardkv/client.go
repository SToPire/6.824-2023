package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId int64 // unique id of client
	seqno    int   // sequence number of request
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.clientId = nrand()
	ck.seqno = 0
	return ck
}

func (ck *Clerk) sendGet(servers []string, index int, args *GetArgs, reply *GetReply) bool {
	result := make(chan bool)

	go func() {
		srv := ck.make_end(servers[index])
		ok := srv.Call("ShardKV.Get", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) sendPutAppend(servers []string, index int, args *PutAppendArgs, reply *PutAppendReply) bool {
	result := make(chan bool)

	go func() {
		srv := ck.make_end(servers[index])
		ok := srv.Call("ShardKV.PutAppend", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.seqno++

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.seqno,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				var reply GetReply

				ok := ck.sendGet(servers, si, &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqno++

	DPrintf("[Client %d] PutAppend %s %s %s", ck.clientId, key, value, op)

	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.seqno,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				var reply PutAppendReply

				DPrintf("[Client %d] config=%v", ck.clientId, ck.config)
				DPrintf("[Client %d] sendPutAppend %s %s %s => [Group %v Server %v]", ck.clientId, args.Key, args.Value, args.Op, gid, si)
				ok := ck.sendPutAppend(servers, si, &args, &reply)
				DPrintf("[Client %d] [Group %v Server %v] => ok=%v reply=%v", ck.clientId, gid, si, ok, reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
