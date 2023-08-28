package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	clientId        int64 // unique id of client
	perceivedLeader int   // index of server which clerk thinks is leader
	seqno           int   // sequence number of request
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.clientId = nrand()
	ck.perceivedLeader = 0
	ck.seqno = 0
	return ck
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	result := make(chan bool)
	DPrintf("[Clerk] sendGet: server=%v args=%+v", server, args)

	go func() {
		ok := ck.servers[server].Call("KVServer.Get", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	result := make(chan bool)
	DPrintf("[Clerk] sendPutAppend: server=%v args=%+v", server, args)

	go func() {
		ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	server := ck.perceivedLeader
	DPrintf("[Clerk] Get: key=%v", key)

	ck.seqno++
	for {
		args := GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply GetReply

		ok := ck.sendGet(server, &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.perceivedLeader = server
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("[Clerk] ErrNoKey: %v", key)
				return ""
			} else if reply.Err == ErrWrongLeader {
				DPrintf("[Clerk] WrongLeader: %v", server)
			} else {
				DPrintf("Clerk.Get: unexpected error: %v", reply.Err)
			}
		}
		// try next server
		server = (server + 1) % len(ck.servers)
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	server := ck.perceivedLeader
	DPrintf("[Clerk] PutAppend: op=%v key=%v value=%v", op, key, value)

	ck.seqno++
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply PutAppendReply

		ok := ck.sendPutAppend(server, &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.perceivedLeader = server
				return
			} else if reply.Err == ErrWrongLeader {
				DPrintf("[Clerk] WrongLeader: %v", server)
			} else {
				DPrintf("Clerk.PutAppend: unexpected error: %v", reply.Err)
			}
		}
		// try next server
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
