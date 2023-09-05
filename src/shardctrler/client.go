package shardctrler

//
// Shardctrler clerk.
//

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
	ck.seqno = 0
	return ck
}

func (ck *Clerk) sendQuery(server int, args *QueryArgs, reply *QueryReply) bool {
	result := make(chan bool)

	go func() {
		ok := ck.servers[server].Call("ShardCtrler.Query", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) sendJoin(server int, args *JoinArgs, reply *JoinReply) bool {
	result := make(chan bool)

	go func() {
		ok := ck.servers[server].Call("ShardCtrler.Join", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) sendLeave(server int, args *LeaveArgs, reply *LeaveReply) bool {
	result := make(chan bool)

	go func() {
		ok := ck.servers[server].Call("ShardCtrler.Leave", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) sendMove(server int, args *MoveArgs, reply *MoveReply) bool {
	result := make(chan bool)

	go func() {
		ok := ck.servers[server].Call("ShardCtrler.Move", args, reply)
		result <- ok
	}()

	select {
	case ok := <-result:
		return ok
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) Query(num int) Config {
	server := ck.perceivedLeader

	ck.seqno++
	for {
		args := QueryArgs{
			Num:       num,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply QueryReply

		ok := ck.sendQuery(server, &args, &reply)
		if ok && !reply.WrongLeader {
			ck.perceivedLeader = server
			return reply.Config
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	server := ck.perceivedLeader

	ck.seqno++
	for {
		args := JoinArgs{
			Servers:   servers,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply JoinReply

		ok := ck.sendJoin(server, &args, &reply)
		if ok && !reply.WrongLeader {
			ck.perceivedLeader = server
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Leave(gids []int) {
	server := ck.perceivedLeader

	ck.seqno++
	for {
		args := LeaveArgs{
			GIDs:      gids,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply LeaveReply

		ok := ck.sendLeave(server, &args, &reply)
		if ok && !reply.WrongLeader {
			ck.perceivedLeader = server
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	server := ck.perceivedLeader

	ck.seqno++
	for {
		args := MoveArgs{
			Shard:     shard,
			GID:       gid,
			ClientId:  ck.clientId,
			RequestId: ck.seqno,
		}
		var reply MoveReply

		ok := ck.sendMove(server, &args, &reply)
		if ok && !reply.WrongLeader {
			ck.perceivedLeader = server
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}
