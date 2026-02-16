package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type data struct {
	version rpc.Tversion
	value   string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv map[string]data
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kv: make(map[string]data),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.kv[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = v.value
	reply.Version = v.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.kv[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.kv[args.Key] = data{version: 1, value: args.Value}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	if args.Version != v.version {
		reply.Err = rpc.ErrVersion
		return
	}

	v.value = args.Value
	v.version++
	kv.kv[args.Key] = v

	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(
	tc *tester.TesterClnt,
	ends []*labrpc.ClientEnd,
	gid tester.Tgid,
	srv int,
	persister *tester.Persister,
) []any {
	kv := MakeKVServer()
	return []any{kv}
}
