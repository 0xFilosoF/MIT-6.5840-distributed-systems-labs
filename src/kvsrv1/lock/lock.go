package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const lockKey = "lock:"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	uniqueID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	uniqueID := kvtest.RandValue(8)
	lk := &Lock{ck: ck, lockname: lockname, uniqueID: uniqueID}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	for {
		val, ver, err := lk.ck.Get(lockKey + lk.lockname)
		if val == lk.uniqueID {
			return
		}

		if err == rpc.ErrNoKey || val == "" {
			if err = lk.ck.Put(lockKey+lk.lockname, lk.uniqueID, ver); err == rpc.OK {
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here

	for {
		val, ver, err := lk.ck.Get(lockKey + lk.lockname)

		if err == rpc.ErrNoKey || val == "" || val != lk.uniqueID {
			return
		}

		if err = lk.ck.Put(lockKey+lk.lockname, "", ver); err == rpc.OK {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}
