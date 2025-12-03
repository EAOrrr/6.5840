package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	version rpc.Tversion
	lockId  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockId = l
	lk.version = 0
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		myValue := kvtest.RandValue(8)
		err := lk.ck.Put(lk.lockId, myValue, lk.version)
		if err == rpc.OK {
			lk.version++
			return
		} else if err == rpc.ErrMaybe {
			value, version, _ := lk.ck.Get(lk.lockId)
			if value == myValue && version == lk.version+1 {
				lk.version++
				return
			}
		}
		// fail to obtain lock
		// sleep?
		for {
			value, version, _ := lk.ck.Get(lk.lockId)
			lk.version = version
			if value == "" { // someone release lock
				break
			}
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.lockId, "", lk.version)
}
