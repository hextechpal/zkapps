package locker

import (
	"context"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
)

const LOCK_PATH = "/%v/%v"

type ZkLock struct {
	path string
}

func (zl ZkLock) GetLockId() string {
	return zl.path
}

type ZkLocker struct {
	zkc *zk.Conn
	ns  string
}

func Make(zkc *zk.Conn, ns string) *ZkLocker {
	return &ZkLocker{
		zkc: zkc,
		ns:  ns,
	}
}

func (zkl *ZkLocker) AcquireLock(ctx context.Context, key string) Lock {
	lp := fmt.Sprintf(LOCK_PATH, zkl.ns, key)
	for {
		res, err := zkl.zkc.Create(lp, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err == nil {
			return ZkLock{path: res}
		}
		// This means we failed to create the file we should wait and establish a watch
		present, _, ch, err := zkl.zkc.ExistsW(lp)
		log.Printf("[ExistsFileError] %v\n", err)
		if (!present && err == nil) || err != nil {
			continue
		}
		e := <-ch
		log.Printf("Evet Received %v\n", e)
	}

}

func (zkl *ZkLocker) ReleaseLock(lock Lock) {
	err := zkl.zkc.Delete(lock.GetLockId(), 0)
	if err != nil {
		panic(err)
	}
}