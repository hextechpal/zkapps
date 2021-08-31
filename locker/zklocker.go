package locker

import (
	"context"
	"fmt"
	"github.com/go-zookeeper/zk"
	"log"
)

const (
	LOCK_PATH    = "/%v/%v"
	LOCK_PATH_V2 = "/%v/%v-"
)

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

func Make(zkc *zk.Conn, ns string) (*ZkLocker, error) {
	z := &ZkLocker{
		zkc: zkc,
		ns:  ns,
	}
	err := z.init()
	if err == nil {
		return z, nil
	}
	return nil, err
}

func (zkl *ZkLocker) init() error {
	path := "/" + zkl.ns
	exists, _, err := zkl.zkc.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	} else {
		_, _ = zkl.zkc.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
		//TODO : Check for file exists error and only return nil in that case
		return nil
	}

}

// AcquireLock with herd effect
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

// AcquireLockV2 without herd effect
func (zkl *ZkLocker) AcquireLockV2(ctx context.Context, key string) Lock {
	bp := "/" + zkl.ns + "/"
	lp := fmt.Sprintf(LOCK_PATH_V2, zkl.ns, key)
	n, _ := zkl.zkc.Create(lp, nil, zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	for {
		c, _, _ := zkl.zkc.Children("/" + zkl.ns)
		if n == bp+c[0] {
			return ZkLock{path: n}
		}

		var p string
		for i := 1; i < len(c); i++ {
			if n == bp+c[i] {
				p = bp + c[i-1]
				break
			}
		}
		if p == "" {
			log.Fatal("Illegal Stage")
		}
		present, _, ch, err := zkl.zkc.ExistsW(p)
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
