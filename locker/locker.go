package locker

import "context"

type Lock interface {
	GetLockId() string
}

type Locker interface {
	AcquireLock(ctx context.Context, key string) <-chan Lock
	ReleaseLock(lock Lock)
}
