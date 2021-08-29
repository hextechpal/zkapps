package main

import (
	"github.com/go-zookeeper/zk"
	"github.com/ppal31/zkapps/locker"
)

type Config struct {
	Zkc  *zk.Conn
	Port int
	Host string
	Ns   string
}

func (c Config) GetLocker() locker.Locker {
	return locker.Make(c.Zkc, c.Ns)
}
