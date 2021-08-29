package main

import (
	"github.com/go-zookeeper/zk"
	"log"
	"net"
	"time"
)

func main() {
	ns := "ppal"
	log.Printf("ZkApps Main")
	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if err != nil {
		log.Fatal(err)
	}

	port, _ := GetFreePort()
	w := Make(Config{
		Zkc:  c,
		Port: port,
		Host: "",
		Ns:   ns,
	})
	err = w.Start()

	if err != nil {
		log.Fatal(err)
	}
}

func GetFreePort() (port int, err error) {
	var a *net.TCPAddr
	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener
		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}
	return
}
