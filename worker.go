package main

import (
	"context"
	"fmt"
	"github.com/ppal31/zkapps/locker"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

const (
	V1 int = iota
	V2
)

type Worker struct {
	wid    int
	locker locker.Locker
	server *http.Server
}

//Starts a worker
func Make(c Config) *Worker {
	rand.Seed(time.Now().UnixNano())
	address := fmt.Sprintf("%s:%d", c.Host, c.Port)
	server := &http.Server{Addr: address}
	l, err := c.GetLocker()
	if err != nil {
		log.Fatal(err)
	}
	w := &Worker{wid: rand.Intn(10), server: server, locker: l}
	log.Printf("Worker %d : %s:%d", w.wid, c.Host, c.Port)
	return w
}

func (w *Worker) Start() error {
	http.HandleFunc("/", w.ping())
	http.HandleFunc("/ping", w.ping())
	http.HandleFunc("/lockV1", w.lockV1())
	http.HandleFunc("/lockV2", w.lockV2())
	return w.server.ListenAndServe()
}

func (w *Worker) ping() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "pong")
	}
}

func (w *Worker) lockV1() func(writer http.ResponseWriter, r *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		w.lockWithVersion(writer, r, V1)
	}
}

func (w *Worker) lockV2() func(writer http.ResponseWriter, r *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		w.lockWithVersion(writer, r, V2)
	}
}

func (w *Worker) lockWithVersion(writer http.ResponseWriter, r *http.Request, lockVersion int) {
	lt := r.URL.Query().Get("lt")
	lk := r.URL.Query().Get("lk")
	if lti, err := strconv.Atoi(lt); err == nil && lk != "" {
		log.Printf("Trying to acquire lock for Key %v\n", lk)
		var lock locker.Lock
		if lockVersion == V1 {
			lock = w.locker.AcquireLock(context.Background(), lk)
		} else {
			lock = w.locker.AcquireLockV2(context.Background(), lk)
		}
		defer w.locker.ReleaseLock(lock)
		log.Printf("TLock Acquired for Key %v Sleeping %v\n", lk, lt)
		time.Sleep(time.Duration(lti) * time.Second)
		_, _ = fmt.Fprint(writer, fmt.Sprintf("Lock Acquired and release for %v\n", lock.GetLockId()))
	} else {
		panic(err)
	}
}
