package main

import (
	"fmt"
	"github.com/ppal31/zkapps/locker"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
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
	server := &http.Server{Addr: address,
		//shamelessly copied following config from internet, will revisit this later
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ReadHeaderTimeout: 20 * time.Second,
	}
	w := &Worker{wid: rand.Intn(10), server: server, locker: c.GetLocker()}
	log.Printf("Worker %d : %s:%d", w.wid, c.Host, c.Port)
	return w
}

func (w *Worker) Start() error {
	http.HandleFunc("/", w.ping())
	http.HandleFunc("/ping", w.ping())
	http.HandleFunc("/lockAndSleep", w.lockAndSleep())
	return w.server.ListenAndServe()
}

func (w *Worker) ping() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "pong")
	}
}

func (w *Worker) lockAndSleep() func(writer http.ResponseWriter, r *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		lt := r.URL.Query().Get("lt")
		lk := r.URL.Query().Get("lk")
		if lti, err := strconv.Atoi(lt); err == nil && lk != "" {
			ctx := r.Context()
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				http.Error(writer, ctx.Err().Error(), http.StatusInternalServerError)
			case lock := <-w.locker.AcquireLock(ctx, lk):
				log.Printf("Trying to acquire lock for Key %v\n", lk)
				defer w.locker.ReleaseLock(lock)
				log.Printf("TLock Acquired for Key %v Sleeping %v\n", lk, lt)
				time.Sleep(time.Duration(lti) * time.Second)
				fmt.Fprint(writer, fmt.Sprintf("Lock Acquired and release for %v\n", lock.GetLockId()))
			}
		} else {
			panic(err)
		}
	}
}
