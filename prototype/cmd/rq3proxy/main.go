package main

// RQ3 fault-injection proxy: intercepts management-node -> role-node HTTP traffic.
// Usage: rq3proxy <listen_port> <target_port> <mode> [arg]
//   delay <ms>     per-request delay before forwarding
//   blackhole      queue requests without forwarding; forward in order on resume
// Control: write "resume" or "delay <ms>" to /tmp/rq3_proxy_<port>.cmd
import (
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	mu      sync.Mutex
	delayMs int
	blacked bool
	pendQ   []chan struct{}
)

func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
	if len(os.Args) < 4 {
		log.Fatal("usage: rq3proxy <listen> <target> <delay|blackhole> [ms]")
	}
	port := os.Args[1]
	target := os.Args[2]
	mode := os.Args[3]
	if mode == "blackhole" {
		blacked = true
	} else if len(os.Args) > 4 {
		delayMs, _ = strconv.Atoi(os.Args[4])
	}
	u, _ := url.Parse("http://127.0.0.1:" + target)
	rp := httputil.NewSingleHostReverseProxy(u)
	cmdFile := "/tmp/rq3_proxy_" + port + ".cmd"
	os.Remove(cmdFile)
	go func() {
		last := ""
		for {
			if b, err := os.ReadFile(cmdFile); err == nil {
				c := strings.TrimSpace(string(b))
				if c != "" && c != last {
					last = c
					mu.Lock()
					if c == "resume" {
						blacked = false
						waiters := pendQ
						pendQ = nil
						log.Printf("RESUME releasing %d queued", len(waiters))
						for _, w := range waiters {
							close(w)
						}
					} else if strings.HasPrefix(c, "delay ") {
						delayMs, _ = strconv.Atoi(strings.TrimSpace(c[6:]))
						log.Printf("DELAY=%d", delayMs)
					}
					mu.Unlock()
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	log.Printf("START listen=%s target=%s mode=%s delay=%d", port, target, mode, delayMs)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		blk := blacked
		d := delayMs
		var myTurn chan struct{}
		if blk {
			myTurn = make(chan struct{})
			pendQ = append(pendQ, myTurn)
			log.Printf("BLACKHOLE queued (%d pending)", len(pendQ))
		}
		mu.Unlock()
		if blk {
			<-myTurn
		}
		if d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		log.Printf("FWD %s bytes=%d delay=%d", r.URL.Path, len(body), d)
		rp.ServeHTTP(w, r)
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:"+port, nil))
}
