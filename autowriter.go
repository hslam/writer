package autowriter

import (
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var numCPU = runtime.NumCPU()

const maximumSegmentSize = 65536
const lastsSize = 4

type AutoWriter struct {
	mu          *sync.Mutex
	conn        io.Writer
	noDelay     bool
	mss         int
	buffer      []byte
	size        int
	count       int
	writeCnt    int
	concurrency Concurrency
	thresh      int
	lasts       [lastsSize]int
	cursor      int
	triggerCnt  int64
	trigger     chan bool
	done        chan bool
}
type Concurrency interface {
	NumConcurrency() int
}

func NewAutoWriter(Conn io.Writer, noDelay bool, maxBytes int, thresh int, concurrency Concurrency) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = maximumSegmentSize
	}
	if thresh < 2 {
		thresh = 2
	}
	w := &AutoWriter{conn: Conn, noDelay: noDelay}
	if !noDelay {
		w.mu = &sync.Mutex{}
		w.thresh = thresh
		w.mss = maxBytes
		w.buffer = make([]byte, maxBytes)
		w.concurrency = concurrency
		w.trigger = make(chan bool, thresh*thresh*4)
		w.done = make(chan bool, 1)
		go w.run()
	}
	return w
}
func (w *AutoWriter) numConcurrency() (n int) {
	concurrency := w.concurrency.NumConcurrency()
	w.cursor += 1
	w.lasts[w.cursor%lastsSize] = concurrency
	var max int
	for i := 0; i < lastsSize; i++ {
		if w.lasts[i] > max {
			max = w.lasts[i]
		}
	}
	return max
}
func (w *AutoWriter) Write(p []byte) (n int, err error) {
	if w.noDelay {
		return w.conn.Write(p)
	}
	concurrency := w.numConcurrency()
	length := len(p)
	w.mu.Lock()
	w.writeCnt += 1
	if w.size+length > w.mss {
		if w.size > 0 {
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		}
		if length > 0 {
			w.conn.Write(p)
		}
	} else if concurrency <= w.thresh {
		if w.size > 0 {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		} else {
			w.conn.Write(p)
			w.size = 0
			w.count = 0
		}
	} else if concurrency <= w.thresh*w.thresh {
		if w.writeCnt < w.thresh {
			if w.size > 0 {
				copy(w.buffer[w.size:], p)
				w.size += length
				w.conn.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
			} else {
				w.conn.Write(p)
				w.size = 0
				w.count = 0
			}
		} else {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			if w.count > concurrency-w.thresh {
				w.conn.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
				w.writeCnt = 0
			} else if atomic.LoadInt64(&w.triggerCnt) < 1 {
				w.trigger <- true
				atomic.AddInt64(&w.triggerCnt, 1)
			}
		}

	} else {
		alpha := w.thresh*2 - (concurrency-1)/w.thresh
		if alpha > 1 {
			if w.writeCnt < alpha {
				if w.size > 0 {
					copy(w.buffer[w.size:], p)
					w.size += length
					w.conn.Write(w.buffer[:w.size])
					w.size = 0
					w.count = 0
				} else {
					w.conn.Write(p)
					w.size = 0
					w.count = 0
				}
			} else {
				copy(w.buffer[w.size:], p)
				w.size += length
				w.count += 1
				if w.count > concurrency-alpha {
					w.conn.Write(w.buffer[:w.size])
					w.size = 0
					w.count = 0
					w.writeCnt = 0
				} else if atomic.LoadInt64(&w.triggerCnt) < 1 {
					w.trigger <- true
					atomic.AddInt64(&w.triggerCnt, 1)
				}
			}
		} else {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			if w.count > concurrency-1 {
				w.conn.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
				w.writeCnt = 0
			} else if atomic.LoadInt64(&w.triggerCnt) < 1 {
				w.trigger <- true
				atomic.AddInt64(&w.triggerCnt, 1)
			}
		}
	}
	w.mu.Unlock()
	return len(p), nil
}

func (w *AutoWriter) run() {
	for {
		w.mu.Lock()
		if w.size > 0 {
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
			w.writeCnt = 0
		}
		w.mu.Unlock()
		var d time.Duration
		if w.concurrency.NumConcurrency() < w.thresh*2 {
			d = time.Second
		} else {
			d = time.Microsecond * 100
		}
		select {
		case <-time.After(d):
		case <-w.trigger:
			time.Sleep(time.Microsecond * 5)
			atomic.AddInt64(&w.triggerCnt, -1)
		case <-w.done:
			return
		}
	}
}

func (w *AutoWriter) Close() error {
	if !w.noDelay && w.done != nil {
		close(w.done)
	}
	if !w.noDelay && w.trigger != nil {
		close(w.trigger)
	}
	return nil
}
