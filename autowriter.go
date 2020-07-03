package autowriter

import (
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var numCPU = runtime.NumCPU()

type AutoWriter struct {
	mu          *sync.Mutex
	conn        io.Writer
	noDelay     bool
	maxBytes    int
	buffer      []byte
	size        int
	count       int
	concurrency Concurrency
	alpha       int
	triggerCnt  int64
	trigger     chan bool
	done        chan bool
}
type Concurrency interface {
	NumConcurrency() int
}

func NewAutoWriter(Conn io.Writer, noDelay bool, maxBytes int, alpha int, concurrency Concurrency) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = 65536 //65536
	}
	if alpha < 1 {
		alpha = 1
	}
	w := &AutoWriter{conn: Conn, noDelay: noDelay, alpha: alpha}
	if !noDelay {
		w.mu = &sync.Mutex{}
		w.maxBytes = maxBytes
		w.buffer = make([]byte, maxBytes)
		w.concurrency = concurrency
		w.trigger = make(chan bool, numCPU*w.alpha*4)
		w.done = make(chan bool, 1)
		go w.run()
	}
	return w
}
func (w *AutoWriter) Write(p []byte) (n int, err error) {
	if w.noDelay {
		return w.conn.Write(p)
	}
	concurrency := w.concurrency.NumConcurrency()
	length := len(p)
	w.mu.Lock()
	if concurrency < numCPU*w.alpha || w.count > concurrency/2 || w.size+length > w.maxBytes {
		if w.size > 0 && w.size+length < w.maxBytes {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		} else {
			if w.size > 0 {
				w.conn.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
			}
			if length > 0 {
				w.conn.Write(p)
			}
		}
	} else {
		copy(w.buffer[w.size:], p)
		w.size += length
		w.count += 1
		if w.count > concurrency/2 && atomic.LoadInt64(&w.triggerCnt) < 1 {
			w.trigger <- true
			atomic.AddInt64(&w.triggerCnt, 1)
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
		}
		w.mu.Unlock()
		s := time.Duration((float64(numCPU*w.alpha-w.concurrency.NumConcurrency()) * 256 / float64(numCPU*w.alpha)))
		if s < 3 {
			s = 3
		}
		select {
		case <-time.After(time.Microsecond * s * s * 16):
		case <-w.trigger:
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
	return nil
}
