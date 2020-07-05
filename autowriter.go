package autowriter

import (
	"io"
	"runtime"
	"sync"
	"time"
)

var numCPU = runtime.NumCPU()

const maxSize = 4

type AutoWriter struct {
	mu          *sync.Mutex
	conn        io.Writer
	noDelay     bool
	maxBytes    int
	buffer      []byte
	size        int
	count       int
	writeCnt    int
	concurrency Concurrency
	thresh      int
	lasts       [maxSize]int
	cursor      int
	done        chan bool
}
type Concurrency interface {
	NumConcurrency() int
}

func NewAutoWriter(Conn io.Writer, noDelay bool, maxBytes int, thresh int, concurrency Concurrency) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = 65536 //65536
	}
	if thresh < 2 {
		thresh = 2
	}
	w := &AutoWriter{conn: Conn, noDelay: noDelay, thresh: thresh}
	if !noDelay {
		w.mu = &sync.Mutex{}
		w.maxBytes = maxBytes
		w.buffer = make([]byte, maxBytes)
		w.concurrency = concurrency
		w.done = make(chan bool, 1)
		go w.run()
	}
	return w
}
func (w *AutoWriter) numConcurrency() (n int) {
	concurrency := w.concurrency.NumConcurrency()
	w.cursor += 1
	w.lasts[w.cursor%maxSize] = concurrency
	var max int
	for i := 0; i < maxSize; i++ {
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
	if w.size+length > w.maxBytes {
		if w.size > 0 {
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		}
		if length > 0 {
			w.conn.Write(p)
		}
	} else if concurrency <= 4 || w.writeCnt < 4 {
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
		if w.count > concurrency-4 {
			w.conn.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
			w.writeCnt = 0
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
		//s := time.Duration((float64(numCPU*w.alpha-w.concurrency.NumConcurrency()) * 256 / float64(numCPU*w.alpha)))
		//if s < 3 {
		//	s = 3
		//}
		time.Sleep(time.Microsecond * 100)
		select {
		case <-w.done:
			return
		default:
		}
	}
}

func (w *AutoWriter) Close() error {
	if !w.noDelay && w.done != nil {
		close(w.done)
	}
	return nil
}
