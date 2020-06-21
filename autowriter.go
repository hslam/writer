package autowriter

import (
	"io"
	"runtime"
	"sync"
	"time"
)

var numCPU = runtime.NumCPU()

type AutoWriter struct {
	mu          *sync.Mutex
	conn        io.Writer
	buffer      []byte
	size        int
	maxBytes    int
	count       int
	concurrency Concurrency
	done        chan struct{}
}
type Concurrency interface {
	Concurrency() int
}

func NewAutoWriter(Conn io.Writer, maxBytes int, concurrency Concurrency) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = 65536 //65536
	}
	w := &AutoWriter{mu: &sync.Mutex{}, conn: Conn, buffer: make([]byte, maxBytes), maxBytes: maxBytes, concurrency: concurrency}
	go w.run()
	return w
}
func (w *AutoWriter) Write(p []byte) (n int, err error) {
	length := len(p)
	w.mu.Lock()
	concurrency := w.concurrency.Concurrency()
	if concurrency < numCPU*4+1 || w.count > concurrency/2 || w.size+length > w.maxBytes {
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
	}
	w.mu.Unlock()
	return len(p), nil
}

func (w *AutoWriter) run() {
	w.done = make(chan struct{}, 1)
	for {
		select {
		case <-w.done:
			return
		default:
			w.mu.Lock()
			if w.size > 0 {
				w.conn.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
			}
			w.mu.Unlock()
		}
		s := time.Duration((float64(numCPU*4-w.concurrency.Concurrency()) * 64 / float64(numCPU*4)))
		if s < 2 {
			s = 2
		}
		time.Sleep(time.Microsecond * s * s * 50)
	}
}

func (w *AutoWriter) Close() error {
	close(w.done)
	return nil
}
