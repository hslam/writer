package autowriter

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const maximumSegmentSize = 65536
const lastsSize = 4

var (
	buffers = sync.Map{}
	assign  int32
)

func assignPool(size int) *sync.Pool {
	for {
		if p, ok := buffers.Load(size); ok {
			return p.(*sync.Pool)
		}
		if atomic.CompareAndSwapInt32(&assign, 0, 1) {
			var pool = &sync.Pool{New: func() interface{} {
				return make([]byte, size)
			}}
			buffers.Store(size, pool)
			return pool
		}
	}
}

type AutoWriter struct {
	lowMemory   bool
	mu          *sync.Mutex
	writer      io.Writer
	noDelay     bool
	mss         int
	buffer      []byte
	size        int
	count       int
	writeCnt    int
	concurrency func() int
	thresh      int
	lasts       [lastsSize]int
	cursor      int
	trigger     chan struct{}
	done        chan struct{}
	closed      int32
}

func NewAutoWriter(writer io.Writer, noDelay bool, maxBytes int, thresh int, concurrency func() int) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = maximumSegmentSize
	}
	if thresh < 2 {
		thresh = 2
	}
	w := &AutoWriter{writer: writer, noDelay: noDelay}
	if !noDelay && concurrency != nil {
		var lowMemory = false
		var buffer []byte
		if !lowMemory {
			buffer = make([]byte, maxBytes)
		}
		w.lowMemory = lowMemory
		w.mu = &sync.Mutex{}
		w.thresh = thresh
		w.mss = maxBytes
		w.buffer = buffer
		w.concurrency = concurrency
		w.trigger = make(chan struct{}, 10)
		w.done = make(chan struct{}, 1)
		go w.run()
	}
	return w
}

func (w *AutoWriter) batch() (n int) {
	w.cursor += 1
	w.lasts[w.cursor%lastsSize] = w.concurrency()
	var max int
	for i := 0; i < lastsSize; i++ {
		if w.lasts[i] > max {
			max = w.lasts[i]
		}
	}
	return max
}

func (w *AutoWriter) Write(p []byte) (n int, err error) {
	if w.noDelay || w.concurrency == nil {
		return w.writer.Write(p)
	}
	batch := w.batch()
	length := len(p)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeCnt += 1
	if w.size+length > w.mss {
		if w.size > 0 {
			w.writer.Write(w.buffer[:w.size])
			if w.lowMemory {
				assignPool(w.mss).Put(w.buffer)
			}
			w.size = 0
			w.count = 0
		}
		if length > 0 {
			w.writer.Write(p)
		}
	} else if batch <= w.thresh {
		if w.size > 0 {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.writer.Write(w.buffer[:w.size])
			if w.lowMemory {
				assignPool(w.mss).Put(w.buffer)
			}
			w.size = 0
			w.count = 0
		} else {
			w.writer.Write(p)
			w.size = 0
			w.count = 0
		}
	} else if batch <= w.thresh*w.thresh {
		if w.writeCnt < w.thresh {
			if w.size > 0 {
				copy(w.buffer[w.size:], p)
				w.size += length
				w.writer.Write(w.buffer[:w.size])
				if w.lowMemory {
					assignPool(w.mss).Put(w.buffer)
				}
				w.size = 0
				w.count = 0
			} else {
				w.writer.Write(p)
				w.size = 0
				w.count = 0
			}
		} else {
			if w.lowMemory && len(w.buffer) == 0 {
				w.buffer = assignPool(w.mss).Get().([]byte)
			}
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			if w.count > batch-w.thresh {
				w.writer.Write(w.buffer[:w.size])
				if w.lowMemory {
					assignPool(w.mss).Put(w.buffer)
				}
				w.size = 0
				w.count = 0
				w.writeCnt = 0
			}
			select {
			case w.trigger <- struct{}{}:
			default:
			}
		}
	} else {
		alpha := w.thresh*2 - (batch-1)/w.thresh
		if alpha > 1 {
			if w.writeCnt < alpha {
				if w.size > 0 {
					copy(w.buffer[w.size:], p)
					w.size += length
					w.writer.Write(w.buffer[:w.size])
					if w.lowMemory {
						assignPool(w.mss).Put(w.buffer)
					}
					w.size = 0
					w.count = 0
				} else {
					w.writer.Write(p)
					w.size = 0
					w.count = 0
				}
			} else {
				if w.lowMemory && len(w.buffer) == 0 {
					w.buffer = assignPool(w.mss).Get().([]byte)
				}
				copy(w.buffer[w.size:], p)
				w.size += length
				w.count += 1
				if w.count > batch-alpha {
					w.writer.Write(w.buffer[:w.size])
					if w.lowMemory {
						assignPool(w.mss).Put(w.buffer)
					}
					w.size = 0
					w.count = 0
					w.writeCnt = 0
				}
				select {
				case w.trigger <- struct{}{}:
				default:
				}
			}
		} else {
			if w.lowMemory && len(w.buffer) == 0 {
				w.buffer = assignPool(w.mss).Get().([]byte)
			}
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			if w.count > batch-1 {
				w.writer.Write(w.buffer[:w.size])
				if w.lowMemory {
					assignPool(w.mss).Put(w.buffer)
				}
				w.size = 0
				w.count = 0
				w.writeCnt = 0
			}
			select {
			case w.trigger <- struct{}{}:
			default:
			}
		}
	}
	return len(p), nil
}

func (w *AutoWriter) run() {
	for {
		w.mu.Lock()
		if w.size > 0 {
			w.writer.Write(w.buffer[:w.size])
			if w.lowMemory {
				assignPool(w.mss).Put(w.buffer)
			}
			w.size = 0
			w.count = 0
			w.writeCnt = 0
		}
		w.mu.Unlock()
		var d time.Duration
		if w.batch() < w.thresh*2 {
			d = time.Second
		} else {
			d = time.Microsecond * 100
		}
		select {
		case <-time.After(d):
		case <-w.trigger:
			time.Sleep(time.Microsecond * time.Duration(w.batch()))
		case <-w.done:
			return
		}
	}
}

func (w *AutoWriter) Close() error {
	w.mu.Lock()
	if w.size > 0 {
		w.writer.Write(w.buffer[:w.size])
		if w.lowMemory {
			assignPool(w.mss).Put(w.buffer)
		}
		w.size = 0
		w.count = 0
		w.writeCnt = 0
	}
	w.mu.Unlock()
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	if !w.noDelay && w.done != nil {
		close(w.done)
	}
	if !w.noDelay && w.trigger != nil {
		close(w.trigger)
	}
	w.buffer = nil
	return nil
}
