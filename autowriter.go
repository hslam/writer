package autowriter

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const maximumSegmentSize = 65536
const lastsSize = 4

type AutoWriter struct {
	mu         *sync.Mutex
	writer     io.Writer
	noDelay    bool
	mss        int
	buffer     []byte
	size       int
	count      int
	writeCnt   int
	batch      Batch
	thresh     int
	lasts      [lastsSize]int
	cursor     int
	triggerCnt int64
	trigger    chan bool
	done       chan bool
	closed     int32
}
type Batch interface {
	Concurrency() int
}

func NewAutoWriter(writer io.Writer, noDelay bool, maxBytes int, thresh int, batch Batch) io.WriteCloser {
	if maxBytes < 1 {
		maxBytes = maximumSegmentSize
	}
	if thresh < 2 {
		thresh = 2
	}
	w := &AutoWriter{writer: writer, noDelay: noDelay}
	if !noDelay && batch != nil {
		w.mu = &sync.Mutex{}
		w.thresh = thresh
		w.mss = maxBytes
		w.buffer = make([]byte, maxBytes)
		w.batch = batch
		w.trigger = make(chan bool, thresh*thresh*4)
		w.done = make(chan bool, 1)
		go w.run()
	}
	return w
}
func (w *AutoWriter) concurrency() (n int) {
	w.cursor += 1
	w.lasts[w.cursor%lastsSize] = w.batch.Concurrency()
	var max int
	for i := 0; i < lastsSize; i++ {
		if w.lasts[i] > max {
			max = w.lasts[i]
		}
	}
	return max
}
func (w *AutoWriter) Write(p []byte) (n int, err error) {
	if w.noDelay || w.batch == nil {
		return w.writer.Write(p)
	}
	concurrency := w.concurrency()
	length := len(p)
	w.mu.Lock()
	w.writeCnt += 1
	if w.size+length > w.mss {
		if w.size > 0 {
			w.writer.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		}
		if length > 0 {
			w.writer.Write(p)
		}
	} else if concurrency <= w.thresh {
		if w.size > 0 {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.writer.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
		} else {
			w.writer.Write(p)
			w.size = 0
			w.count = 0
		}
	} else if concurrency <= w.thresh*w.thresh {
		if w.writeCnt < w.thresh {
			if w.size > 0 {
				copy(w.buffer[w.size:], p)
				w.size += length
				w.writer.Write(w.buffer[:w.size])
				w.size = 0
				w.count = 0
			} else {
				w.writer.Write(p)
				w.size = 0
				w.count = 0
			}
		} else {
			copy(w.buffer[w.size:], p)
			w.size += length
			w.count += 1
			if w.count > concurrency-w.thresh {
				w.writer.Write(w.buffer[:w.size])
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
					w.writer.Write(w.buffer[:w.size])
					w.size = 0
					w.count = 0
				} else {
					w.writer.Write(p)
					w.size = 0
					w.count = 0
				}
			} else {
				copy(w.buffer[w.size:], p)
				w.size += length
				w.count += 1
				if w.count > concurrency-alpha {
					w.writer.Write(w.buffer[:w.size])
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
				w.writer.Write(w.buffer[:w.size])
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
			w.writer.Write(w.buffer[:w.size])
			w.size = 0
			w.count = 0
			w.writeCnt = 0
		}
		w.mu.Unlock()
		var d time.Duration
		if w.concurrency() < w.thresh*2 {
			d = time.Second
		} else {
			d = time.Microsecond * 100
		}
		select {
		case <-time.After(d):
		case <-w.trigger:
			time.Sleep(time.Microsecond * time.Duration(w.concurrency()))
			atomic.AddInt64(&w.triggerCnt, -1)
		case <-w.done:
			return
		}
	}
}

func (w *AutoWriter) Close() error {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	if !w.noDelay && w.done != nil {
		close(w.done)
	}
	if !w.noDelay && w.trigger != nil {
		close(w.trigger)
	}
	return nil
}
