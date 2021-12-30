// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package writer implements batch writing.
package writer

import (
	"errors"
	"github.com/hslam/buffer"
	"github.com/hslam/scheduler"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	thresh             = 1
	maximumSegmentSize = 65536
	lastsSize          = 4
	maxDelay           = time.Microsecond * 40
)

// ErrWriterClosed is returned by the Writer's Write methods after a call to Close.
var ErrWriterClosed = errors.New("Writer closed")

// Flusher is the interface that wraps the basic Flush method.
//
// Flush writes any buffered data to the underlying io.Writer.
type Flusher interface {
	Flush() (err error)
}

// Writer implements batch writing for an io.Writer object.
type Writer struct {
	lock        sync.Mutex
	wLock       sync.Mutex
	cond        sync.Cond
	sched       scheduler.Scheduler
	writer      io.Writer
	scheduling  bool
	concurrency func() int
	lasts       [lastsSize]int
	cursor      int
	mss         int
	pool        *buffer.Pool
	buffer      []byte
	buffers     [][]byte
	size        int
	length      int
	writing     uint64
	closed      int32
	done        int32
}

// NewWriter returns a new batch Writer with the concurrency.
func NewWriter(writer io.Writer, concurrency func() int, size int, scheduling bool) *Writer {
	if size < 1 {
		size = maximumSegmentSize
	}
	var pool = buffer.AssignPool(size)
	var buffer []byte
	if !scheduling {
		buffer = pool.GetBuffer(size)
	}
	w := &Writer{
		writer:      writer,
		concurrency: concurrency,
		scheduling:  scheduling,
		mss:         size,
		pool:        pool,
		buffer:      buffer,
	}
	if w.scheduling {
		w.sched = scheduler.New(1, &scheduler.Options{Threshold: 2})
	} else {
		w.cond.L = &w.lock
		go w.run()
	}
	return w
}

func (w *Writer) batch() (n int) {
	w.cursor++
	w.lasts[w.cursor%lastsSize] = w.concurrency()
	var max int
	for i := 0; i < lastsSize; i++ {
		if w.lasts[i] > max {
			max = w.lasts[i]
		}
	}
	return max
}

func (w *Writer) delay() {
	if w.concurrency != nil {
		batch := w.batch()
		var d time.Duration
		if batch < 128 {
			d = time.Duration(batch * batch / 64)
		} else {
			d = time.Nanosecond * 2 * time.Duration(batch)
		}
		if d > maxDelay {
			d = maxDelay
		}
		if d > 0 {
			time.Sleep(d)
		}
	}
}

// Write writes the contents of p into the buffer or the underlying io.Writer.
// It returns the number of bytes written.
func (w *Writer) Write(p []byte) (n int, err error) {
	if atomic.LoadInt32(&w.closed) > 0 {
		return 0, ErrWriterClosed
	}
	direct := false
	if w.concurrency != nil {
		if w.batch() <= thresh {
			direct = true
		}
	}
	w.lock.Lock()
	if direct {
		var c [][]byte
		var l int
		if w.cached() {
			c, l = w.cache()
		}
		w.wLock.Lock()
		w.lock.Unlock()
		w.flush(p, c, l)
		w.wLock.Unlock()
	} else {
		w.append(p)
		w.lock.Unlock()
		if w.scheduling {
			writing := atomic.AddUint64(&w.writing, 1)
			w.sched.Schedule(func() {
				if atomic.LoadUint64(&w.writing) == writing {
					w.Flush()
				}
			})
		} else {
			w.cond.Signal()
		}
	}
	if err == nil {
		n = len(p)
	}
	return n, err
}

func (w *Writer) append(p []byte) {
	length := len(p)
	w.checkBuffer()
	retain := length
	for retain > w.mss {
		n := copy(w.buffer[w.size:w.mss], p[length-retain:])
		w.size = w.mss
		w.push()
		w.checkBuffer()
		retain -= n
	}
	if retain > 0 {
		if w.size+retain > w.mss {
			w.push()
			w.checkBuffer()
		}
		copy(w.buffer[w.size:], p[length-retain:])
		w.size += retain
	}
}

func (w *Writer) push() {
	buffer := w.buffer[:w.size]
	w.buffers = append(w.buffers, buffer)
	w.length += w.size
	w.buffer = nil
	w.size = 0
}

func (w *Writer) checkBuffer() {
	if cap(w.buffer) == 0 {
		w.buffer = w.pool.GetBuffer(w.mss)
	}
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	w.lock.Lock()
	var c [][]byte
	var l int
	if w.cached() {
		c, l = w.cache()
	}
	w.wLock.Lock()
	w.lock.Unlock()
	err := w.flush(nil, c, l)
	w.wLock.Unlock()
	return err
}

func (w *Writer) cached() bool {
	return len(w.buffers) > 0 || w.size > 0
}

func (w *Writer) cache() (c [][]byte, length int) {
	if w.size > 0 {
		w.push()
		if !w.scheduling {
			w.buffer = w.pool.GetBuffer(w.mss)
		}
	}
	c = w.buffers
	length = w.length
	w.buffers = nil
	w.length = 0
	w.size = 0
	return
}

func (w *Writer) flush(p []byte, c [][]byte, l int) (err error) {
	if l > 0 {
		for _, b := range c {
			_, err = w.writer.Write(b)
			w.pool.PutBuffer(b)
		}
	}
	length := len(p)
	if length > 0 {
		_, err = w.writer.Write(p)
	}
	return
}

func (w *Writer) run() {
	for {
		w.delay()
		w.lock.Lock()
		var c [][]byte
		var l int
		if w.cached() {
			c, l = w.cache()
		}
		if l > 0 {
			w.wLock.Lock()
			w.lock.Unlock()
			w.flush(nil, c, l)
			w.wLock.Unlock()
			continue
		}
		w.cond.Wait()
		if atomic.LoadInt32(&w.closed) > 0 {
			w.lock.Unlock()
			atomic.StoreInt32(&w.done, 1)
			return
		}
		w.lock.Unlock()
	}
}

// Close closes the writer, but do not close the underlying io.Writer
func (w *Writer) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	err = w.Flush()
	if w.scheduling {
		w.sched.Close()
	} else {
		for {
			w.cond.Signal()
			time.Sleep(time.Microsecond * 100)
			if atomic.LoadInt32(&w.done) > 0 {
				break
			}
		}
	}
	return nil
}
