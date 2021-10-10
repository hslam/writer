// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package writer implements batch writing.
package writer

import (
	"errors"
	"github.com/hslam/buffer"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	thresh             = 1
	maximumSegmentSize = 65536
	lastsSize          = 4
)

var (
	buffers = buffer.NewBuffers(1024)
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
	cond        sync.Cond
	writer      io.Writer
	shared      bool
	concurrency func() int
	lasts       [lastsSize]int
	cursor      int
	mss         int
	pool        *buffer.Pool
	buffer      []byte
	buffers     [][]byte
	size        int
	length      int
	closed      int32
	done        int32
}

// NewWriter returns a new batch Writer with the concurrency.
func NewWriter(writer io.Writer, concurrency func() int, size int, shared bool) *Writer {
	if size < 1 {
		size = maximumSegmentSize
	}
	var pool = buffers.AssignPool(size)
	var buffer []byte
	if !shared {
		buffer = pool.GetBuffer(size)
	}
	w := &Writer{
		writer:      writer,
		concurrency: concurrency,
		shared:      shared,
		mss:         size,
		pool:        pool,
		buffer:      buffer,
	}
	w.cond.L = &w.lock
	go w.run()
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
		err = w.flush(p)
		w.lock.Unlock()
	} else {
		w.append(p)
		w.lock.Unlock()
		w.cond.Signal()
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
		copy(w.buffer[w.size:], p)
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
	err := w.flush(nil)
	w.lock.Unlock()
	return err
}

func (w *Writer) cached() bool {
	return len(w.buffers) > 0 || w.size > 0
}

func (w *Writer) cache() (c [][]byte, length int) {
	if w.size > 0 {
		w.push()
		if !w.shared {
			w.buffer = w.pool.GetBuffer(w.mss)
		}
	}
	c = w.buffers
	length = w.length
	w.buffers = nil
	w.length = 0
	return
}

func (w *Writer) flush(p []byte) (err error) {
	var c [][]byte
	var l int
	if w.cached() {
		c, l = w.cache()
	}
	length := len(p)
	if l > 0 {
		size := l + length
		pool := buffers.AssignPool(size)
		buf := pool.GetBuffer(size)
		var pos int
		if len(c) > 0 {
			for _, b := range c {
				s := copy(buf[pos:], b)
				pos += s
				w.pool.PutBuffer(b)
			}
		}
		if length > 0 {
			s := copy(buf[pos:], p)
			pos += s
		}

		if pos > 0 {
			_, err = w.writer.Write(buf[:pos])
		}
		pool.PutBuffer(buf)
	} else if length > 0 {
		_, err = w.writer.Write(p)
	}
	w.size = 0
	return
}

func (w *Writer) run() {
	for {
		w.lock.Lock()
		w.flush(nil)
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
	for {
		w.cond.Signal()
		time.Sleep(time.Microsecond * 100)
		if atomic.LoadInt32(&w.done) > 0 {
			break
		}
	}
	return nil
}
