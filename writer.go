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
	thresh             = 16
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
	buffer      []byte
	size        int
	closed      int32
	done        int32
}

// NewWriter returns a new batch Writer with the concurrency.
func NewWriter(writer io.Writer, concurrency func() int, size int, shared bool) *Writer {
	if size < 1 {
		size = maximumSegmentSize
	}
	var buffer []byte
	if !shared {
		buffer = make([]byte, size)
	}
	w := &Writer{
		writer:      writer,
		concurrency: concurrency,
		shared:      shared,
		mss:         size,
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
	direct := true
	if w.concurrency != nil {
		if w.batch() > thresh {
			direct = false
		}
	}
	length := len(p)
	var flushed bool
	w.lock.Lock()
	if direct || w.size+length > w.mss {
		if w.size > 0 {
			w.flush()
		}
		if length > 0 {
			_, err = w.writer.Write(p)
		}
		flushed = true
	} else {
		if w.shared && len(w.buffer) == 0 {
			w.buffer = buffers.AssignPool(w.mss).GetBuffer(w.mss)
		}
		copy(w.buffer[w.size:], p)
		w.size += length
	}
	w.lock.Unlock()
	if !flushed {
		w.cond.Signal()
	}
	if err == nil {
		n = len(p)
	}
	return n, err
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	w.lock.Lock()
	err := w.flush()
	w.lock.Unlock()
	return err
}

func (w *Writer) flush() (err error) {
	if w.size > 0 {
		_, err = w.writer.Write(w.buffer[:w.size])
		if w.shared {
			buffers.AssignPool(w.mss).PutBuffer(w.buffer)
			w.buffer = nil
		}
		w.size = 0
	}
	return
}

func (w *Writer) run() {
	for {
		var batch int
		if w.concurrency != nil {
			batch = w.batch()
		}
		var duration = time.Microsecond * time.Duration(batch/thresh/8)
		if duration > time.Microsecond*64 {
			duration = time.Microsecond * 64
		}
		if duration > 0 {
			time.Sleep(duration)
		}
		w.lock.Lock()
		w.flush()
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
