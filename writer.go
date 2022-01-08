// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package writer implements buffering for an io.Writer object.
package writer

import (
	"errors"
	"github.com/hslam/buffer"
	"io"
	"sync"
	"sync/atomic"
)

const (
	maximumSegmentSize = 65536
)

// ErrWriterClosed is returned by the Writer's Write methods after a call to Close.
var ErrWriterClosed = errors.New("Writer closed")

// Flusher is the interface that wraps the basic Flush method.
//
// Flush writes any buffered data to the underlying io.Writer.
type Flusher interface {
	Flush() (err error)
}

// Writer implements buffering for an io.Writer object.
type Writer struct {
	lock    sync.Mutex
	wLock   sync.Mutex
	writer  io.Writer
	mss     int
	pool    *buffer.Pool
	buffer  []byte
	size    int
	buffers [][]byte
	writing bool
	closed  int32
	done    int32
}

// NewWriter returns a new buffered writer whose buffer has at least the specified size.
func NewWriter(writer io.Writer, size int) *Writer {
	if size < 1 {
		size = maximumSegmentSize
	}
	w := &Writer{
		writer: writer,
		mss:    size,
		pool:   buffer.AssignPool(size),
	}
	return w
}

// Write writes the contents of p into the buffer or the underlying io.Writer.
// It returns the number of bytes written.
func (w *Writer) Write(p []byte) (n int, err error) {
	if atomic.LoadInt32(&w.closed) > 0 {
		return 0, ErrWriterClosed
	}
	w.lock.Lock()
	if !w.writing {
		w.writing = true
		w.append(p)
		go w.write()
	} else {
		w.append(p)
	}
	w.lock.Unlock()
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
	w.size = 0
	w.buffers = append(w.buffers, buffer)
	w.buffer = nil
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
	if w.buffered() {
		c = w.cache()
	}
	w.wLock.Lock()
	w.lock.Unlock()
	err := w.flush(c)
	w.wLock.Unlock()
	return err
}

func (w *Writer) buffered() bool {
	return len(w.buffers) > 0 || w.size > 0
}

func (w *Writer) cache() (c [][]byte) {
	if w.size > 0 {
		w.push()
	}
	w.size = 0
	c = w.buffers
	w.buffers = nil
	return
}

func (w *Writer) flush(c [][]byte) (err error) {
	for _, b := range c {
		_, err = w.writer.Write(b)
		w.pool.PutBuffer(b)
	}
	return
}

func (w *Writer) write() {
	for {
		w.lock.Lock()
		var c [][]byte
		if w.buffered() {
			c = w.cache()
		}
		if len(c) == 0 {
			w.writing = false
			w.lock.Unlock()
			return
		}
		w.wLock.Lock()
		w.lock.Unlock()
		w.flush(c)
		w.wLock.Unlock()
	}
}

// Close closes the writer, but do not close the underlying io.Writer
func (w *Writer) Close() (err error) {
	if !atomic.CompareAndSwapInt32(&w.closed, 0, 1) {
		return nil
	}
	return w.Flush()
}
