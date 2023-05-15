// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package writer implements buffering for an io.Writer object.
package writer

import (
	"errors"
	"github.com/hslam/atomic"
	"github.com/hslam/buffer"
	"io"
	"sync"
	"syscall"
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

// BufWriter is the interface that wraps the Writev method.
type BufWriter interface {
	Writev(p [][]byte) (n int, err error)
	Write(p []byte) (n int, err error)
}

type bufWriter struct {
	writer io.Writer
}

func (bw *bufWriter) Writev(c [][]byte) (n int, err error) {
	for _, b := range c {
		var written int
		written, err = bw.writer.Write(b)
		n += written
	}
	return
}

func (bw *bufWriter) Write(p []byte) (n int, err error) {
	return bw.writer.Write(p)
}

// NewFdWriter returns a BufWriter.
func NewFdWriter(fd int) BufWriter {
	return &fdWriter{
		fd: fd,
	}
}

type fdWriter struct {
	fd int
}

func (bw *fdWriter) Writev(c [][]byte) (n int, err error) {
	return Writev(bw.fd, c)
}

func (bw *fdWriter) Write(p []byte) (n int, err error) {
	return Writev(bw.fd, [][]byte{p})
}

// ErrSyscallConn will be returned when the net.Conn do not implement the syscall.Conn interface.
var ErrSyscallConn = errors.New("The net.Conn do not implement the syscall.Conn interface")

func syscallFd(w io.Writer) (int, error) {
	c, ok := w.(syscall.Conn)
	if !ok {
		return 0, ErrSyscallConn
	}
	return fileDescriptor(c)
}

func fileDescriptor(c syscall.Conn) (int, error) {
	var descriptor int
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	raw.Control(func(fd uintptr) {
		descriptor = int(fd)
	})
	return descriptor, nil
}

// Writer implements buffering for an io.Writer object.
type Writer struct {
	lock    sync.Mutex
	writing *atomic.Bool
	bw      BufWriter
	mss     int
	pool    *buffer.Pool
	buffer  []byte
	size    int
	buffers [][]byte
	running bool
	closed  *atomic.Bool
}

// NewWriter returns a new buffered writer whose buffer has at least the specified size.
func NewWriter(writer io.Writer, size int) *Writer {
	if size < 1 {
		size = maximumSegmentSize
	}
	w := &Writer{
		mss:     size,
		pool:    buffer.AssignPool(size),
		writing: atomic.NewBool(false),
		closed:  atomic.NewBool(false),
	}
	if bw, ok := writer.(BufWriter); ok {
		w.bw = bw
	} else {
		fd, err := syscallFd(writer)
		if err == nil {
			w.bw = NewFdWriter(fd)
		} else {
			w.bw = &bufWriter{
				writer: writer,
			}
		}
	}
	return w
}

// Write writes the contents of p into the buffer or the underlying io.Writer.
// It returns the number of bytes written.
func (w *Writer) Write(p []byte) (n int, err error) {
	if w.closed.Load() {
		return 0, ErrWriterClosed
	}
	w.lock.Lock()
	if !w.running {
		w.running = true
		w.append(p)
		go w.run()
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
	for !w.writing.CompareAndSwap(false, true) {
	}
	var c [][]byte
	if w.buffered() {
		c = w.cache()
	}
	w.lock.Unlock()
	err := w.flush(c)
	w.writing.Store(false)
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
	if len(c) == 1 {
		b := c[0]
		_, err = w.bw.Write(b)
		w.pool.PutBuffer(b)
	} else if len(c) > 1 {
		_, err = w.bw.Writev(c)
		for _, b := range c {
			w.pool.PutBuffer(b)
		}
	}
	return
}

func (w *Writer) run() {
	for {
		w.lock.Lock()
		if !w.buffered() {
			w.running = false
			w.lock.Unlock()
			return
		}
		if w.writing.CompareAndSwap(false, true) {
			c := w.cache()
			w.lock.Unlock()
			w.flush(c)
			w.writing.Store(false)
		} else {
			w.lock.Unlock()
		}
	}
}

// Close closes the writer, but do not close the underlying io.Writer
func (w *Writer) Close() (err error) {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	return w.Flush()
}
