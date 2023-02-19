// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package writer implements buffering for an io.Writer object.
package writer

import (
	"errors"
	"github.com/hslam/buffer"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

func netFd(conn net.Conn) (int, error) {
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return 0, ErrSyscallConn
	}
	return fd(syscallConn)
}

func fd(c syscall.Conn) (int, error) {
	var nfd int
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	raw.Control(func(fd uintptr) {
		nfd = int(fd)
	})
	return nfd, nil
}

// Writer implements buffering for an io.Writer object.
type Writer struct {
	lock    sync.Mutex
	wLock   sync.Mutex
	bw      BufWriter
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
		mss:  size,
		pool: buffer.AssignPool(size),
	}
	if bw, ok := writer.(BufWriter); ok {
		w.bw = bw
	} else if conn, ok := writer.(net.Conn); ok {
		fd, err := netFd(conn)
		if err == nil {
			w.bw = NewFdWriter(fd)
		} else {
			w.bw = &bufWriter{
				writer: writer,
			}
		}
	} else {
		w.bw = &bufWriter{
			writer: writer,
		}
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
