// Copyright (c) 2023 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd
// +build linux darwin dragonfly freebsd netbsd openbsd

package writer

import (
	"fmt"
	"syscall"
	"unsafe"
)

const (
	IOV_MAX         = 1024
	WRITEV_SIZE_MAX = maximumSegmentSize * IOV_MAX
)

// Writev system call writes iovcnt buffers of data described by iov to the file associated with the file descriptor fd ("gather output").
func Writev(fd int, buffers [][]byte) (n int, err error) {
	if len(buffers) == 0 {
		return
	}
	start := 0
	length := 0
	iovecs := make([]syscall.Iovec, 0, IOV_MAX)
	for _, buf := range buffers {
		if len(buf) > 0 {
			length += len(buf)
			iovec := syscall.Iovec{
				Base: &buf[0],
				Len:  uint64(len(buf)),
			}
			iovecs = append(iovecs, iovec)
		}
		if len(iovecs) >= IOV_MAX || length >= WRITEV_SIZE_MAX || len(buf) == 0 {
			if length > 0 {
				n, err = writevFd(fd, length, buffers[start:start+len(iovecs)], iovecs)
				checkWritev(length, n, err)
				if err != nil {
					return
				}
			}
			start += len(iovecs)
			length = 0
			iovecs = iovecs[:0]
		}
	}
	if len(iovecs) > 0 && length > 0 {
		n, err = writevFd(fd, length, buffers[start:], iovecs)
		checkWritev(length, n, err)
	}
	return n, err
}

func checkWritev(length, n int, err error) {
	if err == nil && length != n || err == syscall.EINVAL {
		panic(fmt.Sprint(length, n, err))
	}
}

func writevFd(fd, length int, buffers [][]byte, iovecs []syscall.Iovec) (n int, err error) {
	for length-n > 0 {
		offset := 0
		var iov []syscall.Iovec
		for i, buf := range buffers {
			if offset+len(buf) > n {
				iov = iovecs[i:]
				baseIndex := n - offset
				iovec := syscall.Iovec{
					Base: &buf[baseIndex],
					Len:  uint64(len(buf) - baseIndex),
				}
				iov[0] = iovec
				break
			}
			offset += len(buf)
		}
		r, _, e := syscall.Syscall(
			syscall.SYS_WRITEV,
			uintptr(fd), uintptr(unsafe.Pointer(&iov[0])), uintptr(len(iov)))
		size := int(r)
		if size > 0 {
			n += size
		}
		if e != syscall.Errno(0) && e != syscall.EINTR && e != syscall.EAGAIN {
			return n, e
		}
	}
	return
}
