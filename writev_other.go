// Copyright (c) 2023 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd
// +build !linux,!darwin,!dragonfly,!freebsd,!netbsd,!openbsd

package writer

// Writev system call writes iovcnt buffers of data described by iov to the file associated with the file descriptor fd ("gather output").
func Writev(fd int, buffers [][]byte) (n int, err error) {
	return writev(fd, buffers)
}
