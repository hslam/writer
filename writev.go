// Copyright (c) 2023 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

package writer

import (
	"syscall"
)

func writev(fd int, buffers [][]byte) (n int, err error) {
	for _, buf := range buffers {
		if len(buf) == 0 {
			continue
		}
		var written int
		written, err = writeFd(fd, buf)
		n += written
		if err != nil {
			return
		}
	}
	return n, err
}

func writeFd(fd int, b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	var remain = len(b)
	for remain > 0 {
		n, err = syscall.Write(fd, b[len(b)-remain:])
		if n > 0 {
			remain -= n
			continue
		}
		if err != syscall.EINTR && err != syscall.EAGAIN {
			return len(b) - remain, err
		}
	}
	return len(b), nil
}
