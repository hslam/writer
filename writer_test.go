// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

package writer

import (
	"github.com/hslam/inproc"
	"io"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestWriter(t *testing.T) {
	testWriter(4, 1, 2, t)
	testWriter(4, 2, 2, t)
	testWriter(4, 3, 2, t)
	testWriter(4, 4, 2, t)
	testWriter(65536, 128, 256, t)
	testWriter(65536, 256, 256, t)
	testWriter(65536, 512, 256, t)
}

func testWriter(contentSize, msgSize, mms int, t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	content := make([]byte, contentSize)
	rand.Read(content)
	res := []byte{}
	go func() {
		for {
			buf := make([]byte, 4096)
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
			res = append(res, buf[:n]...)

		}
		close(done)
	}()
	writer := NewWriter(w, mms)
	sent := 0
	for contentSize-sent > 0 {
		if contentSize-sent > msgSize {
			writer.Write(content[sent : sent+msgSize])
		} else {
			writer.Write(content[sent:])
		}
		sent += msgSize
	}
	writer.Close()
	w.Close()
	<-done
	if size != contentSize {
		t.Error(size)
	}
	if string(content) != string(res) {
		t.Errorf("contentSize %d, msgSize %d, mms %d, sent %v, got %v", contentSize, msgSize, mms, content, res)
	}
}

func TestNoConcurrency(t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(w, 0)
	msg := make([]byte, 512)
	wg := sync.WaitGroup{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				writer.Write(msg)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	w.Close()
	<-done
	if size != 512*100*64 {
		t.Error(size)
	}
}

func TestConcurrency(t *testing.T) {
	max := 10
	for batch := 1; batch < max; batch++ {
		for msgSize := 1; msgSize < max; msgSize++ {
			for num := 1; num < max; num++ {
				for mss := 0; mss < max; mss++ {
					testConcurrency(batch, msgSize, num, mss, t)
				}
			}
		}
	}
}

func testConcurrency(batch, msgSize, num, mss int, t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(w, mss)
	msg := make([]byte, msgSize)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				writer.Write(msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	w.Close()
	n, err := writer.Write(msg)
	if n != 0 || err == nil {
		t.Error(n, err)
	}
	<-done
	if size != msgSize*num*batch {
		t.Errorf("send %d get %d, batch %d msgSize %d num %d mss %d", msgSize*num*batch, size, batch, msgSize, num, mss)
	}
}

func TestBufWriterConcurrency(t *testing.T) {
	max := 10
	for batch := 1; batch < max; batch++ {
		for msgSize := 1; msgSize < max; msgSize++ {
			for num := 1; num < max; num++ {
				for mss := 0; mss < max; mss++ {
					testBufWriterConcurrency(batch, msgSize, num, mss, t)
				}
			}
		}
	}
}

func testBufWriterConcurrency(batch, msgSize, num, mss int, t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(&bufWriter{
		writer: w,
	}, mss)
	msg := make([]byte, msgSize)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				writer.Write(msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	w.Close()
	n, err := writer.Write(msg)
	if n != 0 || err == nil {
		t.Error(n, err)
	}
	<-done
	if size != msgSize*num*batch {
		t.Errorf("send %d get %d, batch %d msgSize %d num %d mss %d", msgSize*num*batch, size, batch, msgSize, num, mss)
	}
}

func TestNetConcurrency(t *testing.T) {
	max := 5
	for batch := 1; batch < max; batch++ {
		for msgSize := 1; msgSize < max; msgSize++ {
			for num := 1; num < max; num++ {
				for mss := 0; mss < max; mss++ {
					testNetConcurrency(batch, msgSize, num, mss, t)
				}
			}
		}
	}
}

func TestNetConcurrencyExtra(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < 8; i++ {
		batch := r.Intn(1024*2) + 1024
		msgSize := r.Intn(1024*2) + 1024*2
		num := r.Intn(128) + 16
		mss := r.Intn(1024*128) + 1024*16
		testNetConcurrency(batch, msgSize, num, mss, t)
	}
}

func testNetConcurrency(batch, msgSize, num, mss int, t *testing.T) {
	var network = "tcp"
	var address = ":9999"
	lis, err := net.Listen(network, address)
	if err != nil {
		t.Error(err)
	}
	size := 0
	done := make(chan struct{})
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			t.Error(err)
		}
		buf := make([]byte, 65536)
		for {
			conn.SetReadDeadline(time.Now().Add(time.Second * 3))
			n, err := conn.Read(buf)
			if err != nil || n == 0 {
				break
			}
			size += n
			if size >= msgSize*num*batch {
				break
			}
		}
		conn.Close()
		lis.Close()
		close(done)
	}()
	conn, err := net.Dial(network, address)
	if err != nil {
		t.Error(err)
	}
	writer := NewWriter(conn, mss)
	msg := make([]byte, msgSize)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				writer.Write(msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	<-done
	conn.Close()
	n, err := writer.Write(msg)
	if n != 0 || err == nil {
		t.Error(n, err)
	}
	if size != msgSize*num*batch {
		t.Errorf("send %d get %d, batch %d msgSize %d num %d mss %d", msgSize*num*batch, size, batch, msgSize, num, mss)
	}
}

type otherFdWriter struct {
	fd int
}

func (bw *otherFdWriter) Writev(c [][]byte) (n int, err error) {
	return writev(bw.fd, c)
}

func (bw *otherFdWriter) Write(p []byte) (n int, err error) {
	return writev(bw.fd, [][]byte{p})
}

func TestFdConcurrency(t *testing.T) {
	max := 5
	for batch := 1; batch < max; batch++ {
		for msgSize := 1; msgSize < max; msgSize++ {
			for num := 1; num < max; num++ {
				for mss := 0; mss < max; mss++ {
					testFdConcurrency(batch, msgSize, num, mss, t)
				}
			}
		}
	}
}

func TestFdConcurrencyExtra(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < 8; i++ {
		batch := r.Intn(1024*2) + 1024
		msgSize := r.Intn(1024) + 512
		num := r.Intn(128) + 32
		mss := r.Intn(1024*128) + 1024*16
		testFdConcurrency(batch, msgSize, num, mss, t)
	}
}

func testFdConcurrency(batch, msgSize, num, mss int, t *testing.T) {
	var network = "tcp"
	var address = ":9999"
	lis, err := net.Listen(network, address)
	if err != nil {
		t.Error(err)
	}
	size := 0
	done := make(chan struct{})
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			t.Error(err)
		}
		buf := make([]byte, 65536)
		for {
			conn.SetReadDeadline(time.Now().Add(time.Second * 3))
			n, err := conn.Read(buf)
			if err != nil || n == 0 {
				break
			}
			size += n
			if size >= msgSize*num*batch {
				break
			}
		}
		conn.Close()
		lis.Close()
		close(done)
	}()
	conn, err := net.Dial(network, address)
	if err != nil {
		t.Error(err)
	}
	fd, err := netFd(conn)
	if err != nil {
		t.Error(err)
	}
	writer := NewWriter(&otherFdWriter{fd: fd}, mss)
	msg := make([]byte, msgSize)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				writer.Write(msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	<-done
	conn.Close()
	n, err := writer.Write(msg)
	if n != 0 || err == nil {
		t.Error(n, err)
	}
	if size != msgSize*num*batch {
		t.Errorf("send %d get %d, batch %d msgSize %d num %d mss %d", msgSize*num*batch, size, batch, msgSize, num, mss)
	}
}

func TestInprocConcurrency(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < 1024; i++ {
		batch := r.Intn(32) + 1
		msgSize := r.Intn(32) + 1
		num := r.Intn(32) + 1
		mss := r.Intn(32)
		testInprocConcurrency(batch, msgSize, num, mss, t)
	}
}

func testInprocConcurrency(batch, msgSize, num, mss int, t *testing.T) {
	var address = ":9999"
	lis, err := inproc.Listen(address)
	if err != nil {
		t.Error(err)
	}
	size := 0
	done := make(chan struct{})
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			t.Error(err)
		}
		buf := make([]byte, 65536)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		lis.Close()
		close(done)
	}()
	conn, err := inproc.Dial(address)
	if err != nil {
		t.Error(err)
	}
	writer := NewWriter(conn, mss)
	msg := make([]byte, msgSize)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				writer.Write(msg)
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	conn.Close()
	n, err := writer.Write(msg)
	if n != 0 || err == nil {
		t.Error(n, err)
	}
	<-done
	if size != msgSize*num*batch {
		t.Errorf("send %d get %d, batch %d msgSize %d num %d mss %d", msgSize*num*batch, size, batch, msgSize, num, mss)
	}
}

func TestRun(t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(w, 0)
	msg := make([]byte, 512)
	wg := sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				writer.Write(msg)
				time.Sleep(time.Millisecond * 10)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	w.Close()
	<-done
	if size != 512*10*1 {
		t.Error(size)
	}
}

func TestShared(t *testing.T) {
	r, w := io.Pipe()
	count := int64(0)
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(w, 65536)
	msg := make([]byte, 512)
	wg := sync.WaitGroup{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				atomic.AddInt64(&count, 1)
				writer.Write(msg)
				atomic.AddInt64(&count, -1)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	w.Close()
	<-done
	if size != 512*100*64 {
		t.Error(size)
	}
}

func TestMMS(t *testing.T) {
	testSize(1, t)
	testSize(64+1, t)
	testSize(64*4+1, t)
	testSize(64*16+1, t)
	testSize(64*28+1, t)
	testSize(64*32+1, t)
}

func testSize(mms int, t *testing.T) {
	r, w := io.Pipe()
	size := 0
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := r.Read(buf)
			if err != nil {
				break
			}
			size += n
		}
		close(done)
	}()
	writer := NewWriter(w, mms)
	msg := make([]byte, 64)
	wg := sync.WaitGroup{}
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 64; i++ {
				writer.Write(msg)
			}
		}()
	}
	wg.Wait()
	writer.Close()
	writer.Close()
	w.Close()
	<-done
	if size != 64*64*64 {
		t.Error(size)
	}
}

func TestWritev(t *testing.T) {
	func() {
		defer func() {
			if e := recover(); e == nil {
				t.Error(e)
			}
		}()
		checkWritev(1024, 0, syscall.EINVAL)
	}()
	func() {
		defer func() {
			if e := recover(); e == nil {
				t.Error(e)
			}
		}()
		checkWritev(1024, 512, nil)
	}()
	Writev(1, [][]byte{})
	writev(1, [][]byte{{}, {}})
	writeFd(1, []byte{})
	func() {
		tmpDir, err := os.MkdirTemp("", "writev-tmp")
		if err != nil {
			t.Error(err)
		}
		defer os.RemoveAll(tmpDir)
		filename := path.Join(tmpDir, "tmp-file")
		f, err := os.Create(filename)
		if err != nil {
			t.Error(err)
		}
		fd := f.Fd()
		f.Close()
		os.Remove(filename)
		_, err = writev(int(fd), [][]byte{{0}})
		if err == nil {
			t.Error("should be err")
		}
	}()
	func() {
		tmpDir, err := os.MkdirTemp("", "writev-tmp")
		if err != nil {
			t.Error(err)
		}
		defer os.RemoveAll(tmpDir)
		filename := path.Join(tmpDir, "tmp-file")
		f, err := os.Create(filename)
		if err != nil {
			t.Error(err)
		}
		fd := f.Fd()
		f.Close()
		os.Remove(filename)
		var buffers [][]byte
		for i := 0; i < IOV_MAX*2; i++ {
			buffers = append(buffers, []byte(strings.Repeat("H", maximumSegmentSize*2)))
		}
		_, err = Writev(int(fd), buffers)
		if err == nil {
			t.Error("should be err")
		}
	}()
}

type testConn struct {
}

func (c *testConn) SyscallConn() (syscall.RawConn, error) {
	return nil, syscall.EBADF
}

func TestFd(t *testing.T) {
	_, err := fd(&testConn{})
	if err == nil {
		t.Error("should be err")
	}
}
