// Copyright (c) 2020 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

package writer

import (
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
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
	testConcurrency(1, 0, t)
	testConcurrency(4, 0, t)
	testConcurrency(64, 128, t)
	testConcurrency(256, 128, t)
	testConcurrency(1024, 1024, t)
}

func testConcurrency(batch, mss int, t *testing.T) {
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
	writer := NewWriter(w, mss)
	num := 512
	msg := make([]byte, 512)
	wg := sync.WaitGroup{}
	for i := 0; i < batch; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < num; i++ {
				atomic.AddInt64(&count, 1)
				writer.Write(msg)
				time.Sleep(time.Millisecond * 1)
				atomic.AddInt64(&count, -1)
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
	if size != 512*num*batch {
		t.Error(size)
	}
}

func TestRun(t *testing.T) {
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
	writer := NewWriter(w, 0)
	msg := make([]byte, 512)
	wg := sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				atomic.AddInt64(&count, 1)
				writer.Write(msg)
				atomic.AddInt64(&count, -1)
				time.Sleep(time.Millisecond * 101)
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
	testSize(512+1, t)
	testSize(512*4+1, t)
	testSize(512*16+1, t)
	testSize(512*28+1, t)
	testSize(512*32+1, t)
}

func testSize(mms int, t *testing.T) {
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
	writer := NewWriter(w, mms)
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
	writer.Close()
	w.Close()
	<-done
	if size != 512*100*64 {
		t.Error(size)
	}
}
