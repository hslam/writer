# writer
[![GoDoc](https://godoc.org/github.com/hslam/writer?status.svg)](https://godoc.org/github.com/hslam/writer)
[![Build Status](https://travis-ci.org/hslam/writer.svg?branch=master)](https://travis-ci.org/hslam/writer)
[![codecov](https://codecov.io/gh/hslam/writer/branch/master/graph/badge.svg)](https://codecov.io/gh/hslam/writer)
[![Go Report Card](https://goreportcard.com/badge/github.com/hslam/writer?v=7e100)](https://goreportcard.com/report/github.com/hslam/writer)
[![LICENSE](https://img.shields.io/github/license/hslam/writer.svg?style=flat-square)](https://github.com/hslam/writer/blob/master/LICENSE)

Package writer implements batch writing for an io.Writer object with the concurrency.

## Feature
* Shared Buffer
* Auto Batch Writing

## Get started

### Install
```
go get github.com/hslam/writer
```
### Import
```
import "github.com/hslam/writer"
```
### Usage
#### Example
```
package main

import (
	"fmt"
	"github.com/hslam/writer"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	RunParallel(1, false)
	RunParallel(16, false)
	RunParallel(32, false)
	RunParallel(1, true)
	RunParallel(16, true)
	RunParallel(32, true)
}

func RunParallel(num int, batch bool) {
	start := time.Now()
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
	worker := &Worker{msg: make([]byte, 512)}
	if batch {
		worker.w = writer.NewWriter(w, worker.concurrency, 512*64, false)
	} else {
		worker.w = w
	}
	wg := sync.WaitGroup{}
	for i := 0; i < num; i++ {
		wg.Add(1)
		go worker.run(&wg)
	}
	wg.Wait()
	worker.w.Close()
	w.Close()
	<-done
	fmt.Printf("time - %.1fs, batch - %t,\tparallel - %d,\twrite - %dMByte/s\n",
		float64(time.Now().Sub(start))/1E9, batch, num, size/1E6)
}

type Worker struct {
	count int64
	w     io.WriteCloser
	msg   []byte
}

func (w *Worker) concurrency() int {
	return int(atomic.LoadInt64(&w.count))
}

func (w *Worker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	t := time.NewTimer(time.Second)
	for {
		atomic.AddInt64(&w.count, 1)
		w.w.Write(w.msg)
		atomic.AddInt64(&w.count, -1)
		select {
		case <-t.C:
			return
		default:
		}
	}
}
```

#### Output
```
time - 1.0s, batch - false,	parallel - 1,	write - 514MByte/s
time - 1.0s, batch - false,	parallel - 16,	write - 386MByte/s
time - 1.0s, batch - false,	parallel - 32,	write - 382MByte/s
time - 1.0s, batch - true,	parallel - 1,	write - 498MByte/s
time - 1.0s, batch - true,	parallel - 16,	write - 1005MByte/s
time - 1.0s, batch - true,	parallel - 32,	write - 2006MByte/s
```

### License
This package is licensed under a MIT license (Copyright (c) 2020 Meng Huang)


### Authors
writer was written by Meng Huang.


