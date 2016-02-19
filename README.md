Diskqueue: Disk storage Message Queue package refer from NSQ
==============

[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/kkdai/diskqueue/master/LICENSE)  [![GoDoc](https://godoc.org/github.com/kkdai/diskqueue?status.svg)](https://godoc.org/github.com/kkdai/diskqueue)  [![Build Status](https://travis-ci.org/kkdai/diskqueue.svg?branch=master)](https://travis-ci.org/kkdai/diskqueue)


What is this "Disk Queue"
=============

Diskqueue is a submodule of [NSQ](https://github.com/nsqio/nsq) which to use Disk to store message queue.




Disclaimer
=============

This is a going project, during I learn the program about [NSQ](https://github.com/nsqio/nsq). Until all ToDo item done, please use this under your own risk. 

If you really need disk queue, please use [NSQ: Diskqueue source code](https://github.com/nsqio/nsq/blob/master/nsqd/diskqueue.go) in your production consideration.


Currently Features
=============
- Basic Queue handle
- One messsage into single file

ToDo Features
=============
- Seek handle 
	- One file store not only one message
	- Seek write/read handle 
- Gracefully handle for file R/W
	- Message size validation (min/max message size)

Installation and Usage
=============


Install
---------------

    go get github.com/kkdai/diskqueue


Usage
---------------

Following is sample code:


```go

package main

import (
	"fmt"
    "github.com/kkdai/diskqueue"
)

func main() {

	//Create a disk queue, please note the path must exist
	dq := NewDiskqueue("workqueue", "./test")

	//First put data
	err = dq.Put([]byte("00"))
	//Second data put
	err = dq.Put([]byte("111"))

	//Read data from queue, it is unbuffered channel
	data := <-dq.ReadChan()
	
	//Close this queue
	dq.Close()
}
```

Inspired By
=============

- [NSQ: Diskqueue source code](https://github.com/nsqio/nsq/blob/master/nsqd/diskqueue.go)
- [nsq源码阅读笔记之nsqd（三）——diskQueue](http://blog.123hurray.tk/2015/11/27/nsqd_source_3_diskqueue/)
- [深入NSQ 之旅](http://www.oschina.net/translate/day-22-a-journey-into-nsq)
- [NSQ源码剖析之NSQD](http://shanks.leanote.com/post/NSQ%E6%BA%90%E7%A0%81%E5%89%96%E6%9E%90%E4%B9%8BNSQD)

Project52
---------------

It is one of my [project 52](https://github.com/kkdai/project52).


License
---------------

This package is licensed under MIT license. See LICENSE for details.
