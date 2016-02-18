package diskqueue

import (
	"bufio"
	"bytes"
	"os"
	"sync"
)

type Diskqueue struct {
	//Inherite from sync RWMutex
	sync.RWMutex

	//File and Buffer operation
	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	//working channel
	readChan chan []byte

	writeChan         chan []byte
	writeResponseChan chan error

	emptyChan         chan int
	emptyResponseChan chan error

	exitChan     chan int
	exitSyncChan chan int
}

//Interface of Diskqueue
type WorkQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// new a instance of diskqueue to retrive data
func NewDiskqueue() WorkQueue {
	dq := Diskqueue{}
	return &dq
}

// Put data to diskqueue
func (d *Diskqueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Read data from disk queue
// this is expected to be an *unbuffered* channel
func (d *Diskqueue) ReadChan() []byte {
	return <-d.readChan
}

func (d *Diskqueue) writeDataToFile(data []byte) error {
	return nil
}

// Major working thread to handle concurrency
func (d *Diskqueue) inLoop() {

	//data for readChan
	var data []byte
	//To store total data write
	var count int64
	readC := new(chan []byte)

	//TODO process data

	for {

		select {
		case readC <- data:
			// Handle read case
			// the Go channel spec dictates that nil channel operations (read or write)
			// in a select are skipped, we set r to d.readChan only when there is data to read

		case <-d.emptyChan:
			// Handle empty command thread
		case dataWrite := <-d.writeChan:
			// Handle write case
			count++
			d.writeResponseChan <- d.writeDataToFile(dataWrite)

		case <-d.exitChan:
			// Handle exist case
			goto exit
		}
	}
exit:

	d.exitSyncChan <- 1

}
