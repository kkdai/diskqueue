package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type Diskqueue struct {
	//Inherite from sync RWMutex
	sync.RWMutex

	//Initialize parameter
	name     string
	filePath string

	//meata data related
	depth        int64
	readFileNum  int64
	writeFileNum int64

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
	//Delete() error
	Depth() int64
	Empty() error
}

// new a instance of diskqueue to retrive data
func NewDiskqueue(name string, path string) WorkQueue {
	dq := Diskqueue{
		name:     name,
		filePath: path,
	}
	return &dq
}

// Cleanup queue and files
func (d *Diskqueue) Empty() error {
	return nil
}

// Return the depth from this queue (return total length this queue, include all data in files)
func (d *Diskqueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// Close this diskQueue
func (d *Diskqueue) Close() error {
	return nil
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
func (d *Diskqueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *Diskqueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.filePath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *Diskqueue) fileName(filenum int64) string {
	return fmt.Sprintf(path.Join(d.filePath, "%s.diskqueue.%06d.dat"), d.name, filenum)
}

func (d *Diskqueue) writeMetaDataFile() error {
	var f *os.File
	var err error

	metaFile := d.metaDataFileName()
	tmpFile := fmt.Sprintf("%s.%d.tmp", d.filePath, rand.Int())

	//Opeb temp meta tata file to write first, avoid any conflict
	f, err = os.OpenFile(tmpFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d\n%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum,
		d.writeFileNum)

	if err != nil {
		f.Close()
		return err
	}

	f.Sync()
	f.Close()

	//Done, replace to real meta data file
	return os.Rename(tmpFile, metaFile)
}

func (d *Diskqueue) readMetaDataFile() error {

	var f *os.File
	var err error

	metaFile := d.metaDataFileName()
	f, err = os.OpenFile(metaFile, os.O_RDONLY, 0600)
	if err != nil {
		f.Close()
		return err
	}

	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d\n%d\n",
		&depth,
		&d.readFileNum,
		&d.writeFileNum)

	atomic.StoreInt64(&d.depth, depth)
	return nil
}

func (d *Diskqueue) writeDataToFile(data []byte) error {
	var err error

	if d.writeFile == nil {
		//never open file before
		d.writeFile, err = os.OpenFile(d.fileName(d.writeFileNum), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

	}

	dataLength := int32(len(data))

	//TODO. check message size

	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLength)
	if err != nil {
		return err
	}

	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	//TODO handle seek and size control

	d.writeFileNum++
	return nil
}

// Major working thread to handle concurrency
func (d *Diskqueue) inLoop() {

	var dataRead []byte //data for readChan
	var count int64     //To store total data write

	for {

		//TODO process data
		readC := d.readChan

		select {
		case readC <- dataRead:
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
