package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

//Diskqueue A Disk queue structure
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
	exitChan          chan int
	exitSyncChan      chan int
}

// WorkQueue :Interface of Diskqueue
type WorkQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	//Delete() error
	Depth() int64
	Empty() error
}

// NewDiskqueue :A new a instance of diskqueue to retrive data
func NewDiskqueue(name string, path string) WorkQueue {
	dq := Diskqueue{
		name:              name,
		filePath:          path,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
	}

	err := dq.readMetaDataFile()
	if err != nil && !os.IsNotExist(err) {
		//if error is meta file not exist, log it down
		log.Println("err on init read meta data file")
	}

	err = dq.writeMetaDataFile()
	if err != nil {
		log.Println("err on init write meta data file")
	}

	//Start to working loop
	go dq.inLoop()
	return &dq
}

// Empty :Cleanup queue and files
func (d *Diskqueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

// Depth :Return the depth from this queue (return total length this queue, include all data in files)
func (d *Diskqueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// Close :this diskQueue
func (d *Diskqueue) Close() error {
	d.exitChan <- 1
	return nil
}

// Put :data to diskqueue
func (d *Diskqueue) Put(data []byte) error {
	log.Println("Enter put data:", string(data))
	d.RLock()
	defer d.RUnlock()

	log.Println("put after lock")
	d.writeChan <- data
	return <-d.writeResponseChan
}

// ReadChan : Read data from disk queue
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

	log.Println("temp meta file:", tmpFile)

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

// readDataFromFile perform low level way to retrieval data from file
func (d *Diskqueue) readDataFromFile() ([]byte, error) {
	var err error
	var dataLength int32

	log.Println("read file:", d.fileName(d.readFileNum))
	if d.readFile == nil {
		//never open file before
		d.readFile, err = os.OpenFile(d.fileName(d.readFileNum), os.O_RDONLY, 0600)
		if err != nil {
			log.Println("Open read file err")
			return nil, err
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	//TODO handle seek and size control

	err = binary.Read(d.reader, binary.BigEndian, &dataLength)
	if err != nil {
		log.Println("Read read file binary err")
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}
	log.Println("Read find data size:", dataLength)
	readBuf := make([]byte, dataLength)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		log.Println("Read read to buffer error")
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	//TODO. check message size

	d.readFile.Close()
	d.readFile = nil
	return readBuf, nil
}

// writeDataToFile perform low level data write to file using buffer
func (d *Diskqueue) writeDataToFile(data []byte) error {
	var err error

	if d.writeFile == nil {
		//never open file before
		d.writeFile, err = os.OpenFile(d.fileName(d.writeFileNum), os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Println("write data file err:", err)
			return err
		}

	}

	log.Println("Write data:", string(data), " to file:", d.fileName(d.writeFileNum))

	dataLength := int32(len(data))
	//TODO. check message size

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLength)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	log.Println("Write buf:", d.writeBuf, " data:", string(d.writeBuf.Bytes()), " ori:", string(data))
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	log.Println("Write data to buffer done")
	//TODO handle seek and size control
	d.writeFile.Close()
	d.writeFile = nil
	d.writeFileNum++
	return nil
}

func (d *Diskqueue) moveReaderForward() {
	os.Remove(d.fileName(d.readFileNum))
	d.readFileNum++
	d.writeMetaDataFile()
}

func (d *Diskqueue) removeAllFiles() error {
	os.RemoveAll(d.metaDataFileName())
	return os.RemoveAll(d.filePath)
}

func (d *Diskqueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	return d.writeMetaDataFile()
}

// Major working thread to handle concurrency
func (d *Diskqueue) inLoop() {
	var err error
	var dataRead []byte //data for readChan
	var count int64     //To store total data write

	var readDataChan chan []byte
	for {

		var fileSync bool
		if count > 5 {
			count = 0
			fileSync = true
		}

		if fileSync {
			err = d.sync()
			if err != nil {
				log.Println("File sync error:", err)
			}
		}

		//When first data put the read can work normally, otherwise keep loop
		if d.readFileNum < d.writeFileNum {
			dataRead, err = d.readDataFromFile()
			if err != nil {
				//TODO handle read error
				log.Panic("Read err:", err)
				continue
			} else {
				log.Println("Got read data:", dataRead)
			}
			readDataChan = d.readChan
		} else {
			readDataChan = nil
		}

		select {

		case <-d.emptyChan:
			// Handle empty command thread
			d.emptyResponseChan <- d.removeAllFiles()

		case readDataChan <- dataRead:
			// Handle read case
			// the Go channel spec dictates that nil channel operations (read or write)
			// in a select are skipped, we set r to d.readChan only when there is data to read
			d.moveReaderForward()
			log.Println("Read move next..")

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
