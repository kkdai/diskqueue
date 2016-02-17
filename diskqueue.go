package diskqueue

type Diskqueue struct {
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
func (d *Diskqueue) Put([]byte) error {
	return nil
}

// Read data from disk queue
// this is expected to be an *unbuffered* channel
func (d *Diskqueue) ReadChan() []byte {
}

// Major working thread to handle concurrency
func (d *Diskqueue) inLoop() {
}
