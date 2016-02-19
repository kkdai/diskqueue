package diskqueue_test

import (
	"log"
	"testing"

	. "github.com/kkdai/diskqueue"
)

func TestEmptyQueue(t *testing.T) {
}

func TestBasicQueue(t *testing.T) {
	dq := NewDiskqueue("new1", "./test")
	err := dq.Put([]byte("00"))
	if err != nil {
		t.Error("err on test1 :", err)
	}

	log.Println("First put done")
	err = dq.Put([]byte("111"))
	if err != nil {
		t.Error("err on test2 :", err)
	}

	data := <-dq.ReadChan()
	log.Println("READ: ", string(data))
}

func TestEmpty(t *testing.T) {
	dq := NewDiskqueue("new1", "./test")
	err := dq.Empty()
	if err != nil {
		t.Error("Empty err:", err)
	}
}
