package diskqueue_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/kkdai/diskqueue"
)

func TestEmptyQueue(t *testing.T) {
}

func TestBasicQueue(t *testing.T) {
	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	dq := NewDiskqueue(dqName, tmpDir)
	err = dq.Put([]byte("00"))
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
	dq.Close()
}

func TestEmpty(t *testing.T) {
	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	dq := NewDiskqueue(dqName, tmpDir)
	err = dq.Empty()
	if err != nil {
		t.Error("Empty err:", err)
	}
}
