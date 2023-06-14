package storage

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestSearchVolumesWithDeletedNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	count := 20

	for i := 1; i < count; i++ {
		n := newRandomNeedle(uint64(i))
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	for i := 1; i < 15; i++ {
		n := newEmptyNeedle(uint64(i))
		err := v.nm.Put(n.Id, types.Offset{}, types.TombstoneFileSize)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	ts1 := time.Now().UnixNano()

	for i := 15; i < count; i++ {
		n := newEmptyNeedle(uint64(i))
		_, err := v.doDeleteRequest(n)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	offset, isLast, err := v.BinarySearchByAppendAtNs(uint64(ts1))
	if err != nil {
		t.Fatalf("lookup by ts: %v", err)
	}
	fmt.Printf("offset: %v, isLast: %v\n", offset.ToActualOffset(), isLast)

}

func isFileExist(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func assertFileExist(t *testing.T, expected bool, path string) {
	exist, err := isFileExist(path)
	if err != nil {
		t.Fatalf("isFileExist: %v", err)
	}
	assert.Equal(t, expected, exist)
}

func TestDestroyEmptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume with onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(true)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyEmptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume without onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyNonemptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should return "volume not empty" error and do not delete file when Destroy non-empty volume
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(true)
	assert.EqualError(t, err, "volume not empty")
	assertFileExist(t, true, path)

	// should keep working after "volume not empty"
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}

	assert.Equal(t, uint64(2), v.FileCount())
}

func TestDestroyNonemptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy non-empty volume without onlyEmpty
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestPhantomRead(t *testing.T) {
	//dir := t.TempDir()
	// RAMDisk: https://stackoverflow.com/questions/2033362/does-os-x-have-an-equivalent-to-dev-shm
	dir := fmt.Sprintf("/Volumes/RAMDisk/seaweedtest/%d", time.Now().UnixNano())
	os.Mkdir(dir, os.ModePerm)

	wrongCount := 0

	for i := 1; i <= 70000; i++ {
		if i%10000 == 0 {
			fmt.Printf("%d at %s\n", i, time.Now())
		}
		v, err := NewVolume(dir, dir, "", needle.VolumeId(uint32(i)), NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
		if err != nil {
			t.Fatalf("volume creation: %v", err)
		}
		path := v.DataBackend.Name()
		var wg sync.WaitGroup
		cleanerCompletedChan := make(chan interface{}, 1)
		wg.Add(2)
		go func() {
			defer func() {
				cleanerCompletedChan <- nil
				wg.Done()
			}()
			cleaner(v)
		}()
		go func() {
			defer wg.Done()

			writerErr := writer(t, v, path, cleanerCompletedChan)
			if writerErr != nil {
				//assertFileExist(t, true, path)
				fmt.Printf("writer %s: %v\n", path, writerErr)
				wrongCount += 1
			}
		}()
		wg.Wait()
		v.Close()
	}
	fmt.Printf("wrongCount: %d\n", wrongCount)
}

func writer(t *testing.T, v *Volume, path string, cleanerCompletedChan chan interface{}) error {
	originalNeedle := newRandomNeedle(1)
	if len(originalNeedle.Data) == 0 {
		return nil // skip empty data
	}
	err := catchPanic(func() (err error) {
		_, _, _, err = v.writeNeedle2(originalNeedle, true, false)
		return
	})
	if err != nil {
		return nil // accept cannot write (maybe already cleaned)
	}

	<-cleanerCompletedChan

	//v.read
	exist, err := isFileExist(path)
	if err != nil {
		return fmt.Errorf("isFileExist: %v", err)
	}
	if !exist {
		return fmt.Errorf("file not exists after write")
	}

	nv, _ := v.nm.Get(originalNeedle.Id)
	readNeedle := new(needle.Needle)
	readNeedle.DataSize = uint32(len(originalNeedle.Data))
	buf := make([]byte, len(originalNeedle.Data))
	count, err := readNeedle.ReadNeedleData(v.DataBackend, nv.Offset.ToActualOffset(), buf, 0)
	if err != nil {
		return fmt.Errorf("volume read after write: %v", err)
	}
	if count != len(originalNeedle.Data) {
		return fmt.Errorf("count != len(originalNeedle.Data)")
	}
	if !assert.Equal(t, originalNeedle.Data, buf) {
		return fmt.Errorf("buf not originalNeedle.Data")
	}
	return nil
}

func cleaner(v *Volume) {
	err := v.DestroyOnlyEmpty()
	//err := v.Destroy(true)
	if err != nil && err != ErrVolumeNotEmpty {
		fmt.Printf("cleaner: %v\n", err)
	}
}

func catchPanic(fun func() error) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if err == nil {
				err = fmt.Errorf("panic occurred: %v", e)
			}
		}
	}()
	err = fun()
	return
}
