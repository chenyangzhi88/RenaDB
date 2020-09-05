package mem_cache

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
)

func createEmptyFile(size int64) (*os.File, error) {
	f, err := os.Create("foo.bar")
	if err != nil {
		log.Fatal(err)
	}

	if err := f.Truncate(size); err != nil {
		log.Fatal(err)
	}
	return f, err
}
func TestMmap(t *testing.T) {
	//configFile, err := os.OpenFile("/tmp/test/logtext", os.O_RDWR, 0666)
	configFile, err := createEmptyFile(1024 * 1024 * 256)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err)
	}
	ts := time.Now()
	mp, err := mmap.MapRegion(configFile, 1024*1024*256, mmap.RDWR, 0, 0)
	defer mp.Unmap()
	//var mp [1024 * 1024 * 1024 * 4]byte
	if err != nil {
		fmt.Println("mmap error", err)
	}
	elapsed := time.Since(ts)
	//root := tr.GetRootNode()
	//root.Print(os.Stdout, 2)
	fmt.Println("the time elapsed ", elapsed)
	//fmt.Println(mp)
	var bt mmap.MMap
	for i := 0; i < 4095; i++ {
		bt = append(bt, 's')
	}
	bt = append(bt, '\n')
	ts = time.Now()
	for i := 0; i < 100; i++ {
		copy(mp[i*4096:(i+1)*4096], bt)
	}
	elapsed = time.Since(ts)
	fmt.Println("the time elapsed ", elapsed)
	//fmt.Println(mp)
	//some actions happen here
	//mp.Flush()
	//configFile.Write([]byte("fadfasfsdf"))
	//configFile.Sync()
	//configFile.Truncate(1024)
}
