package iowrapper

import (
	"common"
	"io/ioutil"
	"log"
	"os"

	"github.com/directio"
)

func PathExist(_path string) bool {
	_, err := os.Stat(_path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func CreateMMapFile(pathFile string, fileSize int64) (*os.File, error) {
	if PathExist(pathFile) {
		f, err := os.Open(pathFile)
		return f, err
	}
	f, err := os.Create(pathFile)
	if err == nil {
		err = f.Truncate(fileSize)
	}
	return f, err
}

func CreateSparseFile(pathFile string, fileSize int64) error {
	if PathExist(pathFile) {
		return nil
	}
	f, err := os.Create(pathFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := f.Truncate(fileSize); err != nil {
		log.Fatal(err)
	}
	return nil
}

func TruncateIndexFile(pathFile string, extendSize int64) error {
	fileHandle, err := os.OpenFile(pathFile, os.O_RDWR, 0666)
	defer fileHandle.Close()
	fi, err := fileHandle.Stat()
	fileHandle.Truncate(extendSize + fi.Size())
	fileHandle.Sync()
	return err
}

func MmapIndexFile() {

}

func WriteFile(fileName string, b []byte) {
	err := ioutil.WriteFile("output.txt", b, 0644)
	if err != nil {
		panic(err)
	}
}

func DirectWrite(path string, blocks []byte) (*os.File, error) {
	out, err := directio.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0666)
	common.Check(err)
	if err != nil {
		return nil, err
	}
	_, err = out.Write(blocks)
	common.Check(err)
	if err != nil {
		return nil, err
	}
	return out, nil
}
