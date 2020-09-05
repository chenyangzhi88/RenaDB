package common

import "fmt"

func EncodeKey(key string, version uint64) string {
	concatenated := fmt.Sprint(key, version)
	return concatenated
}
