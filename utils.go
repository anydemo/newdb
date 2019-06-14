package newdb

import (
	"reflect"
)

// Sizeof t's sizeof
func Sizeof(t interface{}) uintptr {
	return reflect.TypeOf(t).Size()
}
