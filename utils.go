package newdb

import (
	"reflect"
)

// Sizeof t's sizeof
func Sizeof(t interface{}) uintptr {
	return reflect.TypeOf(t).Size()
}

func assertEqual(a, b interface{}) {
	// TODO: implement
	logger := log.WithField("name", "assert")
	logger.Debug(a, b)
}
