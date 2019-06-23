package newdb

import (
	"math/rand"
	"reflect"
	"time"
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

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Seed defaultRand.Seed
func Seed(seed int64) {
	seededRand.Seed(seed)
}

//RandString rand string with len
func RandString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
