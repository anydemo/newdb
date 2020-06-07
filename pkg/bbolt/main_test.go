package bbolt

import (
	"fmt"
	"log"
	"testing"

	bolt "go.etcd.io/bbolt"
)

var (
	db *bolt.DB

	bucket = []byte("MyBucket")
)

func init() {
	var err error
	db, err = bolt.Open("test.db", 0600, nil)
	if err != nil {
		log.Panic(err)
	}
}

func Test_ReadWriteTx(t *testing.T) {
	if err := db.Update(func(tx *bolt.Tx) error {
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func Test_ReadOnlyTx(t *testing.T) {
	if err := db.View(func(tx *bolt.Tx) error {
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func Test_BatchReadWriteTx(t *testing.T) {
	if err := db.Batch(func(tx *bolt.Tx) error {
		return nil
	}); err != nil {
		t.Error(err)
	}
}

func Test_ManualTx(t *testing.T) {
	tx, err := db.Begin(true)
	if err != nil {
		t.Error(err)
		return
	}
	defer tx.Rollback()

	// Use the transaction...
	_, err = tx.CreateBucketIfNotExists(bucket)
	if err != nil {
		t.Error(err)
		return
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		t.Error(err)
		return
	}
}

func Test_Op(t *testing.T) {
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		err := b.Put([]byte("answer1"), []byte("41"))
		err = b.Put([]byte("answer2"), []byte("42"))
		return err
	}); err != nil {
		t.Error(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v := b.Get([]byte("answer"))
		fmt.Printf("The answer is: %s\n", v)
		return nil
	}); err != nil {
		t.Error(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("MyBucket"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(bucket)

		b.ForEach(func(k, v []byte) error {
			fmt.Printf("key=%s, value=%s\n", k, v)
			return nil
		})
		return nil
	}); err != nil {
		t.Error(err)
	}
}
