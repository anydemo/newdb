package main

import (
	"fmt"
	"log"
	"os"

	"github.com/anydemo/newdb"
)

func main() {
	types := []*newdb.Type{newdb.IntType, newdb.IntType, newdb.IntType}
	names := []string{"field0", "field1", "field2"}
	td := newdb.NewTupleDesc(types, names)
	dbpath := "data/seqscan.data"
	file, err := os.Create(dbpath)
	if err != nil {
		panic(fmt.Errorf("create file err: %v", err))
	}
	table1 := newdb.NewHeapFile(file, td)
	newdb.DB.C().AddTable(table1, "seqscan_table")
	txID := newdb.NewTxID()

	// add some tuples
	tuple := &newdb.Tuple{
		TD:     td,
		Fields: []newdb.Field{newdb.NewIntField(9), newdb.NewIntField(8), newdb.NewIntField(7)},
	}
	err = newdb.DB.B().InsertTuple(txID, table1.ID(), tuple)
	if err != nil {
		panic(fmt.Errorf("insert tuple err: %v", err))
	}

	// real SeqScan
	seq := newdb.NewSeqScan(txID, table1.ID(), "seqscan")
	err = seq.Open()
	if err != nil {
		panic(fmt.Errorf("open SeqScan err: %v", err))
	}
	log.Println("TupleDesc", td.String())
	for seq.HasNext() {
		tuple := seq.Next()
		if seq.Error() != nil {
			panic(fmt.Errorf("seq next err: %v", seq.Error()))
		}
		log.Printf("%v \n", tuple.String())
	}
}
