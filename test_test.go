// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package newdb

import (
	"fmt"
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	bolt "go.etcd.io/bbolt"
)

// WalkVisitor struct
type WalkVisitor struct{}

// Enter enter the node and visit
func (v WalkVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	fmt.Printf("-> %v\n", spew.Sdump(in))
	return in, false
}

// Leave leave the node
func (v WalkVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	fmt.Printf("<- %T\n", in)
	return in, true
}

func newDB4tbolt() {
	db, err := bolt.Open("data/testBolt.db", 0666, nil)
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
}

func sqlParser(sql string) {
	// var sql = "SELECT /*+ TIDB_SMJ(employees) */ emp_no, first_name, last_name " +
	// 	"FROM employees USE INDEX (last_name) " +
	// 	"where last_name='Aamodt' and gender='F' and birth_date > '1960-01-01'"

	var parser = parser.New()
	stmtNodes, warns, err := parser.Parse(sql, "", "")
	if err != nil {
		log.Printf("parse error:\n%v\n%s", err, sql)
		os.Exit(2)
	}
	for _, warn := range warns {
		log.Printf("warn: %v", warn)
	}
	v := WalkVisitor{}
	for _, stmtNode := range stmtNodes {
		stmtNode.Accept(&v)
	}
}

func TestSimpleParsetTest(t *testing.T) {
	// cmd.Execute()
	// newDB4tbolt()
	// sqlParser(`select * from tbl where id = 1`)
	// sqlParser(`drop table tdl`)
	// sqlParser(`select * from tdl where a LIKE '_%'`)
	// sqlParser(`insert into tld(id)values('id')`)
	// sqlParser(`create index ID on student(ID)`)
	// sqlParser(`select * from tlb where id = ?`)
	var defaultPageSize = os.Getpagesize()
	t.Log(defaultPageSize)
}
