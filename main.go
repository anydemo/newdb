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

package main

import (
	// "github.com/anydemo/newdb/cmd"
	"fmt"
	"log"

	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	bolt "go.etcd.io/bbolt"
)

const (
	// DbPath bolt db file path
	DbPath = "data/db.db"
)

func newDB() {
	db, err := bolt.Open(DbPath, 0666, nil)
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
}

func sqlParser() {
	p := parser.New()

	// 2. Parse a text SQL into AST([]ast.StmtNode).
	stmtNodes, _, err := p.Parse("select * from tbl where id = 1", "", "")

	// 3. Use AST to do cool things.
	fmt.Printf("%#v %v", stmtNodes[0], err)
}

func main() {
	// cmd.Execute()
	newDB()
	sqlParser()
}
