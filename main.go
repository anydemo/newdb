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

	"os"

	hparser "github.com/anydemo/newdb/parser"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	log "github.com/sirupsen/logrus"
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
	// var sql = "SELECT /*+ TIDB_SMJ(employees) */ emp_no, first_name, last_name " +
	// 	"FROM employees USE INDEX (last_name) " +
	// 	"where last_name='Aamodt' and gender='F' and birth_date > '1960-01-01'"

	var sql = `select * from tbl where id = 1`

	var parser = parser.New()
	stmtNodes, warns, err := parser.Parse(sql, "", "")
	if err != nil {
		log.Printf("parse error:\n%v\n%s", err, sql)
		os.Exit(2)
	}
	for _, warn := range warns {
		log.Printf("warn: %v", warn)
	}
	v := hparser.WalkVisitor{}
	for _, stmtNode := range stmtNodes {
		stmtNode.Accept(&v)
	}
}

func main() {
	// cmd.Execute()
	newDB()
	sqlParser()
}
