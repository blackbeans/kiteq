package mysql

import (
	"database/sql"
	"log"
	"testing"
	"time"
)

func TestStmtPool(t *testing.T) {

	// dropsql := "drop database if exists `test`;"
	// createsql :=
	// 	"create table `test_a` ( " +
	// 		"id int(10) primary key auto_increment ," +
	// 		"username char(20)" +
	// 		");"

	// db, _ := dbcreate("root:@tcp(localhost:3306)/test")
	// _, err := db.Exec(dropsql)
	// t.Errorf("%s", err)

	// db.Exec("create database test;")
	// db.Exec(createsql)
	// db.Close()

	db, _ := dbcreate("root:@tcp(localhost:3306)/test")
	//建表
	db.Exec("insert `test_a`(username) values('a')")

	preSql := "update `test_a` set username=? where id=?"

	err, p := NewStmtPool(10, 10, 20, 10*time.Second, func() (error, *sql.Stmt) {
		prepare, err := db.Prepare(preSql)
		return err, prepare
	})

	if nil != err {
		t.Fail()
		t.Logf("NewStmtPool|FAIL|%s\n", err)
		return
	}

	row, err := db.Query("select * from test_a where username=?", "a")
	if nil != err {
		t.Logf("Query DB |FAIL|%s\n", err)
		t.Fail()
		return
	}

	if row.Next() {
		var id int
		var username string

		err := row.Scan(&id, &username)
		if nil != err {
			t.Fail()
			t.Logf("db|QUERY|Scan |FAIL|%s\n", err)
			return
		}

		if username != "a" {
			t.Fail()
			t.Logf("db|QUERY|username is not a |FAIL\n")
			return
		}
	} else {
		t.Fail()
		t.Logf("db|QUERY|FAIL|%s\n", err)
	}

	err, stmt := p.Get()
	if nil != err {
		t.Fail()
		t.Logf("Get Stmt |FAIL|%s\n", err)
		return
	}

	stmt.Exec("b", 1)
	p.Release(stmt)

	row, err = db.Query("select * from test_a where username=?", "b")
	if nil != err {
		t.Logf("Query Stmt |FAIL|%s\n", err)
		t.Fail()
		return
	}

	if row.Next() {
		var id int
		var username string

		err := row.Scan(&id, &username)
		if nil != err {
			t.Fail()
			t.Logf("Query Stmt|Scan |FAIL|%s\n", err)
			return
		}

		if username != "b" || id != 1 {
			t.Fail()
			t.Logf("Query Stmt|username is not a |FAIL\n")
			return
		}
	} else {
		t.Fail()
		t.Logf("Query Stmt|username is not a |FAIL\n")
	}

	p.Shutdown()
}

func dbcreate(addr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", addr)
	if err != nil {
		log.Panicf("NewKiteMysql|CONNECT FAIL|%s|%s\n", err, addr)
	}

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)
	return db, nil
}
