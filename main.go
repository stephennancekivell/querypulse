package main

import (
	"database/sql"
	"time"

	// "database/sql/driver"
	"fmt"
	"log"

	_ "github.com/lib/pq" // add this

	"github.com/stephennancekivell/go-sql-slow-log/driver2"
)

func main() {
	fmt.Println("hello world!")

	driverName, err := driver2.Register("postgres")
	if err != nil {
		panic(err)
	}

	for _, d := range sql.Drivers() {
		fmt.Printf("d %v\n", d)
	}

	connStr := "postgresql://test:test@localhost/test?sslmode=disable"
	// Connect to database
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	rows, err := db.Query("select * from pgbench_accounts limit $1", 100)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	fmt.Printf("Query took %v\n", (time.Now().Sub(start)))
	start = time.Now()
	for rows.Next() {
		var id int
		rows.Scan(&id)
	}
	fmt.Printf("rows.Next took %v\n", (time.Now().Sub(start)))

	// dbc := callbackdb.Make(db, onSuccess)

	// _, err = dbc.Query("select * from users")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	//dbx := sqlx.NewDb(db, "postgres")

}

func onSuccess() {
	fmt.Println("call")
}
