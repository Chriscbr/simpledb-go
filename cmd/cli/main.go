package main

import (
	"fmt"
	"os"
	"path/filepath"
	simpledb "simpledb/internal/server"
)

const workspaceDir = "./data"

func main() {
	fmt.Println("Hello, World!")
	err := os.MkdirAll(workspaceDir, 0755)
	if err != nil {
		fmt.Println("error initializing database:", err)
		return
	}

	dbDir := filepath.Join(workspaceDir, "testdb")
	db, err := simpledb.NewSimpleDB(dbDir, 400, 10)
	if err != nil {
		fmt.Println("error initializing database:", err)
		return
	}
	defer db.Close()

	fmt.Println("database initialized")
}
