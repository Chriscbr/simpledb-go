package main

import (
	"fmt"
	"os"
	"path/filepath"
	simpledb "simpledb/internal/server"
)

const workspaceDir = "./data"

func main() {
	err := os.MkdirAll(workspaceDir, 0755)
	if err != nil {
		fmt.Println("error initializing database:", err)
		return
	}

	dbDir := filepath.Join(workspaceDir, "testdb")
	db, err := simpledb.NewSimpleDB(dbDir)
	if err != nil {
		fmt.Println("error initializing database:", err)
		return
	}
	defer db.Close()

	fmt.Println("database initialized")
}
