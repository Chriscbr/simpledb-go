package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"simpledb/internal/record"
	simpledb "simpledb/internal/server"
	"strings"
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

	tx, err := db.NewTx()
	if err != nil {
		fmt.Println("error creating transaction:", err)
		return
	}

	fmt.Printf("Type 'quit' or 'exit' to exit.\n> ")
	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		line := input.Text()
		if strings.TrimSpace(line) == "" {
			fmt.Printf("> ")
			continue
		}
		if line == "quit" || line == "exit" {
			break
		}
		if strings.HasPrefix(strings.ToLower(line), "select") {
			plan, err := db.Planner.CreateQueryPlan(line, tx)
			if err != nil {
				fmt.Println("error creating query plan:", err)
				continue
			}
			scan, err := plan.Open()
			if err != nil {
				fmt.Println("error opening query plan:", err)
				continue
			}
			printHeader(plan.Schema())
			for scan.Next() {
				err := printRecord(plan.Schema(), scan)
				if err != nil {
					fmt.Println("error printing record:", err)
				}
			}
			scan.Close()
		} else {
			rows, err := db.Planner.ExecuteUpdate(line, tx)
			if err != nil {
				fmt.Println("error creating update plan:", err)
			}
			fmt.Printf("updated %d rows\n", rows)
			err = tx.Commit()
			if err != nil {
				fmt.Println("error committing transaction:", err)
			}
		}
		fmt.Printf("> ")
	}
}

func printHeader(schema *record.Schema) {
	if len(schema.Fields) == 0 {
		fmt.Println("(empty table)")
		return
	}
	fmt.Printf("(")
	for i, field := range schema.Fields {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%s", field)
	}
	fmt.Println(")")
}

func printRecord(schema *record.Schema, scan record.Scan) error {
	var line strings.Builder
	line.WriteString("(")
	for i, field := range schema.Fields {
		if i > 0 {
			line.WriteString(", ")
		}
		val, err := scan.GetVal(field)
		if err != nil {
			return err
		} else {
			line.WriteString(fmt.Sprintf("%v", val))
		}
	}
	line.WriteString(")")
	fmt.Println(line.String())
	return nil
}
