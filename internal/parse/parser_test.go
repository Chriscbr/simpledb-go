package parse

import (
	"fmt"
	"testing"
)

func TestParserQuery(t *testing.T) {
	stmts := []string{
		"SELECT col1 FROM table1",
		"SELECT col1, col2 FROM table1, table2",
		"SELECT col1 FROM table1 WHERE col1 = 1",
		"SELECT col1 FROM table1 WHERE col1 = 'value' AND col2 = 42",
		"SELECT col1 FROM table1 WHERE col1 = col2",
		"SELECT col1, col2 FROM table1 WHERE col1 = 'value' AND col2 = col1",
	}
	for _, stmt := range stmts {
		lexer := NewLexer(stmt)
		parser := NewParser(lexer)
		query, err := parser.Query()
		if err != nil {
			t.Fatalf("case %s: expected nil, got %v", stmt, err)
		}
		if query.String() != stmt {
			t.Fatalf("case %s: expected %s, got %s", stmt, stmt, query.String())
		}
	}
}

func TestParserUpdate(t *testing.T) {
	stmts := []string{
		"INSERT INTO table1 (col1) VALUES ('value1')",
		"INSERT INTO table1 (col1, col2) VALUES (1, 'value2')",
		"INSERT INTO table1 (col1, col2) VALUES (1, 2)",
		"DELETE FROM table1",
		"DELETE FROM table1 WHERE col1 = 1",
		"DELETE FROM table1 WHERE col1 = 'value1' AND col2 = 42",
		"UPDATE table1 SET col1 = 42",
		"UPDATE table1 SET col1 = 'updated value' WHERE col2 = 99",
		"UPDATE table1 SET col1 = col2 WHERE col3 = 'text'",
		"CREATE TABLE table1 (col1 INT)",
		"CREATE TABLE table1 (col1 INT, col2 VARCHAR(100))",
		"CREATE TABLE table1 (col1 VARCHAR(50), col2 INT)",
		"CREATE TABLE table1 (col1 VARCHAR(0))",
		"CREATE TABLE table1 (col1 VARCHAR(50), col2 INT, col3 VARCHAR(50))",
		"CREATE VIEW view1 AS SELECT col1 FROM table1",
		"CREATE VIEW view2 AS SELECT col1, col2 FROM table1 WHERE col1 = 'value'",
		"CREATE INDEX index1 ON table1 (col1)",
		"CREATE INDEX index2 ON table1 (col2)",
	}
	for _, stmt := range stmts {
		lexer := NewLexer(stmt)
		parser := NewParser(lexer)
		cmd, err := parser.UpdateCmd()
		if err != nil {
			t.Fatalf("case %s: expected nil, got %v", stmt, err)
		}
		obj, ok := cmd.(fmt.Stringer)
		if !ok {
			t.Fatalf("case %s: expected UpdateCmd to return a stringer", stmt)
		}
		if obj.String() != stmt {
			t.Fatalf("case %s: expected %s, got %s", stmt, stmt, obj.String())
		}
	}
}
