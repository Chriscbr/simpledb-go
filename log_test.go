package simpledb

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestLogMgr(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("logtest")
	})

	db, err := NewSimpleDB("logtest", 400)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	lm := db.lm
	printLogRecords(t, lm, "The initial empty log file:")
	t.Log("done")
	createRecords(t, lm, 1, 35)
	printLogRecords(t, lm, "The log file now has these records:")
	createRecords(t, lm, 36, 70)
	lm.Flush(65)
	printLogRecords(t, lm, "The log file now has these records:")
}

func printLogRecords(t *testing.T, lm *LogMgr, msg string) {
	t.Log(msg)
	var sb strings.Builder
	for rec, err := range lm.All() {
		if err != nil {
			t.Fatal(err)
		}
		p := NewPageFromBytes(rec)
		s := p.GetString(0)
		npos := MaxLength(len(s))
		val := p.GetInt(npos)
		sb.WriteString(fmt.Sprintf("[%s, %v]\n", s, val))
	}
	t.Log(sb.String())
}

func createRecords(t *testing.T, lm *LogMgr, start int, end int) {
	var sb strings.Builder
	sb.WriteString("Creating records: ")
	for i := start; i <= end; i++ {
		rec := createLogRecord(fmt.Sprint("record", i), int32(i+100))
		lsn, err := lm.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		sb.WriteString(fmt.Sprintf("%d ", lsn))
	}
	t.Log(sb.String())
}

func createLogRecord(s string, n int32) []byte {
	spos := 0
	npos := spos + MaxLength(len(s))
	buf := make([]byte, 4+npos)
	p := NewPageFromBytes(buf)
	p.SetString(spos, s)
	p.SetInt(npos, n)
	return buf
}
