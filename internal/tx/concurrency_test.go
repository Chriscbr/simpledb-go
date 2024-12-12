package tx_test

import (
	"fmt"
	"os"
	"simpledb/internal/file"
	"simpledb/internal/server"
	"sync"
	"testing"
	"time"
)

func TestConcurrency(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("concurrencytest")
	})

	db, err := server.NewSimpleDBWithConfig("concurrencytest", 400, 8)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	errChan := make(chan error, 3)
	go runTx(db, &wg, errChan, runTxA)
	go runTx(db, &wg, errChan, runTxB)
	go runTx(db, &wg, errChan, runTxC)
	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	}
}

func runTx(db *server.SimpleDB, wg *sync.WaitGroup, errChan chan error, txFunc func(db *server.SimpleDB) error) {
	defer wg.Done()
	errChan <- txFunc(db)
}

func runTxA(db *server.SimpleDB) error {
	tx, err := db.NewTx()
	if err != nil {
		return fmt.Errorf("Failed to create transaction: %v", err)
	}

	blk1 := file.NewBlockID("testfile", 1)
	if err := tx.Pin(blk1); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}
	blk2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(blk2); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}

	fmt.Println("Tx A: request slock 1")
	if _, err := tx.GetInt(blk1, 0); err != nil {
		return fmt.Errorf("Failed to get int: %v", err)
	}
	fmt.Println("Tx A: got slock 1")
	time.Sleep(1000 * time.Millisecond)

	fmt.Println("Tx A: request slock 2")
	if _, err := tx.GetInt(blk2, 0); err != nil {
		return fmt.Errorf("Failed to get int: %v", err)
	}
	fmt.Println("Tx A: got slock 2")
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Failed to commit: %v", err)
	}
	fmt.Println("Tx A: committed")
	return nil
}

func runTxB(db *server.SimpleDB) error {
	tx, err := db.NewTx()
	if err != nil {
		return fmt.Errorf("Failed to create transaction: %v", err)
	}

	blk1 := file.NewBlockID("testfile", 1)
	if err := tx.Pin(blk1); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}
	blk2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(blk2); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}

	fmt.Println("Tx B: request xlock 2")
	if err := tx.SetInt(blk2, 0, 0, false); err != nil {
		return fmt.Errorf("Failed to set int: %v", err)
	}
	fmt.Println("Tx B: got xlock 2")
	time.Sleep(1000 * time.Millisecond)

	fmt.Println("Tx B: request slock 1")
	if _, err := tx.GetInt(blk1, 0); err != nil {
		return fmt.Errorf("Failed to get int: %v", err)
	}
	fmt.Println("Tx B: got slock 1")
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Failed to commit: %v", err)
	}
	fmt.Println("Tx B: committed")
	return nil
}

func runTxC(db *server.SimpleDB) error {
	tx, err := db.NewTx()
	if err != nil {
		return fmt.Errorf("Failed to create transaction: %v", err)
	}

	blk1 := file.NewBlockID("testfile", 1)
	if err := tx.Pin(blk1); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}
	blk2 := file.NewBlockID("testfile", 2)
	if err := tx.Pin(blk2); err != nil {
		return fmt.Errorf("Failed to pin block: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	fmt.Println("Tx C: request xlock 1")
	if err := tx.SetInt(blk1, 0, 0, false); err != nil {
		return fmt.Errorf("Failed to set int: %v", err)
	}
	fmt.Println("Tx C: got xlock 1")
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("Tx C: request slock 2")
	if _, err := tx.GetInt(blk2, 0); err != nil {
		return fmt.Errorf("Failed to get int: %v", err)
	}
	fmt.Println("Tx C: got slock 2")
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Failed to commit: %v", err)
	}
	fmt.Println("Tx C: committed")
	return nil
}
