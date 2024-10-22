package concurrency

import (
	"fmt"
	"log"
	"math/rand"
	"simpledb/internal/file"
	"sync"
	"testing"
	"time"
)

// Function to simulate a transaction trying to acquire and release locks
func simulateTransaction(id int, lt *LockTable, blk file.BlockID, wg *sync.WaitGroup) {
	defer wg.Done()
	rand.Seed(time.Now().UnixNano())

	// Randomly decide whether to request a shared or exclusive lock
	if rand.Intn(2) == 0 {
		// Try acquiring a shared lock
		log.Printf("Transaction %d: Trying to acquire SLock on Block %v\n", id, blk)
		err := lt.SLock(blk)
		if err != nil {
			log.Printf("Transaction %d: Failed to acquire SLock on Block %v: %v\n", id, blk, err)
			return
		}
		log.Printf("Transaction %d: Acquired SLock on Block %v\n", id, blk)

		// Simulate work with the lock
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

		// Release the lock
		lt.Unlock(blk)
		log.Printf("Transaction %d: Released SLock on Block %v\n", id, blk)

	} else {
		// Try acquiring an exclusive lock
		log.Printf("Transaction %d: Trying to acquire XLock on Block %v\n", id, blk)
		err := lt.XLock(blk)
		if err != nil {
			log.Printf("Transaction %d: Failed to acquire XLock on Block %v: %v\n", id, blk, err)
			return
		}
		log.Printf("Transaction %d: Acquired XLock on Block %v\n", id, blk)

		// Simulate work with the lock
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

		// Release the lock
		lt.Unlock(blk)
		log.Printf("Transaction %d: Released XLock on Block %v\n", id, blk)
	}
}

func TestLockTable(t *testing.T) {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	// Initialize the lock table
	lt := NewLockTable()

	// Define the block IDs that transactions will lock
	blockIds := []file.BlockID{
		{Filename: "file1", Blknum: 1},
		{Filename: "file2", Blknum: 2},
		{Filename: "file3", Blknum: 3},
	}

	// Use a WaitGroup to wait for all transactions to complete
	var wg sync.WaitGroup

	// Number of transactions to simulate
	numTransactions := 100

	for i := 1; i <= numTransactions; i++ {
		wg.Add(1)
		blk := blockIds[rand.Intn(len(blockIds))] // Randomly choose a block to lock
		go simulateTransaction(i, lt, blk, &wg)
	}

	// Wait for all transactions to complete
	wg.Wait()

	fmt.Println("Stress test completed!")
}
