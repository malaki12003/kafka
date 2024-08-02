package kafkautils

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFileLock_Lock(t *testing.T) {
	filePath := filepath.Join(os.TempDir(), "test_lock_file")
	lock1, err := NewFileLock(filePath)
	assert.NoError(t, err)

	defer lock1.Destroy()

	// First lock should succeed
	log.Println("Trying to acquire first lock")
	err = lock1.Lock()
	assert.NoError(t, err)
	log.Println("First lock acquired")

	// Attempting to acquire second lock
	lock2, err := NewFileLock(filePath)
	assert.NoError(t, err)

	// Wait for a timeout if it takes too long to lock
	done := make(chan error, 1)
	go func() {
		log.Println("Trying to acquire second lock")
		done <- lock2.Lock()
	}()

	select {
	case err := <-done:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout occurred while acquiring lock")
		log.Println("Second lock failed due to timeout as expected")
	case <-time.After(6 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestFileLock_TryLock(t *testing.T) {
	filePath := filepath.Join(os.TempDir(), "test_trylock_file")
	lock1, err := NewFileLock(filePath)
	assert.NoError(t, err)

	defer lock1.Destroy()

	// First TryLock should succeed
	log.Println("Trying to acquire first TryLock")
	success, err := lock1.TryLock()
	assert.NoError(t, err)
	assert.True(t, success)
	log.Println("First TryLock acquired")

	// Second TryLock should fail
	lock2, err := NewFileLock(filePath)
	assert.NoError(t, err)

	log.Println("Trying to acquire second TryLock")
	success, err = lock2.TryLock()
	assert.NoError(t, err)
	assert.False(t, success)
	log.Println("Second TryLock failed as expected")
}

func TestFileLock_Destroy(t *testing.T) {
	filePath := filepath.Join(os.TempDir(), "test_destroy_file")
	lock1, err := NewFileLock(filePath)
	assert.NoError(t, err)

	// Lock and then destroy
	log.Println("Trying to acquire lock for destroy test")
	err = lock1.Lock()
	assert.NoError(t, err)
	log.Println("Lock acquired for destroy test")

	err = lock1.Destroy()
	assert.NoError(t, err)

	// Ensure file is deleted
	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
	log.Println("File successfully destroyed and removed")
}
