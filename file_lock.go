package kafkautils

import (
	"fmt"
	"os"
	"time"

	"github.com/juju/fslock"
)

// FileLock represents a file lock
type FileLock struct {
	lock     *fslock.Lock
	filePath string
}

// NewFileLock creates a new FileLock
func NewFileLock(filePath string) (*FileLock, error) {
	lock := fslock.New(filePath)

	// Check if the file exists; if not, create it
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file: %w", err)
		}
		file.Close()
	}

	return &FileLock{
		lock:     lock,
		filePath: filePath,
	}, nil
}

// Lock locks the file or returns an error if the lock is already held
func (fl *FileLock) Lock() error {
	ch := make(chan error, 1)

	go func() {
		err := fl.lock.Lock()
		ch <- err
	}()

	select {
	case err := <-ch:
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
		return nil
	case <-time.After(3 * time.Second):
		return fmt.Errorf("timeout occurred while acquiring lock")
	}
}

// TryLock tries to lock the file and returns true if the locking succeeds
func (fl *FileLock) TryLock() (bool, error) {
	err := fl.lock.TryLock()
	if err == nil {
		return true, nil
	}
	if err == fslock.ErrLocked {
		return false, nil
	}
	return false, fmt.Errorf("failed to acquire lock: %w", err)
}

// Unlock unlocks the file lock if it is held
func (fl *FileLock) Unlock() error {
	err := fl.lock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

// Destroy unlocks the file and removes it
func (fl *FileLock) Destroy() error {
	if err := fl.Unlock(); err != nil {
		return err
	}
	if err := os.Remove(fl.filePath); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}
