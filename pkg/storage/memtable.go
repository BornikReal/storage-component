package storage

import (
	"sync"

	"github.com/BornikReal/storage-component/pkg/iterator"
	"github.com/emirpasic/gods/containers"
)

type Tree interface {
	Get(key interface{}) (value interface{}, found bool)
	Put(key interface{}, value interface{})
	Size() int
	Iterator() containers.ReverseIteratorWithKey
	Clear()
}

type SSManager interface {
	SaveTree(it iterator.Iterator) error
	Get(key string) (string, bool, error)
}

type MemTable struct {
	mu        sync.RWMutex
	storage   Tree
	ssManager SSManager

	maxSize int
}

func NewMemTable(tree Tree, ssManager SSManager, maxSize int) *MemTable {
	return &MemTable{
		storage:   tree,
		maxSize:   maxSize,
		ssManager: ssManager,
	}
}

func (i *MemTable) Get(key string) (string, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.storage == nil {
		return "", NotInitError
	}
	value, ok := i.storage.Get(key)
	if !ok {
		valueStr, okSS, err := i.ssManager.Get(key)
		if err != nil {
			return "", err
		}
		if !okSS {
			return "", NotFoundError
		}
		return valueStr, nil
	}
	valueStr, ok := value.(string)
	if !ok {
		return "", NotStringError(valueStr)
	}

	return valueStr, nil
}

func (i *MemTable) Set(key string, value string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.storage == nil {
		return NotInitError
	}
	i.storage.Put(key, value)

	if i.storage.Size() >= i.maxSize {
		if err := i.ssManager.SaveTree(i.storage.Iterator()); err != nil {
			return err
		}
		i.storage.Clear()
	}
	return nil
}
