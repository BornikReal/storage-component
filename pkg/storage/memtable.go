package storage

import "sync"

type Tree interface {
	Get(key interface{}) (value interface{}, found bool)
	Put(key interface{}, value interface{})
	Size() int
}

type MemTable struct {
	mu      sync.RWMutex
	storage Tree

	maxSize int
}

func NewMemTable(tree Tree, maxSize int) *MemTable {
	return &MemTable{
		storage: tree,
		maxSize: maxSize,
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
		return "", NotFoundError
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

	}
	return nil
}

//func (i *MemTable) saveInSSTable() error {
//
//}
