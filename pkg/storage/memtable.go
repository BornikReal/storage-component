package storage

import (
	"fmt"
	"github.com/BornikReal/storage-component/pkg/ss_storage/iterator"
	"github.com/BornikReal/storage-component/pkg/tree_with_clone"
	"sync"
)

type MemTable struct {
	mu      sync.RWMutex
	storage tree_with_clone.Tree
	dumper  chan iterator.Iterator
	//ssManager SSSaver

	maxSize int
}

func NewMemTable(tree tree_with_clone.Tree, dumper chan iterator.Iterator, maxSize int) *MemTable {
	return &MemTable{
		storage: tree,
		dumper:  dumper,
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
		return "", fmt.Errorf("expected string, but got %T", valueStr)
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
		i.dumper <- i.storage.Clone().Iterator()
	}
	return nil
}
