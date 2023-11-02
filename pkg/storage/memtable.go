package storage

import (
	"sync"

	"github.com/BornikReal/storage-component/pkg/iterator"
	"github.com/BornikReal/storage-component/pkg/kv_file"
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

type KVFile interface {
	WriteKV(kv kv_file.KV) error
	Read(batch int64) (kv_file.KV, bool, error)
	Clear() error
}

type MemTable struct {
	mu        sync.RWMutex
	storage   Tree
	ssManager SSManager
	wal       KVFile

	maxSize int
}

func NewMemTable(tree Tree, ssManager SSManager, wal KVFile, maxSize int) *MemTable {
	return &MemTable{
		storage:   tree,
		maxSize:   maxSize,
		ssManager: ssManager,
		wal:       wal,
	}
}

func (i *MemTable) Init() error {
	for {
		kv, ok, err := i.wal.Read(1)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		i.storage.Put(kv.Key, kv.Value)
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

	if err := i.wal.WriteKV(kv_file.KV{
		Key:   key,
		Value: value,
	}); err != nil {
		return err
	}

	i.storage.Put(key, value)

	if i.storage.Size() >= i.maxSize {
		if err := i.ssManager.SaveTree(i.storage.Iterator()); err != nil {
			return err
		}
		i.storage.Clear()
		if err := i.wal.Clear(); err != nil {
			return err
		}
	}
	return nil
}
