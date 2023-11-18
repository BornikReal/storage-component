package storage

import (
	"github.com/BornikReal/storage-component/pkg/ss_storage/kv_file"
	"sync"
)

type KVFile interface {
	WriteKV(kv kv_file.KV) error
	Read(batch int64) (kv_file.KV, bool, error)
	Clear() error
}

type MemTableWithWal struct {
	mu       sync.RWMutex
	memtable *MemTableWithSS
	wal      KVFile
}

func NewMemTableWithWal(memtable *MemTableWithSS, wal KVFile) *MemTableWithWal {
	return &MemTableWithWal{
		memtable: memtable,
		wal:      wal,
	}
}

func (i *MemTableWithWal) Init() error {
	for {
		kv, ok, err := i.wal.Read(1)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err = i.memtable.Set(kv.Key, kv.Value); err != nil {
			return err
		}
	}
}

func (i *MemTableWithWal) Get(key string) (string, error) {
	if i.memtable == nil {
		return "", NotInitError
	}

	return i.memtable.Get(key)
}

func (i *MemTableWithWal) Set(key string, value string) error {
	if i.memtable == nil {
		return NotInitError
	}

	i.mu.Lock()
	if err := i.wal.WriteKV(kv_file.KV{
		Key:   key,
		Value: value,
	}); err != nil {
		return err
	}
	i.mu.Unlock()

	return i.memtable.Set(key, value)
}
