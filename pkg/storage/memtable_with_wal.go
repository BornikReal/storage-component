package storage

import (
	"github.com/BornikReal/storage-component/pkg/ss_storage/kv_file"
	"sync"
)

type KVFile interface {
	Init() error
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
	//for {
	//	kv, ok, err := i.wal.Read(1)
	//	if err != nil {
	//		return err
	//	}
	//	if !ok {
	//		return nil
	//	}
	//	if err = i.memtable.Set(kv.Key, kv.Value); err != nil {
	//		return err
	//	}
	//}

	res, err := i.GetWalElements(false)
	if err != nil || res == nil {
		return err
	}
	for k, v := range res {
		if err = i.memtable.Set(k, v); err != nil {
			return err
		}
	}

	return nil
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

func (i *MemTableWithWal) GetWalElements(init bool) (map[string]string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if init {
		if err := i.wal.Init(); err != nil {
			return nil, err
		}
	}

	walEl := make(map[string]string)

	for {
		kv, ok, err := i.wal.Read(1)
		if err != nil {
			return nil, err
		}
		if !ok {
			return walEl, nil
		}
		walEl[kv.Key] = kv.Value
	}
}
