package storage

import "errors"

type SSGetter interface {
	Get(key string) (string, bool, error)
}

type MemTableWithSS struct {
	memtable *MemTable
	ssGetter SSGetter
}

func NewMemTableWithSS(memtable *MemTable, ssGetter SSGetter) *MemTableWithSS {
	return &MemTableWithSS{
		memtable: memtable,
		ssGetter: ssGetter,
	}
}

func (i *MemTableWithSS) Get(key string) (string, error) {
	if i.memtable == nil {
		return "", NotInitError
	}

	value, err := i.memtable.Get(key)
	if errors.Is(err, NotFoundError) {
		var ok bool
		value, ok, err = i.ssGetter.Get(key)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", NotFoundError
		}
		return value, nil
	} else if err != nil {
		return "", err
	}

	return value, nil
}

func (i *MemTableWithSS) Set(key string, value string) error {
	if i.memtable == nil {
		return NotInitError
	}

	return i.memtable.Set(key, value)
}
