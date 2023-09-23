package storage

import "sync"

type InMemoryStorage struct {
	mu      sync.RWMutex
	storage map[string]string
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		storage: make(map[string]string),
	}
}

func (i *InMemoryStorage) Get(key string) (string, error) {
	i.mu.RLock()
	defer i.mu.Unlock()

	if i.storage == nil {
		return "", NotInitError
	}
	value, ok := i.storage[key]
	if !ok {
		return "", NotFoundError
	}
	return value, nil
}

func (i *InMemoryStorage) Set(key string, value string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.storage == nil {
		return NotInitError
	}
	i.storage[key] = value
	return nil
}
