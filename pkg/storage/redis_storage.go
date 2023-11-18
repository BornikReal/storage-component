package storage

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(ctx context.Context, key string) *redis.StringCmd
}

type RedisStorage struct {
	master RedisClient
	slaves []RedisClient

	rrCounter atomic.Int64
}

func NewRedisStorage(master RedisClient, slaves []RedisClient) *RedisStorage {
	return &RedisStorage{
		master: master,
		slaves: slaves,
	}
}

func (i *RedisStorage) Get(key string) (string, error) {
	repl := i.getReplica()
	if repl == nil {
		return "", NotInitError
	}

	value, err := repl.Get(context.Background(), key).Result()
	if err == redis.Nil {
		return "", NotFoundError
	} else if err != nil {
		return "", err
	}

	return value, nil
}

func (i *RedisStorage) Set(key string, value string) error {
	if i.master == nil {
		return NotInitError
	}

	return i.master.Set(context.Background(), key, value, 0).Err()
}

func (i *RedisStorage) getReplica() RedisClient {
	if len(i.slaves) == 0 {
		return i.master
	}
	rrCounter := i.rrCounter.Add(1)
	if rrCounter > int64(len(i.slaves)) {
		i.rrCounter.Store(0)
	}
	repl := i.slaves[rrCounter-1]
	return repl
}
