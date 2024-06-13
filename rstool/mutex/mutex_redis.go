package mutex

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Locker = (*RedisMutex)(nil)

type RedisMutex struct {
	client *redis.Client
}

type RedisConfig struct {
	Addr     string
	Username string
	Password string
	DB       int
}

func NewRedisMutex(ctx context.Context, cfg *RedisConfig) (Locker, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &RedisMutex{
		client: client,
	}, nil
}

func NewRedisMutexWithClient(client *redis.Client) (Locker, error) {
	return &RedisMutex{
		client: client,
	}, nil
}

// Lock implements Locker.
func (r *RedisMutex) Lock(key string) error {
	panic("unimplemented")
}

// TryUnlock implements Locker.
func (r *RedisMutex) TryUnlock(key string) (bool, error) {
	panic("unimplemented")
}

// Unlock implements Locker.
func (r *RedisMutex) Unlock(key string) error {
	panic("unimplemented")
}
