package cache

import "time"

type Cache interface {
	Get(key string) (any, error)
	Set(key string, value any) error
	SetWithTTL(key string, value any, ttl time.Duration) error
	Delete(key string) error
	Close() error
	Length() int
}

type LRUCache interface {
	Get(key string) (any, error)
	Set(key string, value any) error
	Delete(key string) error
	Close() error
	Length() int
}

type itemWithTTL struct {
	value      any
	expiration time.Time
}
