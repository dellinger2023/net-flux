package cache

import (
	"sync"
	"time"
)

const (
	defaultTTL = 24 * time.Hour
	interval   = 10 * time.Second
)

type inMemCache struct {
	mu      sync.RWMutex
	cache   map[string]*itemWithTTL
	closeCh chan struct{}
	closed  bool
}

func NewInMemCache() Cache {
	c := &inMemCache{
		cache:   make(map[string]*itemWithTTL),
		closeCh: make(chan struct{}),
	}
	go c.periodicCleanup()
	return c
}

func (c *inMemCache) Get(key string) (any, error) {
	c.mu.RLock()
	item, ok := c.cache[key]
	c.mu.RUnlock()
	if !ok || item == nil {
		return nil, nil
	}
	if !item.expiration.IsZero() && time.Now().After(item.expiration) {
		_ = c.Delete(key)
		return nil, nil
	}
	return item.value, nil
}

func (c *inMemCache) Set(key string, value any) error {
	return c.SetWithTTL(key, value, defaultTTL)
}

func (c *inMemCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, key)
	return nil
}

func (c *inMemCache) SetWithTTL(key string, value any, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.cache[key] = &itemWithTTL{
		value:      value,
		expiration: time.Now().Add(ttl),
	}
	return nil
}

func (c *inMemCache) periodicCleanup() {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.mu.Lock()
			for key, value := range c.cache {
				if !value.expiration.IsZero() && now.After(value.expiration) {
					delete(c.cache, key)
				}
			}
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

func (c *inMemCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closeCh)
	c.cache = make(map[string]*itemWithTTL)
	return nil
}

func (c *inMemCache) Length() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}
