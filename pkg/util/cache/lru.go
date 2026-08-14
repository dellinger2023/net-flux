package cache

import (
	"container/list"
	"sync"
)

const defaultLRULimit = 1024

type lruEntry struct {
	key   string
	value any
}

type lruCache struct {
	mu      sync.Mutex
	cache   map[string]*list.Element
	ll      *list.List
	limit   int
	closed  bool
}

// NewLRUCache 创建容量为 limit 的 LRU 缓存；limit <= 0 时使用默认容量。
func NewLRUCache(limit int) LRUCache {
	if limit <= 0 {
		limit = defaultLRULimit
	}
	return &lruCache{
		cache: make(map[string]*list.Element),
		ll:    list.New(),
		limit: limit,
	}
}

func (c *lruCache) Get(key string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil
	}

	elem, ok := c.cache[key]
	if !ok {
		return nil, nil
	}

	c.ll.MoveToFront(elem)
	return elem.Value.(*lruEntry).value, nil
}

func (c *lruCache) Set(key string, value any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if elem, ok := c.cache[key]; ok {
		c.ll.MoveToFront(elem)
		elem.Value.(*lruEntry).value = value
		return nil
	}

	elem := c.ll.PushFront(&lruEntry{key: key, value: value})
	c.cache[key] = elem

	for c.ll.Len() > c.limit {
		c.removeOldestLocked()
	}
	return nil
}

func (c *lruCache) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if elem, ok := c.cache[key]; ok {
		c.removeElementLocked(elem)
	}
	return nil
}

func (c *lruCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cache = make(map[string]*list.Element)
	c.ll = list.New()
	return nil
}

func (c *lruCache) Length() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

func (c *lruCache) removeOldestLocked() {
	elem := c.ll.Back()
	if elem != nil {
		c.removeElementLocked(elem)
	}
}

func (c *lruCache) removeElementLocked(elem *list.Element) {
	c.ll.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(c.cache, entry.key)
}
