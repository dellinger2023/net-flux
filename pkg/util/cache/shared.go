package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"time"

	"github.com/dellinger2023/net-flux/pkg/logger"
	"github.com/dellinger2023/net-flux/pkg/util"
	"github.com/dellinger2023/net-flux/pkg/util/redis"
	goredis "github.com/go-redis/redis/v8"
)

const (
	sharedKeyPrefix  = "netflux:cache:"
	defaultSharedTTL = redis.DefaultExpiration
)

type redisCache struct {
	prefix string
	cli    *redis.Client
}

func NewSharedCache(cli *redis.Client, prefix string) (Cache, error) {
	if cli == nil {
		return nil, errors.New("redis client is nil")
	}

	if err := cli.Ping(context.Background()); err != nil {
		return nil, errors.New("failed to ping redis server")
	}

	if util.IsEmptyStr(prefix) {
		prefix = sharedKeyPrefix
	}

	return &redisCache{cli: cli, prefix: prefix}, nil
}

func NewSharedCacheWithAddress(opt *redis.Options, prefix string) (Cache, error) {
	cli, err := redis.New(opt)
	if err != nil {
		logger.Errorf("failed to create redis client: %v", err)
		return nil, err
	}
	return NewSharedCache(cli, prefix)
}

func (c *redisCache) Get(key string) (any, error) {
	ctx := context.Background()
	raw, err := c.cli.Get(ctx, c.fullKey(key))
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return gobDecode([]byte(raw))
}

func (c *redisCache) Set(key string, value any) error {
	return c.SetWithTTL(key, value, defaultSharedTTL)
}

func (c *redisCache) SetWithTTL(key string, value any, ttl time.Duration) error {
	data, err := gobEncode(value)
	if err != nil {
		return err
	}
	if ttl < 0 {
		ttl = 0
	}
	return c.cli.Set(context.Background(), c.fullKey(key), data, ttl)
}

func (c *redisCache) Delete(key string) error {
	_, err := c.cli.Del(context.Background(), c.fullKey(key))
	return err
}

func (c *redisCache) Close() error {
	if c == nil || c.cli == nil {
		return nil
	}
	return c.cli.Close()
}

func (c *redisCache) Length() int {
	return 0
}

func (c *redisCache) fullKey(key string) string {
	return c.prefix + key
}

func gobEncode(value any) ([]byte, error) {
	if value != nil {
		// gob 通过 interface{} 编解码时需注册具体类型
		gob.Register(value)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(data []byte) (any, error) {
	var value any
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}
