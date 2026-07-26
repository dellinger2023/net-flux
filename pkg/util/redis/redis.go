package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	DefaultExpiration = 600 * time.Second
)

var (
	ErrNil = redis.Nil
)

// Options Redis 连接配置
type Options struct {
	Addr     string // "host:port"
	Password string
	DB       int
	PoolSize int
}

// NewOptionsFromHostPort 从 host/port 等构造 Options，便于与 config.RedisSetting 配合
func NewOptionsFromHostPort(host string, port int, password string, poolSize int) *Options {
	if poolSize <= 0 {
		poolSize = 10
	}
	return &Options{
		Addr:     net.JoinHostPort(host, strconv.Itoa(port)),
		Password: password,
		DB:       0,
		PoolSize: poolSize,
	}
}

// Client go-redis 封装，提供常用操作
type Client struct {
	cli *redis.Client
}

// New 根据 Options 创建 Redis 客户端
func New(opt *Options) (*Client, error) {
	if opt == nil {
		return nil, fmt.Errorf("redis: options is nil")
	}
	cli := redis.NewClient(&redis.Options{
		Addr:         opt.Addr,
		Password:     opt.Password,
		DB:           opt.DB,
		PoolSize:     opt.PoolSize,
		DialTimeout:  8 * time.Second,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("redis ping %s: %w", opt.Addr, err)
	}
	return &Client{cli: cli}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c == nil || c.cli == nil {
		return nil
	}
	return c.cli.Close()
}

// Raw 返回底层 redis.Client，用于高级用法
func (c *Client) Raw() *redis.Client {
	return c.cli
}

// --- String ---

// Get 获取字符串
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.cli.Get(ctx, key).Result()
}

// Set 设置字符串
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.cli.Set(ctx, key, value, expiration).Err()
}

// SetNX 不存在时设置
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return c.cli.SetNX(ctx, key, value, expiration).Result()
}

// GetSet 设置新值并返回旧值
func (c *Client) GetSet(ctx context.Context, key string, value interface{}) (string, error) {
	return c.cli.GetSet(ctx, key, value).Result()
}

// MGet 批量获取
func (c *Client) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	return c.cli.MGet(ctx, keys...).Result()
}

// MSet 批量设置
func (c *Client) MSet(ctx context.Context, pairs ...interface{}) error {
	return c.cli.MSet(ctx, pairs...).Err()
}

// Incr 自增
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.cli.Incr(ctx, key).Result()
}

// IncrBy 按指定步长自增
func (c *Client) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.cli.IncrBy(ctx, key, value).Result()
}

// Decr 自减
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return c.cli.Decr(ctx, key).Result()
}

// DecrBy 按指定步长自减
func (c *Client) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.cli.DecrBy(ctx, key, value).Result()
}

// --- Key ---

// Del 删除键
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.cli.Del(ctx, keys...).Result()
}

// Exists 判断键是否存在
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.cli.Exists(ctx, keys...).Result()
}

// Expire 设置过期时间
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.cli.Expire(ctx, key, expiration).Result()
}

// TTL 获取剩余 TTL
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.cli.TTL(ctx, key).Result()
}

// Keys 按 pattern 查询键（生产慎用，建议用 Scan）
func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.cli.Keys(ctx, pattern).Result()
}

// Scan 迭代键，pattern 如 "*"，每次返回一批
func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return c.cli.Scan(ctx, cursor, match, count).Result()
}

// --- Hash ---

// HGet 获取 hash 字段
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return c.cli.HGet(ctx, key, field).Result()
}

// HSet 设置 hash 字段
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.cli.HSet(ctx, key, values...).Result()
}

// HSetNX hash 字段不存在时设置
func (c *Client) HSetNX(ctx context.Context, key, field string, value interface{}) (bool, error) {
	return c.cli.HSetNX(ctx, key, field, value).Result()
}

// HGetAll 获取 hash 所有字段
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.cli.HGetAll(ctx, key).Result()
}

// HDel 删除 hash 字段
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.cli.HDel(ctx, key, fields...).Result()
}

// HExists 判断 hash 字段是否存在
func (c *Client) HExists(ctx context.Context, key, field string) (bool, error) {
	return c.cli.HExists(ctx, key, field).Result()
}

// HIncrBy hash 字段数值增加
func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	return c.cli.HIncrBy(ctx, key, field, incr).Result()
}

// --- List ---

// LPush 从左侧推入列表
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.cli.LPush(ctx, key, values...).Result()
}

// RPush 从右侧推入列表
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.cli.RPush(ctx, key, values...).Result()
}

// LPop 从左侧弹出
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	return c.cli.LPop(ctx, key).Result()
}

// RPop 从右侧弹出
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	return c.cli.RPop(ctx, key).Result()
}

// LRange 按索引范围获取列表元素
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.cli.LRange(ctx, key, start, stop).Result()
}

// LLen 列表长度
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	return c.cli.LLen(ctx, key).Result()
}

// LTrim 只保留 [start, stop] 区间
func (c *Client) LTrim(ctx context.Context, key string, start, stop int64) error {
	return c.cli.LTrim(ctx, key, start, stop).Err()
}

// --- Set ---

// SAdd 向集合添加成员
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return c.cli.SAdd(ctx, key, members...).Result()
}

// SRem 从集合移除成员
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return c.cli.SRem(ctx, key, members...).Result()
}

// SMembers 返回集合所有成员
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.cli.SMembers(ctx, key).Result()
}

// SIsMember 判断是否为成员
func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return c.cli.SIsMember(ctx, key, member).Result()
}

// SCard 集合基数
func (c *Client) SCard(ctx context.Context, key string) (int64, error) {
	return c.cli.SCard(ctx, key).Result()
}

// --- 通用 ---

// Ping 检查连接
func (c *Client) Ping(ctx context.Context) error {
	return c.cli.Ping(ctx).Err()
}
