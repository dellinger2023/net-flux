package cache

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dellinger2023/net-flux/pkg/util/redis"
	"github.com/google/uuid"
)

const testRedisAddr = "localhost:6379"

func newTestSharedCache(t *testing.T) (Cache, string) {
	t.Helper()

	prefix := "test-shared-cache-" + uuid.NewString()
	c, err := NewSharedCacheWithAddress(&redis.Options{
		Addr:     testRedisAddr,
		PoolSize: 10,
		DB:       0,
	}, prefix)
	if err != nil {
		t.Fatalf("connect redis %s failed: %v", testRedisAddr, err)
	}

	t.Cleanup(func() {
		// 清理本次测试写入的 key
		rc, ok := c.(*redisCache)
		if !ok {
			_ = c.Close()
			return
		}
		ctxKeys := make([]string, 0)
		var cursor uint64
		pattern := prefix + "*"
		for {
			keys, next, err := rc.cli.Scan(t.Context(), cursor, pattern, 100)
			if err != nil {
				break
			}
			ctxKeys = append(ctxKeys, keys...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(ctxKeys) > 0 {
			_, _ = rc.cli.Del(t.Context(), ctxKeys...)
		}
		_ = c.Close()
	})

	return c, prefix
}

func TestSharedCache_Basic(t *testing.T) {
	c, p := newTestSharedCache(t)

	key := p + "basic"
	if err := c.Set(key, "hello"); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, err := c.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if v != "hello" {
		t.Fatalf("get want hello, got %#v", v)
	}

	if err := c.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	v, err = c.Get(key)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if v != nil {
		t.Fatalf("after delete want nil, got %#v", v)
	}
}

func TestSharedCache_TypedValues(t *testing.T) {
	c, p := newTestSharedCache(t)

	cases := []struct {
		key string
		val any
	}{
		{p + "int", 42},
		{p + "str", "net-flux"},
		{p + "bool", true},
		{p + "float", 3.14},
		{p + "bytes", []byte("bin")},
		{p + "slice", []string{"a", "b"}},
		{p + "map", map[string]int{"x": 1, "y": 2}},
	}

	for _, tc := range cases {
		if err := c.Set(tc.key, tc.val); err != nil {
			t.Fatalf("set %s: %v", tc.key, err)
		}
		got, err := c.Get(tc.key)
		if err != nil {
			t.Fatalf("get %s: %v", tc.key, err)
		}
		switch want := tc.val.(type) {
		case []byte:
			gb, ok := got.([]byte)
			if !ok || string(gb) != string(want) {
				t.Fatalf("key %s want %v, got %#v", tc.key, want, got)
			}
		case []string:
			gs, ok := got.([]string)
			if !ok || len(gs) != len(want) || gs[0] != want[0] {
				t.Fatalf("key %s want %v, got %#v", tc.key, want, got)
			}
		case map[string]int:
			gm, ok := got.(map[string]int)
			if !ok || gm["x"] != 1 || gm["y"] != 2 {
				t.Fatalf("key %s want %v, got %#v", tc.key, want, got)
			}
		default:
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
				t.Fatalf("key %s want %#v, got %#v", tc.key, want, got)
			}
		}
	}
}

func TestSharedCache_SetWithTTL(t *testing.T) {
	c, p := newTestSharedCache(t)

	key := p + "ttl"
	if err := c.SetWithTTL(key, "temp", 200*time.Millisecond); err != nil {
		t.Fatalf("set with ttl: %v", err)
	}
	if v, _ := c.Get(key); v != "temp" {
		t.Fatalf("before expire want temp, got %#v", v)
	}

	time.Sleep(350 * time.Millisecond)
	v, err := c.Get(key)
	if err != nil {
		t.Fatalf("get after expire: %v", err)
	}
	if v != nil {
		t.Fatalf("after expire want nil, got %#v", v)
	}
}

func TestSharedCache_Miss(t *testing.T) {
	c, p := newTestSharedCache(t)
	v, err := c.Get(p + "missing")
	if err != nil {
		t.Fatalf("get miss: %v", err)
	}
	if v != nil {
		t.Fatalf("miss want nil, got %#v", v)
	}
}

func TestSharedCache_Length(t *testing.T) {
	c, p := newTestSharedCache(t)

	for i := 0; i < 5; i++ {
		if err := c.Set(p+"len-"+strconv.Itoa(i), i); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if c.Length() != 0 {
		t.Fatalf("shared cache Length always returns 0, got %d", c.Length())
	}
}

func TestSharedCache_Concurrent(t *testing.T) {
	c, p := newTestSharedCache(t)

	const (
		goroutines = 16
		opsPerG    = 100
	)

	var (
		wg      sync.WaitGroup
		setFail atomic.Int64
		getFail atomic.Int64
		delFail atomic.Int64
	)

	start := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				key := p + "c-" + strconv.Itoa((id*31+i)%64)
				switch i % 3 {
				case 0:
					if err := c.Set(key, i); err != nil {
						setFail.Add(1)
					}
				case 1:
					if _, err := c.Get(key); err != nil {
						getFail.Add(1)
					}
				default:
					if err := c.Delete(key); err != nil {
						delFail.Add(1)
					}
				}
			}
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := int64(goroutines * opsPerG)
	t.Logf("shared concurrent: goroutines=%d ops=%d elapsed=%v qps=%.0f setFail=%d getFail=%d delFail=%d",
		goroutines, total, elapsed, float64(total)/elapsed.Seconds(),
		setFail.Load(), getFail.Load(), delFail.Load())

	if setFail.Load()+getFail.Load()+delFail.Load() > 0 {
		t.Fatalf("unexpected redis errors under concurrency")
	}
}

func TestNewSharedCache_NilClient(t *testing.T) {
	if _, err := NewSharedCache(nil, ""); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func BenchmarkSharedCache_SetGet(b *testing.B) {
	prefix := "bench-" + uuid.NewString() + "-"
	c, err := NewSharedCacheWithAddress(&redis.Options{Addr: testRedisAddr, PoolSize: 20}, prefix)
	if err != nil {
		b.Fatalf("redis unavailable: %v", err)
	}
	defer c.Close()

	b.Cleanup(func() {
		if rc, ok := c.(*redisCache); ok {
			var cursor uint64
			for {
				keys, next, err := rc.cli.Scan(b.Context(), cursor, prefix+"*", 200)
				if err != nil || len(keys) == 0 && next == 0 {
					break
				}
				if len(keys) > 0 {
					_, _ = rc.cli.Del(b.Context(), keys...)
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i % 1024)
		_ = c.Set(key, i)
		_, _ = c.Get(key)
	}
}
