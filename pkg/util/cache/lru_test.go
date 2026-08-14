package cache

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLRUCache_Basic(t *testing.T) {
	c := NewLRUCache(2)

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := c.Set("b", 2); err != nil {
		t.Fatalf("set b: %v", err)
	}

	v, err := c.Get("a")
	if err != nil || v != 1 {
		t.Fatalf("get a: got %v, %v", v, err)
	}

	// 访问 a 后，再写入 c，应淘汰最久未用的 b
	if err := c.Set("c", 3); err != nil {
		t.Fatalf("set c: %v", err)
	}
	if c.Length() != 2 {
		t.Fatalf("length want 2, got %d", c.Length())
	}

	if v, _ := c.Get("b"); v != nil {
		t.Fatalf("b should be evicted, got %v", v)
	}
	if v, _ := c.Get("a"); v != 1 {
		t.Fatalf("a should remain, got %v", v)
	}
	if v, _ := c.Get("c"); v != 3 {
		t.Fatalf("c should remain, got %v", v)
	}
}

func TestLRUCache_UpdateAndDelete(t *testing.T) {
	c := NewLRUCache(2)
	_ = c.Set("a", 1)
	_ = c.Set("a", 10)

	v, _ := c.Get("a")
	if v != 10 {
		t.Fatalf("update want 10, got %v", v)
	}
	if c.Length() != 1 {
		t.Fatalf("length want 1, got %d", c.Length())
	}

	_ = c.Delete("a")
	if v, _ := c.Get("a"); v != nil {
		t.Fatalf("deleted key still present: %v", v)
	}
	if c.Length() != 0 {
		t.Fatalf("length want 0, got %d", c.Length())
	}
}

func TestLRUCache_ConcurrentSameKey(t *testing.T) {
	const (
		goroutines = 64
		opsPerG    = 2000
		key        = "hot-key"
	)

	c := NewLRUCache(16)
	var (
		wg       sync.WaitGroup
		setErrs  atomic.Int64
		getErrs  atomic.Int64
		delErrs  atomic.Int64
		getHits  atomic.Int64
		getMiss  atomic.Int64
	)

	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerG; j++ {
				switch j % 3 {
				case 0:
					if err := c.Set(key, id*opsPerG+j); err != nil {
						setErrs.Add(1)
					}
				case 1:
					v, err := c.Get(key)
					if err != nil {
						getErrs.Add(1)
						continue
					}
					if v == nil {
						getMiss.Add(1)
					} else {
						getHits.Add(1)
					}
				default:
					if err := c.Delete(key); err != nil {
						delErrs.Add(1)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalOps := int64(goroutines * opsPerG)
	t.Logf("same-key conflict: goroutines=%d ops=%d elapsed=%v qps=%.0f hits=%d miss=%d setErr=%d getErr=%d delErr=%d len=%d",
		goroutines, totalOps, elapsed, float64(totalOps)/elapsed.Seconds(),
		getHits.Load(), getMiss.Load(), setErrs.Load(), getErrs.Load(), delErrs.Load(), c.Length())

	if setErrs.Load()+getErrs.Load()+delErrs.Load() > 0 {
		t.Fatalf("unexpected errors under same-key contention")
	}
	if c.Length() > 16 {
		t.Fatalf("length exceeded limit: %d", c.Length())
	}
}

func TestLRUCache_ConcurrentMixedKeys(t *testing.T) {
	const (
		goroutines = 32
		opsPerG    = 5000
		limit      = 128
		keySpace   = 256
	)

	c := NewLRUCache(limit)
	var (
		wg      sync.WaitGroup
		ops     atomic.Int64
		setFail atomic.Int64
		getFail atomic.Int64
		delFail atomic.Int64
	)

	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerG; j++ {
				key := "k-" + strconv.Itoa((id*31+j)%keySpace)
				ops.Add(1)
				switch j % 5 {
				case 0, 1, 2: // 偏写，制造淘汰冲突
					if err := c.Set(key, j); err != nil {
						setFail.Add(1)
					}
				case 3:
					if _, err := c.Get(key); err != nil {
						getFail.Add(1)
					}
				default:
					if err := c.Delete(key); err != nil {
						delFail.Add(1)
					}
				}
				_ = c.Length()
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := ops.Load()
	t.Logf("mixed-key conflict: goroutines=%d ops=%d elapsed=%v qps=%.0f setFail=%d getFail=%d delFail=%d finalLen=%d",
		goroutines, total, elapsed, float64(total)/elapsed.Seconds(),
		setFail.Load(), getFail.Load(), delFail.Load(), c.Length())

	if setFail.Load()+getFail.Load()+delFail.Load() > 0 {
		t.Fatalf("unexpected errors under mixed-key contention")
	}
	if c.Length() > limit {
		t.Fatalf("length exceeded limit: got %d > %d", c.Length(), limit)
	}
}

func TestLRUCache_ConcurrentEvictionInvariant(t *testing.T) {
	const (
		goroutines = 16
		rounds     = 3000
		limit      = 32
	)

	c := NewLRUCache(limit)
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < rounds; j++ {
				key := fmt.Sprintf("g%d-%d", id, j)
				if err := c.Set(key, j); err != nil {
					errCh <- err
					return
				}
				if n := c.Length(); n > limit {
					errCh <- fmt.Errorf("length %d exceeds limit %d", n, limit)
					return
				}
				_, _ = c.Get(fmt.Sprintf("g%d-%d", id, j/2))
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("invariant broken: %v", err)
	}
	if c.Length() > limit {
		t.Fatalf("final length %d exceeds limit %d", c.Length(), limit)
	}
}

func TestLRUCache_ConcurrentClose(t *testing.T) {
	c := NewLRUCache(64)
	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				key := strconv.Itoa(i*500 + j)
				_ = c.Set(key, j)
				_, _ = c.Get(key)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		_ = c.Close()
		_ = c.Close() // 幂等
	}()

	wg.Wait()
	if c.Length() != 0 {
		t.Fatalf("after close length want 0, got %d", c.Length())
	}
}

func TestLRUCache_PerfSummary(t *testing.T) {
	type scenario struct {
		name       string
		limit      int
		goroutines int
		opsPerG    int
		keySpace   int
		writeRatio int // 0-10, Set 占比
	}

	scenarios := []scenario{
		{name: "read-heavy", limit: 1024, goroutines: 8, opsPerG: 20000, keySpace: 512, writeRatio: 2},
		{name: "write-heavy", limit: 256, goroutines: 16, opsPerG: 10000, keySpace: 1024, writeRatio: 8},
		{name: "contention", limit: 64, goroutines: 32, opsPerG: 8000, keySpace: 16, writeRatio: 5},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			c := NewLRUCache(sc.limit)
			var wg sync.WaitGroup
			var ops atomic.Int64

			start := time.Now()
			for g := 0; g < sc.goroutines; g++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for i := 0; i < sc.opsPerG; i++ {
						key := "k" + strconv.Itoa((id*97+i)%sc.keySpace)
						ops.Add(1)
						if i%10 < sc.writeRatio {
							_ = c.Set(key, i)
						} else {
							_, _ = c.Get(key)
						}
					}
				}(g)
			}
			wg.Wait()
			elapsed := time.Since(start)

			total := ops.Load()
			nsPerOp := float64(elapsed.Nanoseconds()) / float64(total)
			t.Logf("perf[%s]: goroutines=%d ops=%d elapsed=%v qps=%.0f ns/op=%.1f finalLen=%d",
				sc.name, sc.goroutines, total, elapsed,
				float64(total)/elapsed.Seconds(), nsPerOp, c.Length())
		})
	}
}

func BenchmarkLRUCache_Get(b *testing.B) {
	c := NewLRUCache(1024)
	for i := 0; i < 1024; i++ {
		_ = c.Set(strconv.Itoa(i), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(strconv.Itoa(i % 1024))
	}
}

func BenchmarkLRUCache_Set(b *testing.B) {
	c := NewLRUCache(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Set(strconv.Itoa(i%2048), i)
	}
}

func BenchmarkLRUCache_ParallelMixed(b *testing.B) {
	c := NewLRUCache(1024)
	for i := 0; i < 512; i++ {
		_ = c.Set(strconv.Itoa(i), i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.Itoa(i % 2048)
			if i%3 == 0 {
				_ = c.Set(key, i)
			} else if i%3 == 1 {
				_, _ = c.Get(key)
			} else {
				_ = c.Delete(key)
			}
			i++
		}
	})
}
