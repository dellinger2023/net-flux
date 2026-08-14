package cache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInMemCache_Basic(t *testing.T) {
	c := NewInMemCache()
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("set a: %v", err)
	}
	v, err := c.Get("a")
	if err != nil || v != 1 {
		t.Fatalf("get a: got %v, %v", v, err)
	}

	if err := c.Delete("a"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if v, _ := c.Get("a"); v != nil {
		t.Fatalf("deleted key still present: %v", v)
	}
	if c.Length() != 0 {
		t.Fatalf("length want 0, got %d", c.Length())
	}
}

func TestInMemCache_SetWithTTL(t *testing.T) {
	c := NewInMemCache()
	defer c.Close()

	if err := c.SetWithTTL("ttl", "alive", 50*time.Millisecond); err != nil {
		t.Fatalf("set with ttl: %v", err)
	}
	if v, _ := c.Get("ttl"); v != "alive" {
		t.Fatalf("before expire want alive, got %v", v)
	}

	time.Sleep(80 * time.Millisecond)
	if v, _ := c.Get("ttl"); v != nil {
		t.Fatalf("after expire want nil, got %v", v)
	}
}

func TestInMemCache_UpdateOverwrite(t *testing.T) {
	c := NewInMemCache()
	defer c.Close()

	_ = c.Set("k", 1)
	_ = c.Set("k", 2)
	v, _ := c.Get("k")
	if v != 2 {
		t.Fatalf("overwrite want 2, got %v", v)
	}
	if c.Length() != 1 {
		t.Fatalf("length want 1, got %d", c.Length())
	}
}

func TestInMemCache_ConcurrentSameKey(t *testing.T) {
	const (
		goroutines = 64
		opsPerG    = 2000
		key        = "hot-key"
	)

	c := NewInMemCache()
	defer c.Close()

	var (
		wg      sync.WaitGroup
		setErrs atomic.Int64
		getErrs atomic.Int64
		delErrs atomic.Int64
		getHits atomic.Int64
		getMiss atomic.Int64
	)

	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerG; j++ {
				switch j % 4 {
				case 0:
					if err := c.Set(key, id*opsPerG+j); err != nil {
						setErrs.Add(1)
					}
				case 1:
					if err := c.SetWithTTL(key, id, time.Minute); err != nil {
						setErrs.Add(1)
					}
				case 2:
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
}

func TestInMemCache_ConcurrentMixedKeys(t *testing.T) {
	const (
		goroutines = 32
		opsPerG    = 5000
		keySpace   = 512
	)

	c := NewInMemCache()
	defer c.Close()

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
				case 0, 1:
					if err := c.Set(key, j); err != nil {
						setFail.Add(1)
					}
				case 2:
					if err := c.SetWithTTL(key, j, time.Hour); err != nil {
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
}

func TestInMemCache_ConcurrentClose(t *testing.T) {
	c := NewInMemCache()
	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				key := strconv.Itoa(i*500 + j)
				_ = c.Set(key, j)
				_ = c.SetWithTTL(key, j, time.Minute)
				_, _ = c.Get(key)
				_ = c.Delete(key)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		_ = c.Close()
		_ = c.Close()
	}()

	wg.Wait()
	if c.Length() != 0 {
		t.Fatalf("after close length want 0, got %d", c.Length())
	}
}

func TestInMemCache_PerfSummary(t *testing.T) {
	type scenario struct {
		name       string
		goroutines int
		opsPerG    int
		keySpace   int
		writeRatio int // 0-10
	}

	scenarios := []scenario{
		{name: "read-heavy", goroutines: 8, opsPerG: 20000, keySpace: 512, writeRatio: 2},
		{name: "write-heavy", goroutines: 16, opsPerG: 10000, keySpace: 1024, writeRatio: 8},
		{name: "contention", goroutines: 32, opsPerG: 8000, keySpace: 16, writeRatio: 5},
	}

	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			c := NewInMemCache()
			defer c.Close()

			// 预热，避免读场景全 miss
			for i := 0; i < sc.keySpace; i++ {
				_ = c.Set("k"+strconv.Itoa(i), i)
			}

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
						switch {
						case i%10 < sc.writeRatio/2:
							_ = c.Set(key, i)
						case i%10 < sc.writeRatio:
							_ = c.SetWithTTL(key, i, time.Hour)
						default:
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

func BenchmarkInMemCache_Get(b *testing.B) {
	c := NewInMemCache()
	defer c.Close()
	for i := 0; i < 1024; i++ {
		_ = c.Set(strconv.Itoa(i), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(strconv.Itoa(i % 1024))
	}
}

func BenchmarkInMemCache_Set(b *testing.B) {
	c := NewInMemCache()
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Set(strconv.Itoa(i%2048), i)
	}
}

func BenchmarkInMemCache_SetWithTTL(b *testing.B) {
	c := NewInMemCache()
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.SetWithTTL(strconv.Itoa(i%2048), i, time.Minute)
	}
}

func BenchmarkInMemCache_ParallelMixed(b *testing.B) {
	c := NewInMemCache()
	defer c.Close()
	for i := 0; i < 512; i++ {
		_ = c.Set(strconv.Itoa(i), i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.Itoa(i % 2048)
			switch i % 4 {
			case 0:
				_ = c.Set(key, i)
			case 1:
				_ = c.SetWithTTL(key, i, time.Minute)
			case 2:
				_, _ = c.Get(key)
			default:
				_ = c.Delete(key)
			}
			i++
		}
	})
}
