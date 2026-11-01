package redis

import (
	"reflect"
	"testing"

	goRedis "github.com/go-redis/redis/v8"
)

func TestNewOptionsFromCluster(t *testing.T) {
	opt := NewOptionsFromCluster(" 127.0.0.1:7000, ,127.0.0.1:7001 ", "secret", 0)
	wantAddrs := []string{"127.0.0.1:7000", "127.0.0.1:7001"}
	if !reflect.DeepEqual(opt.Cluster, wantAddrs) {
		t.Fatalf("cluster addresses = %#v, want %#v", opt.Cluster, wantAddrs)
	}
	if opt.Password != "secret" {
		t.Errorf("password = %q, want %q", opt.Password, "secret")
	}
	if opt.PoolSize != 10 {
		t.Errorf("pool size = %d, want 10", opt.PoolSize)
	}
}

func TestNewRedisClientSelectsMode(t *testing.T) {
	t.Run("standalone", func(t *testing.T) {
		cli, err := newRedisClient(&Options{Addr: "127.0.0.1:6379"})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		if _, ok := cli.(*goRedis.Client); !ok {
			t.Fatalf("client type = %T, want *redis.Client", cli)
		}
		client := &Client{cli: cli}
		if client.Raw() == nil || client.RawCluster() != nil {
			t.Fatal("standalone raw client accessors returned unexpected values")
		}
	})

	t.Run("cluster", func(t *testing.T) {
		cli, err := newRedisClient(&Options{
			Cluster: []string{"127.0.0.1:7000", " 127.0.0.1:7001 "},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		if _, ok := cli.(*goRedis.ClusterClient); !ok {
			t.Fatalf("client type = %T, want *redis.ClusterClient", cli)
		}
		client := &Client{cli: cli}
		if client.Raw() != nil || client.RawCluster() == nil {
			t.Fatal("cluster raw client accessors returned unexpected values")
		}
	})
}

func TestNewRedisClientRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  *Options
	}{
		{name: "nil options"},
		{name: "empty address", opt: &Options{}},
		{name: "empty cluster", opt: &Options{Cluster: []string{"", " "}}},
		{name: "cluster database", opt: &Options{Cluster: []string{"127.0.0.1:7000"}, DB: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cli, err := newRedisClient(test.opt)
			if err == nil {
				if cli != nil {
					_ = cli.Close()
				}
				t.Fatal("expected an error")
			}
		})
	}
}