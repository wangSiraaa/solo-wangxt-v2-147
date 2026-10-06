// Package testsupport provides ephemeral Redis/Postgres instances for tests.
package testsupport

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// freePort asks the kernel for a free TCP port.
func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return addr
}

var (
	realRedisOnce sync.Once
	realRedisBin  string
	realRedisOK   bool
)

func findRedisBinary() string {
	if p := os.Getenv("RL_REDIS_BIN"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	for _, c := range []string{"/tmp/redis-7.4.2/src/redis-server"} {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if p, err := exec.LookPath("redis-server"); err == nil {
		return p
	}
	return ""
}

// RealRedis starts a real redis-server and returns its address. Tests skip
// when no binary is available unless RL_REQUIRE_REDIS=1.
func RealRedis(t testing.TB) (addr string, cleanup func()) {
	t.Helper()
	realRedisOnce.Do(func() {
		realRedisBin = findRedisBinary()
		realRedisOK = realRedisBin != ""
	})
	if !realRedisOK {
		if os.Getenv("RL_REQUIRE_REDIS") == "1" {
			t.Fatal("real redis binary required (RL_REQUIRE_REDIS=1) but none found")
		}
		t.Skip("redis-server binary not found; skipping real-redis test")
	}

	port := freePort(t)
	dir := t.TempDir()
	logFile, err := os.Create(filepath.Join(dir, "redis.log"))
	if err != nil {
		t.Fatalf("redis log: %v", err)
	}
	cmd := exec.Command(realRedisBin,
		"--port", strconv.Itoa(port),
		"--bind", "127.0.0.1",
		"--save", "",
		"--appendonly", "no",
		"--dir", dir,
		"--protected-mode", "no",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis: %v", err)
	}
	addr = "127.0.0.1:" + strconv.Itoa(port)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	ready := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cfn := context.WithTimeout(context.Background(), 300*time.Millisecond)
		pong, err := rdb.Ping(ctx).Result()
		cfn()
		if err == nil && pong == "PONG" {
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		out, _ := os.ReadFile(filepath.Join(dir, "redis.log"))
		t.Fatalf("redis not ready\n%s", out)
	}

	cleanup = func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}
	return addr, cleanup
}

// RedisClient returns a go-redis client for addr.
func RedisClient(t testing.TB, addr string) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// MiniRedis starts an in-process miniredis with a controllable clock and
// returns the server and a go-redis client.
func MiniRedis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}
