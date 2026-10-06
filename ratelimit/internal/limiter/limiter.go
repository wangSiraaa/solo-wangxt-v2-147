// Package limiter 执行多桶令牌桶的原子判定。
//
// 判定全部在 Redis 端的单个 Lua 脚本内完成（见 script.lua），时钟源为
// Redis 服务器 TIME，应用实例与客户端的时钟不参与判定。
package limiter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed script.lua
var scriptLua string

var script = redis.NewScript(scriptLua)

// Layer 标识限流层级。
type Layer string

const (
	LayerTenant   Layer = "tenant"
	LayerUser     Layer = "user"
	LayerEndpoint Layer = "endpoint"
)

// Bucket 描述参与一次判定的一个令牌桶。
type Bucket struct {
	Layer      Layer
	Key        string  // Redis 键
	Capacity   float64 // 突发容量（桶上限）
	RatePerSec float64 // 补充速率（令牌/秒），0 表示不补充
}

// ttl 返回桶状态的过期时间：足够两次满桶补充再加余量，避免闲置键永久残留。
func (b Bucket) ttl() time.Duration {
	if b.RatePerSec <= 0 {
		return 24 * time.Hour
	}
	return time.Duration(2*b.Capacity/b.RatePerSec*float64(time.Second)) + time.Minute
}

// Decision 是一次原子判定的结果。
type Decision struct {
	Allowed    bool
	Replay     bool          // 是否为幂等窗口内的重放（未重复扣减）
	LimitedBy  Layer         // 拒绝时约束最强的层级
	RetryAfter time.Duration // 建议重试等待时间；-1 表示当前策略下无法满足
	Remaining  map[Layer]float64
}

// Limiter 在 Redis 上执行原子判定。
type Limiter struct {
	rdb redis.UniversalClient
}

func New(rdb redis.UniversalClient) *Limiter { return &Limiter{rdb: rdb} }

var errMalformedReply = errors.New("limiter: malformed script reply")

// Check 对 buckets 做一次全有或全无的判定：
// 所有桶都允许才各扣 cost，否则任何一个桶都不扣。
// idemKey 非空且 idemTTL > 0 时，同一 idemKey 在窗口内重试返回首次判定、不重复扣减。
func (l *Limiter) Check(ctx context.Context, buckets []Bucket, cost float64, idemKey string, idemTTL time.Duration) (Decision, error) {
	d := Decision{Remaining: map[Layer]float64{}}
	if len(buckets) == 0 {
		d.Allowed = true // 没有任何层级配置策略时不限制
		return d, nil
	}

	keys := make([]string, 0, len(buckets)+1)
	for _, b := range buckets {
		keys = append(keys, b.Key)
	}
	idemMs := int64(0)
	if idemKey != "" && idemTTL > 0 {
		idemMs = idemTTL.Milliseconds()
		keys = append(keys, idemKey)
	} else {
		keys = append(keys, "rl:idem:__disabled__") // 占位，脚本不会访问
	}

	args := make([]interface{}, 0, 3+3*len(buckets))
	args = append(args, len(buckets), cost, idemMs)
	for _, b := range buckets {
		args = append(args, b.Capacity, b.RatePerSec, b.ttl().Milliseconds())
	}

	raw, err := script.Run(ctx, l.rdb, keys, args...).Result()
	if err != nil {
		return d, err
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 2 {
		return d, errMalformedReply
	}
	head := toString(arr[0])
	d.Replay = toString(arr[1]) == "1"

	switch {
	case head == "A":
		d.Allowed = true
	case strings.HasPrefix(head, "D:"):
		parts := strings.Split(head, ":")
		if len(parts) != 3 {
			return d, errMalformedReply
		}
		layerIdx, err := strconv.Atoi(parts[1])
		if err != nil || layerIdx < 1 || layerIdx > len(buckets) {
			return d, errMalformedReply
		}
		d.LimitedBy = buckets[layerIdx-1].Layer
		waitMs, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return d, errMalformedReply
		}
		if waitMs < 0 {
			d.RetryAfter = -1
		} else {
			d.RetryAfter = time.Duration(waitMs) * time.Millisecond
		}
	default:
		return d, errMalformedReply
	}

	// 重放响应不携带各桶余量
	for i, b := range buckets {
		if 2+i < len(arr) {
			if v, err := strconv.ParseFloat(toString(arr[2+i]), 64); err == nil {
				d.Remaining[b.Layer] = v
			}
		}
	}
	return d, nil
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", v)
	}
}
