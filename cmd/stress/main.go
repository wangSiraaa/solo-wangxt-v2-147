// Command stress runs a concurrent workload against the decision API and
// verifies the admitted request count against the token-bucket UPPER BOUND
// over sliding windows — not merely that average QPS is near target.
//
// Usage (against a running server):
//
//	go run ./cmd/stress -url http://127.0.0.1:8080 \
//	    -tenant t1 -user u1 -api search -duration 10s -concurrency 32
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		base        = flag.String("url", "http://127.0.0.1:8080", "server base URL")
		tenant      = flag.String("tenant", "t1", "tenant id")
		user        = flag.String("user", "u1", "user id")
		apiName     = flag.String("api", "search", "api id")
		duration    = flag.Duration("duration", 5*time.Second, "test duration")
		concurrency = flag.Int("concurrency", 16, "worker goroutines")
		// Expected per-bucket policy so the test can assert the bound itself.
		capTok  = flag.Float64("capacity", 50, "tightest bucket burst capacity (tokens)")
		rateTPS = flag.Float64("rate", 20, "tightest bucket refill rate (tokens/s)")
		noUser  = flag.Bool("no-user", false, "do not send user id")
	)
	flag.Parse()

	end := time.Now().Add(*duration)
	var allowed, denied int64
	var mu sync.Mutex
	allowTimes := []time.Duration{}
	var wg sync.WaitGroup
	var startWg sync.WaitGroup
	startWg.Add(1)

	client := &http.Client{Timeout: 3 * time.Second}
	start := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startWg.Wait()
			for time.Now().Before(end) {
				body := map[string]any{
					"tenant": *tenant,
					"api":    *apiName,
				}
				if !*noUser {
					body["user"] = *user
				}
				b, _ := json.Marshal(body)
				req, _ := http.NewRequest(http.MethodPost,
					*base+"/api/v1/ratelimit/check", bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					continue
				}
				var out map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&out)
				resp.Body.Close()
				t := time.Since(start)
				if ok, _ := out["allowed"].(bool); ok {
					atomic.AddInt64(&allowed, 1)
					mu.Lock()
					allowTimes = append(allowTimes, t)
					mu.Unlock()
				} else {
					atomic.AddInt64(&denied, 1)
				}
			}
		}()
	}
	startWg.Done()
	wg.Wait()
	elapsed := time.Since(start)

	total := allowed + denied
	fmt.Printf("elapsed=%s concurrency=%d total=%d allowed=%d denied=%d allow%%=%.2f\n",
		elapsed.Truncate(time.Millisecond), *concurrency, total, allowed, denied,
		100*float64(allowed)/math.Max(float64(total), 1))

	// ---- Upper-bound audit -------------------------------------------------
	// Token bucket bound: over any interval [t0, t0+T], the number of
	// admitted requests never exceeds capacity + rate*T (plus one token of
	// discrete slack). We check this over MULTIPLE sliding window lengths so
	// a correct average cannot hide short-window violations.
	capMT := int64(*capTok * 1000)
	rateMTPS := int64(*rateTPS * 1000)
	failed := false
	for _, winMs := range []int64{200, 500, 1000, 2000, 5000} {
		if int64(elapsed/time.Millisecond) < winMs {
			continue
		}
		// Sliding count via two pointers over sorted admission timestamps.
		maxCount := int64(0)
		j := 0
		for i := range allowTimes {
			for int64(allowTimes[j]/time.Millisecond)+winMs <= int64(allowTimes[i]/time.Millisecond) {
				j++
			}
			cnt := int64(i - j + 1)
			if cnt > maxCount {
				maxCount = cnt
			}
		}
		bound := int64(float64(capMT)/1000.0 + float64(rateMTPS)/1000.0*float64(winMs)/1000.0 + 1.0)
		status := "OK"
		if maxCount > bound {
			status = "VIOLATION"
			failed = true
		}
		fmt.Printf("[bound] window=%5dms admitted=%4d upperBound=%4d  %s\n",
			winMs, maxCount, bound, status)
	}

	// First-window burst must not exceed capacity (+1 discrete slack).
	burst := 0
	burstWindow := time.Second // count admissions in first second; bound cap+rate
	for _, t := range allowTimes {
		if t <= burstWindow {
			burst++
		}
	}
	burstBound := int(*capTok + *rateTPS + 1)
	if burst > burstBound {
		fmt.Printf("[burst] first-second admissions=%d exceed cap+rate=%d VIOLATION\n",
			burst, burstBound)
		failed = true
	} else {
		fmt.Printf("[burst] first-second admissions=%d <= cap+rate=%d OK\n",
			burst, burstBound)
	}

	if failed {
		os.Exit(1)
	}
	fmt.Println("PASS: admitted counts never exceed the token-bucket upper bound")
}
