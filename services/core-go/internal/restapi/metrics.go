package restapi

import (
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	startTime     = time.Now()
	totalRequests atomic.Uint64
)

func (server *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	uptime := time.Since(startTime).Seconds()

	fmt.Fprintf(w, "# HELP streamforge_build_info Build and version metadata.\n")
	fmt.Fprintf(w, "# TYPE streamforge_build_info gauge\n")
	fmt.Fprintf(w, "streamforge_build_info{service=\"core-go\",version=\"0.1.0\"} 1\n\n")

	fmt.Fprintf(w, "# HELP streamforge_uptime_seconds Seconds elapsed since service startup.\n")
	fmt.Fprintf(w, "# TYPE streamforge_uptime_seconds gauge\n")
	fmt.Fprintf(w, "streamforge_uptime_seconds %.2f\n\n", uptime)

	fmt.Fprintf(w, "# HELP streamforge_go_goroutines Number of running goroutines.\n")
	fmt.Fprintf(w, "# TYPE streamforge_go_goroutines gauge\n")
	fmt.Fprintf(w, "streamforge_go_goroutines %d\n\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP streamforge_go_mem_alloc_bytes Bytes allocated and currently in use.\n")
	fmt.Fprintf(w, "# TYPE streamforge_go_mem_alloc_bytes gauge\n")
	fmt.Fprintf(w, "streamforge_go_mem_alloc_bytes %d\n\n", m.Alloc)

	fmt.Fprintf(w, "# HELP streamforge_go_mem_sys_bytes Total bytes of memory obtained from the OS.\n")
	fmt.Fprintf(w, "# TYPE streamforge_go_mem_sys_bytes gauge\n")
	fmt.Fprintf(w, "streamforge_go_mem_sys_bytes %d\n\n", m.Sys)

	fmt.Fprintf(w, "# HELP streamforge_go_gc_count Total garbage collections completed.\n")
	fmt.Fprintf(w, "# TYPE streamforge_go_gc_count counter\n")
	fmt.Fprintf(w, "streamforge_go_gc_count %d\n\n", m.NumGC)

	fmt.Fprintf(w, "# HELP streamforge_http_requests_total Total number of HTTP requests serviced.\n")
	fmt.Fprintf(w, "# TYPE streamforge_http_requests_total counter\n")
	fmt.Fprintf(w, "streamforge_http_requests_total %d\n\n", totalRequests.Load())

	if server.lagReader != nil {
		if lag, err := server.lagReader.ReadConsumerLag(r.Context()); err == nil {
			fmt.Fprintf(w, "# HELP streamforge_kafka_consumer_lag Total uncommitted consumer lag records.\n")
			fmt.Fprintf(w, "# TYPE streamforge_kafka_consumer_lag gauge\n")
			fmt.Fprintf(w, "streamforge_kafka_consumer_lag{group=\"%s\"} %d\n\n", lag.Group, lag.TotalLag)
		}
	}
}
