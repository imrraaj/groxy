package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Protocol string

const (
	HTTP  Protocol = "http"
	HTTPS Protocol = "https"
)

type BackendHealth struct {
	HealthCheckURL string
	Healthy        bool
	LastCheck      time.Time
	Failures       int
	mu             sync.Mutex
	Timeout        time.Duration

	// Circuit breaker fields
	CircuitOpen    bool
	CircuitOpenAt  time.Time
	CircuitTimeout time.Duration
}

func NewBackendHealth(url string, timeout time.Duration) BackendHealth {
	return BackendHealth{
		HealthCheckURL: url,
		Timeout:        timeout,
		Healthy:        true,
		LastCheck:      time.Now(),
		CircuitTimeout: 30 * time.Second, // Circuit breaker timeout
	}
}

type Backend struct {
	Protocol Protocol
	Host     string
	Port     int
	Weight   float32
	Bh       BackendHealth
}

func (this *Backend) Stringify() string {
	return fmt.Sprintf("%s://%s:%d", this.Protocol, this.Host, this.Port)
}

func (this *Backend) HealthCheckURL() string {
	return fmt.Sprintf("%s://%s:%d%s", this.Protocol, this.Host, this.Port, this.Bh.HealthCheckURL)
}

func (this *Backend) CheckHealth() {
	this.Bh.mu.Lock()
	defer this.Bh.mu.Unlock()

	client := &http.Client{
		Timeout: this.Bh.Timeout,
	}

	resp, err := client.Get(this.HealthCheckURL())
	this.Bh.LastCheck = time.Now()

	if err != nil {
		this.Bh.Failures++
		this.Bh.Healthy = false
		// Only log after multiple failures to reduce noise
		if this.Bh.Failures == 1 || this.Bh.Failures%5 == 0 {
			log.Printf("Health check failed for %s (failures: %d): %v",
				this.Stringify(), this.Bh.Failures, err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if !this.Bh.Healthy {
			log.Printf("Backend %s recovered after %d failures",
				this.Stringify(), this.Bh.Failures)
		}
		this.Bh.Healthy = true
		this.Bh.Failures = 0
	} else {
		this.Bh.Failures++
		this.Bh.Healthy = false
		if this.Bh.Failures == 1 || this.Bh.Failures%5 == 0 {
			log.Printf("Health check failed for %s: status %d (failures: %d)",
				this.Stringify(), resp.StatusCode, this.Bh.Failures)
		}
	}
}

func (this *Backend) IsHealthy() bool {
	this.Bh.mu.Lock()
	defer this.Bh.mu.Unlock()

	// Circuit breaker logic
	if this.Bh.CircuitOpen {
		if time.Since(this.Bh.CircuitOpenAt) > this.Bh.CircuitTimeout {
			this.Bh.CircuitOpen = false
			this.Bh.Failures = 0 // Reset failures when circuit closes
			log.Printf("Circuit breaker reset for %s", this.Stringify())
		} else {
			return false
		}
	}

	// Open circuit after too many failures
	if this.Bh.Failures >= 5 && !this.Bh.CircuitOpen {
		this.Bh.CircuitOpen = true
		this.Bh.CircuitOpenAt = time.Now()
		log.Printf("Circuit breaker opened for %s after %d failures",
			this.Stringify(), this.Bh.Failures)
		return false
	}

	return this.Bh.Healthy
}

type HealthChecker struct {
	Backends []*Backend
	Interval time.Duration
	Timeout  time.Duration
}

func (this *HealthChecker) StartHealthCheck() {
	ticker := time.NewTicker(this.Interval)
	go func() {
		for range ticker.C {
			log.Println("Healthcheck")
			for _, bh := range this.Backends {
				go bh.CheckHealth()
			}
		}
	}()
}

type LoadBalancer struct {
	backends []*Backend
	last     int
}

func (this *LoadBalancer) Next() *Backend {
	attempts := 0
	totalBackends := len(this.backends)

	for attempts < totalBackends {
		this.last = (this.last + 1) % totalBackends
		backend := this.backends[this.last]
		if backend.IsHealthy() {
			return backend
		}
		attempts++
	}

	log.Println("Warning: All backends are unhealthy!")
	return this.backends[0]
}

type Proxy struct {
	router       *Router
	client       *http.Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	hc           HealthChecker
}

func NewReverseProxy(config *Config) *Proxy {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 5,
		IdleConnTimeout:     time.Duration(config.Proxy.ReadTimeout) * time.Second,
		DisableCompression:  true,
	}

	router := NewRouter(config)
	allBackends := router.getAllBackends()
	hc := HealthChecker{
		Backends: allBackends,
		Interval: time.Second * 5,
		Timeout:  time.Second,
	}
	return &Proxy{
		router:       router,
		client:       &http.Client{Transport: transport},
		hc:           hc,
		ReadTimeout:  time.Duration(config.Proxy.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.Proxy.WriteTimeout) * time.Second,
	}
}

func (this *Proxy) CopyHeaders(dst, src http.Header) {
	hopByHopHeaders := map[string]bool{
		"Connection":          true,
		"Keep-Alive":          true,
		"Proxy-Authenticate":  true,
		"Proxy-Authorization": true,
		"Te":                  true,
		"Trailers":            true,
		"Transfer-Encoding":   true,
		"Upgrade":             true,
	}

	for name, values := range src {
		if hopByHopHeaders[name] {
			continue
		}
		for _, v := range values {
			dst.Add(name, v)
		}
	}
}

func (this *Proxy) AddProxyHeaders(dst http.Request) {
	dst.Header.Set("X-Forwarded-For", dst.RemoteAddr)
	dst.Header.Set("X-Forwarded-Host", dst.Host)
	dst.Header.Add("server", "groxy")
}

func (this *Proxy) StreamResponse(w http.ResponseWriter, res *http.Response) {
	this.CopyHeaders(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	_, err := io.Copy(w, res.Body)
	if err != nil {
		log.Printf("Error streaming response: %v", err)
	}
}
func (this *Proxy) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	metrics := map[string]any{
		"backends":  make(map[string]any),
		"timestamp": time.Now().Unix(),
	}

	for i, backend := range this.router.getAllBackends() {
		metrics["backends"].([]map[string]any)[i] = map[string]any{
			"url":        backend.Stringify(),
			"healthy":    backend.IsHealthy(),
			"failures":   backend.Bh.Failures,
			"last_check": backend.Bh.LastCheck.Unix(),
		}
	}
	json.NewEncoder(w).Encode(metrics)
}

func (this *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if strings.Contains(host, ":") {
		host, _, _ = strings.Cut(host, ":")
	}
	backend := this.router.GetBackend(host)
	if backend == nil {
		log.Printf("No backend found for host: %s", host)
		http.Error(w, "No backend available for this host", http.StatusNotFound)
		return
	}

	if backend.Bh.Healthy == false {
		// If the returned backend is unhealty it means all the backends are unhealty. The Next() function bound to return healthy backend if it exists
		// handle all the backend are unhealth
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)

		html := `<!DOCTYPE html>
					<html>
						<head><title>Service Unavailable</title></head>
						<body>
						    <h1>503 - Service Temporarily Unavailable</h1>
						    <script>setTimeout(() => location.reload(), 30000);</script>
						</body>
					</html>`

		w.Write([]byte(html))
		return
	}
	backendURL := fmt.Sprintf("%s%s", backend.Stringify(), r.RequestURI)

	proxyReq, err := http.NewRequest(r.Method, backendURL, r.Body)
	if err != nil {
		http.Error(w, "Proxy failure", http.StatusInternalServerError)
		return
	}

	this.CopyHeaders(proxyReq.Header, r.Header)
	this.AddProxyHeaders(*proxyReq)

	resp, err := this.client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Backend unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	this.StreamResponse(w, resp)
}

func main() {
	configFile := flag.String("config", "~/.config/groxy.json", "Configuration file for the proxy / reverse proxy")
	flag.Parse()
	config, err := LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("ERROR: Unable to load the file %s\n", err.Error())
	}

	groxyURL := fmt.Sprintf(":%d", config.Proxy.Port)
	proxy := NewReverseProxy(config)
	// go proxy.hc.StartHealthCheck()

	// Setup graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.HandleFunc("/", proxy.ServeHTTP)
	mux.HandleFunc("/metrics", proxy.MetricsHandler)

	server := &http.Server{
		Addr:         groxyURL,
		Handler:      mux,
		ReadTimeout:  time.Duration(config.Proxy.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.Proxy.WriteTimeout) * time.Second,
	}

	go func() {
		log.Printf("Groxy server starting on %s", groxyURL)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()
	<-c
	log.Println("Shutting down gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	} else {
		log.Println("Server shutdown complete")
	}
}
