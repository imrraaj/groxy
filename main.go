package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	Health   BackendHealth
}

func (b *Backend) Stringify() string {
	return fmt.Sprintf("%s://%s:%d", b.Protocol, b.Host, b.Port)
}

func (b *Backend) HealthCheckURL() string {
	return fmt.Sprintf("%s://%s:%d%s", b.Protocol, b.Host, b.Port, b.Health.HealthCheckURL)
}

func (b *Backend) CheckHealth() {
	b.Health.mu.Lock()
	defer b.Health.mu.Unlock()

	client := &http.Client{
		Timeout: b.Health.Timeout,
	}

	resp, err := client.Get(b.HealthCheckURL())
	b.Health.LastCheck = time.Now()

	if err != nil {
		b.Health.Failures++
		b.Health.Healthy = false
		// Only log after multiple failures to reduce noise
		if b.Health.Failures == 1 || b.Health.Failures%5 == 0 {
			log.Printf("Health check failed for %s (failures: %d): %v",
				b.Stringify(), b.Health.Failures, err)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if !b.Health.Healthy {
			log.Printf("Backend %s recovered after %d failures",
				b.Stringify(), b.Health.Failures)
		}
		b.Health.Healthy = true
		b.Health.Failures = 0
	} else {
		b.Health.Failures++
		b.Health.Healthy = false
		if b.Health.Failures == 1 || b.Health.Failures%5 == 0 {
			log.Printf("Health check failed for %s: status %d (failures: %d)",
				b.Stringify(), resp.StatusCode, b.Health.Failures)
		}
	}
}

func (b *Backend) IsHealthy() bool {
	b.Health.mu.Lock()
	defer b.Health.mu.Unlock()

	// Circuit breaker logic
	if b.Health.CircuitOpen {
		if time.Since(b.Health.CircuitOpenAt) > b.Health.CircuitTimeout {
			b.Health.CircuitOpen = false
			b.Health.Failures = 0 // Reset failures when circuit closes
			log.Printf("Circuit breaker reset for %s", b.Stringify())
		} else {
			return false
		}
	}

	// Open circuit after too many failures
	if b.Health.Failures >= 5 && !b.Health.CircuitOpen {
		b.Health.CircuitOpen = true
		b.Health.CircuitOpenAt = time.Now()
		log.Printf("Circuit breaker opened for %s after %d failures",
			b.Stringify(), b.Health.Failures)
		return false
	}

	return b.Health.Healthy
}

type HealthChecker struct {
	Backends []*Backend
	Interval time.Duration
	Timeout  time.Duration
}

func (hc *HealthChecker) StartHealthCheck() {
	ticker := time.NewTicker(hc.Interval)
	go func() {
		for range ticker.C {
			log.Println("Healthcheck")
			for _, backend := range hc.Backends {
				go backend.CheckHealth()
			}
		}
	}()
}

type LoadBalancer struct {
	backends []*Backend
	last     int
}

func (lb *LoadBalancer) Next() *Backend {
	attempts := 0
	totalBackends := len(lb.backends)

	for attempts < totalBackends {
		lb.last = (lb.last + 1) % totalBackends
		backend := lb.backends[lb.last]
		if backend.IsHealthy() {
			return backend
		}
		attempts++
	}

	log.Println("Warning: All backends are unhealthy!")
	return lb.backends[0]
}

type Proxy struct {
	router       *Router
	client       *http.Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	hc           HealthChecker
	mu           sync.Mutex
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

func (p *Proxy) CopyHeaders(dst, src http.Header) {
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

func (p *Proxy) AddProxyHeaders(dst http.Request) {
	dst.Header.Set("X-Forwarded-For", dst.RemoteAddr)
	dst.Header.Set("X-Forwarded-Host", dst.Host)
	dst.Header.Add("server", "groxy")
}

func (p *Proxy) StreamResponse(w http.ResponseWriter, res *http.Response) {
	p.CopyHeaders(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	_, err := io.Copy(w, res.Body)
	if err != nil {
		log.Printf("Error streaming response: %v", err)
	}
}
func (p *Proxy) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	backends := []map[string]any{}
	for _, backend := range p.router.getAllBackends() {
		backends = append(backends, map[string]any{
			"url":        backend.Stringify(),
			"healthy":    backend.IsHealthy(),
			"failures":   backend.Health.Failures,
			"last_check": backend.Health.LastCheck.Unix(),
		})
	}

	metrics := map[string]any{
		"backends":  backends,
		"timestamp": time.Now().Unix(),
	}
	json.NewEncoder(w).Encode(metrics)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := r.Host
	if strings.Contains(host, ":") {
		host, _, _ = strings.Cut(host, ":")
	}
	backend := p.router.GetBackend(host)
	if backend == nil {
		log.Printf("No backend found for host: %s", host)
		http.Error(w, "No backend available for this host", http.StatusNotFound)
		return
	}

	if backend.Health.Healthy == false {
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

	p.CopyHeaders(proxyReq.Header, r.Header)
	p.AddProxyHeaders(*proxyReq)

	resp, err := p.client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Backend unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	p.StreamResponse(w, resp)
}

func sendReloadRequest(configPath string, endpoint string) error {
	url := fmt.Sprintf("%s/__groxy_reload", endpoint)
	payload := map[string]string{
		"config_path": configPath,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to connect to groxy server on port %s: %v", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("reload failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func (p *Proxy) ReloadHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow from localhost
	clientIP := strings.Split(r.RemoteAddr, ":")[0]
	if clientIP != "127.0.0.1" && clientIP != "::1" {
		http.Error(w, "Forbidden - reload only allowed from localhost", http.StatusForbidden)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get config path from request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var reloadReq struct {
		ConfigPath string `json:"config_path"`
	}

	if err := json.Unmarshal(body, &reloadReq); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if reloadReq.ConfigPath == "" {
		http.Error(w, "config_path is required", http.StatusBadRequest)
		return
	}

	// Reload config
	log.Printf("Reloading config from: %s", reloadReq.ConfigPath)
	if err := p.reloadConfig(reloadReq.ConfigPath); err != nil {
		log.Printf("Failed to reload config: %v", err)
		http.Error(w, fmt.Sprintf("Reload failed: %v", err), http.StatusInternalServerError)
		return
	}

	log.Println("Config reloaded successfully")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Config reloaded successfully",
	})
}

func (p *Proxy) reloadConfig(configPath string) error {
	config, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	// Create new router with updated config
	router := NewRouter(config)
	allBackends := router.getAllBackends()

	// Update proxy configuration atomically
	p.mu.Lock()
	defer p.mu.Unlock()

	p.router = router
	p.hc.Backends = allBackends
	p.ReadTimeout = time.Duration(config.Proxy.ReadTimeout) * time.Second
	p.WriteTimeout = time.Duration(config.Proxy.WriteTimeout) * time.Second

	return nil
}

func handleReload(oldConfig *Config) {
	fs := flag.NewFlagSet("reload", flag.ExitOnError)
	configFile := fs.String("c", "", "Config file path")
	// Skip -config flag parsing as it's already handled in main
	args := []string{}
	skip := false
	for _, arg := range os.Args[2:] {
		if skip {
			skip = false
			continue
		}
		if arg == "-config" {
			skip = true
			continue
		}
		args = append(args, arg)
	}
	fs.Parse(args)

	if *configFile == "" {
		fmt.Println("Error: -c flag is required")
		fmt.Println("Usage: groxy reload -c <config-file> [-p <port>]")
		os.Exit(1)
	}

	// Resolve config file path
	configPath, err := filepath.Abs(*configFile)
	if err != nil {
		fmt.Printf("Error resolving config path: %v\n", err)
		os.Exit(1)
	}

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("Error: Config file does not exist: %s\n", configPath)
		os.Exit(1)
	}

	// Send reload request
	endpoint := fmt.Sprintf("http://localhost:%d", oldConfig.Proxy.Port)
	if err := sendReloadRequest(configPath, endpoint); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✓ Config reloaded successfully")
}

func handleStatus(config *Config) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	// Skip -config flag parsing as it's already handled in main
	args := []string{}
	skip := false
	for _, arg := range os.Args[2:] {
		if skip {
			skip = false
			continue
		}
		if arg == "-config" {
			skip = true
			continue
		}
		args = append(args, arg)
	}
	fs.Parse(args)

	endpoint := fmt.Sprintf("http://localhost:%d/metrics", config.Proxy.Port)
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get(endpoint)
	if err != nil {
		fmt.Printf("Error: Failed to connect to groxy server on port %d: %v\n", config.Proxy.Port, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("Error: Server returned status %d\n", resp.StatusCode)
		os.Exit(1)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error: Failed to read response: %v\n", err)
		os.Exit(1)
	}

	var metrics map[string]any
	if err := json.Unmarshal(body, &metrics); err != nil {
		fmt.Printf("Error: Failed to parse response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Groxy Status (Port %d):\n", config.Proxy.Port)
	fmt.Printf("Timestamp: %v\n", time.Unix(int64(metrics["timestamp"].(float64)), 0))

	if backends, ok := metrics["backends"].([]any); ok {
		fmt.Printf("Backends (%d):\n", len(backends))
		for i, backend := range backends {
			if b, ok := backend.(map[string]any); ok {
				healthy := "✗ Unhealthy"
				if b["healthy"].(bool) {
					healthy = "✓ Healthy"
				}
				fmt.Printf("  %d. %s - %s (failures: %.0f)\n",
					i+1, b["url"], healthy, b["failures"])
			}
		}
	}
}

func handleCommand(config *Config) {
	command := os.Args[1]

	switch command {
	case "reload":
		handleReload(config)
	case "status":
		handleStatus(config)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Available commands:")
		fmt.Println("  reload -c <config-file> [-p <port>]")
		fmt.Println("  status [-p <port>]")
		os.Exit(1)
	}
}

func main() {
	// Check if it's a command first
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		// For commands, we need to handle config flag manually
		configPath := "./groxy.json"
		for i, arg := range os.Args {
			if arg == "-config" && i+1 < len(os.Args) {
				configPath = os.Args[i+1]
				break
			}
		}

		if strings.HasPrefix(configPath, "~/") {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				log.Fatalf("ERROR: Unable to get home directory: %v\n", err)
			}
			configPath = filepath.Join(homeDir, configPath[2:])
		}

		absFilePath, err := filepath.Abs(configPath)
		if err != nil {
			log.Fatalf("ERROR: Unable to resolve config file path: %v\n", err)
		}

		config, err := LoadConfig(absFilePath)
		if err != nil {
			log.Fatalf("ERROR: Unable to load the file %s\n", err.Error())
		}

		handleCommand(config)
		return
	}

	// Normal server mode
	configFile := flag.String("config", "~/.config/groxy.json", "Configuration file for the proxy / reverse proxy")
	flag.Parse()

	configPath := *configFile
	if strings.HasPrefix(configPath, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("ERROR: Unable to get home directory: %v\n", err)
		}
		configPath = filepath.Join(homeDir, configPath[2:])
	}

	absFilePath, err := filepath.Abs(configPath)
	if err != nil {
		log.Fatalf("ERROR: Unable to resolve config file path: %v\n", err)
	}
	config, err := LoadConfig(absFilePath)
	if err != nil {
		log.Fatalf("ERROR: Unable to load the file %s\n", err.Error())
	}

	groxyURL := fmt.Sprintf(":%d", config.Proxy.Port)
	proxy := NewReverseProxy(config)
	go proxy.hc.StartHealthCheck()

	// Setup graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.HandleFunc("/", proxy.ServeHTTP)
	mux.HandleFunc("/metrics", proxy.MetricsHandler)
	mux.HandleFunc("/__groxy_reload", proxy.ReloadHandler)

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
