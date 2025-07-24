package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
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
}

func NewBackendHealth(url string) BackendHealth {
	return BackendHealth{
		HealthCheckURL: url,
		Timeout:        time.Second,
		Healthy:        false,
		LastCheck:      time.Now(),
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
	if err != nil {
		this.Bh.Failures++
		this.Bh.Healthy = false
		this.Bh.LastCheck = time.Now()
		log.Printf("Health check failed for %s: %v", this.Stringify(), err)
		return
	}
	defer resp.Body.Close()

	log.Printf("%s: %d\n", this.HealthCheckURL(), resp.StatusCode)

	this.Bh.LastCheck = time.Now()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		this.Bh.Healthy = true
		this.Bh.Failures = 0
		log.Println("set healthy")
	} else {
		this.Bh.Failures++
		this.Bh.Healthy = false
		log.Printf("Health check failed for %s: %v", this.Stringify(), err)
	}
}

func (this *Backend) IsHealthy() bool {
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
	lb     LoadBalancer
	client *http.Client

	hc HealthChecker
}

func NewReverseProxy(backends []*Backend) *Proxy {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 5,
		IdleConnTimeout:     10 * time.Second,
		DisableCompression:  true,
	}
	hc := HealthChecker{
		Backends: backends,
		Interval: time.Second * 5,
		Timeout:  time.Second,
	}
	return &Proxy{
		lb: LoadBalancer{
			backends: backends,
			last:     0,
		},
		client: &http.Client{Transport: transport},
		hc:     hc,
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

func (this *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	backend := this.lb.Next()
	log.Printf("%s: %s", backend.HealthCheckURL(), backend.Bh.Healthy)
	if backend != nil && backend.Bh.Healthy == false {
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
	mux := http.NewServeMux()
	backends := []*Backend{
		{Protocol: HTTP, Host: "localhost", Port: 8001, Bh: NewBackendHealth("/health")},
		{Protocol: HTTP, Host: "localhost", Port: 8002, Bh: NewBackendHealth("/health")},
	}
	proxy := NewReverseProxy(backends)
	go proxy.hc.StartHealthCheck()
	mux.HandleFunc("/", proxy.ServeHTTP)

	if err := http.ListenAndServe(":6969", mux); err != nil {
		log.Fatalln("ERROR: ", err.Error())
	}
}
