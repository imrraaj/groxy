package main

import (
	"strings"
	"sync"
	"time"
)

type Route struct {
	Lb   LoadBalancer
	Host string
}

type Router struct {
	routes map[string]*Route
	mu     sync.RWMutex
}

func NewRouter(config *Config) *Router {
	routes := make(map[string]*Route)
	for _, cRoute := range config.Routes {
		rBackends := make([]*Backend, 0, len(cRoute.Backends))
		for _, b := range cRoute.Backends {
			bk := Backend{
				Protocol: Protocol(b.Protocol),
				Host:     b.Host,
				Port:     b.Port,
				Health:   NewBackendHealth(b.Health.Path, b.Health.Interval*time.Second),
			}
			rBackends = append(rBackends, &bk)
		}
		newRoute := Route{
			Lb:   LoadBalancer{backends: rBackends, last: 0},
			Host: cRoute.Host,
		}
		routes[newRoute.Host] = &newRoute
	}

	return &Router{
		routes: routes,
	}
}

func (r *Router) matchesWildcard(pattern, host string) bool {
	if strings.HasPrefix(pattern, "*.") {
		suffix := pattern[2:]
		return strings.HasSuffix(host, suffix)
	}
	return false
}

// No mutex is required since router is a read only struct which is initialised before the server starts
// While reloading the config entire proxy is locked momentarily so no locks required on router
func (r *Router) GetBackend(host string) *Backend {
	// Try exact host match first
	if route, exists := r.routes[host]; exists {
		return route.Lb.Next()
	}

	// Try wildcard matching (e.g., *.example.com)
	for routeHost, route := range r.routes {
		if r.matchesWildcard(routeHost, host) {
			return route.Lb.Next()
		}
	}

	return nil
}

func (r *Router) getAllBackends() []*Backend {
	var allBackends []*Backend
	for _, route := range r.routes {
		if route != nil {
			allBackends = append(allBackends, route.Lb.backends...)
		}
	}

	return allBackends
}
