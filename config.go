package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Config struct {
	Proxy struct {
		Port         int `json:"port"`
		ReadTimeout  int `json:"read_timeout,format:sec"`
		WriteTimeout int `json:"write_timeout,format:sec"`
	} `json:"proxy"`
	Backends []struct {
		Protocol string  `json:"protocol"`
		Host     string  `json:"host"`
		Port     int     `json:"port"`
		Weight   float32 `json:"weight"`
		Health   struct {
			Path     string        `json:"path"`
			Interval time.Duration `json:"interval"`
		} `json:"health"`
	} `json:"backends"`

	LoadBalancer struct {
		Algorithm string `json:"algorithm"` // round-robin, weighted, least-conn
	} `json:"load_balancer"`
}

func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("Configuration error: %v", err)
	}

	return &config, err
}

func (config *Config) Validate() error {
	if config.Proxy.Port <= 0 || config.Proxy.Port > 65535 {
		return fmt.Errorf("invalid proxy port: %d", config.Proxy.Port)
	}

	if len(config.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}

	for i, backend := range config.Backends {
		if backend.Host == "" {
			return fmt.Errorf("backend %d: host is required", i)
		}
		if backend.Port <= 0 {
			return fmt.Errorf("backend %d: invalid port %d", i, backend.Port)
		}
	}

	return nil
}
