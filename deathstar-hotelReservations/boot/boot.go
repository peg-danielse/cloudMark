package boot

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
)

var advertisePort int

func init() {
	flag.IntVar(&advertisePort, "advertise-port", 0, "Port this service listens on and advertises")
}

// ListenPort returns the advertisement port when one was passed, otherwise the configured port.
func ListenPort(configured int) int {
	if advertisePort > 0 {
		return advertisePort
	}
	return configured
}

// Bind is the TCP address a service listens on.
func Bind(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// Config is the localhost address of each process.
type Config struct {
	Jaeger    string            `json:"jaeger"`
	Mongo     string            `json:"mongo"`
	Memcached map[string]string `json:"memcached"`
	Services  map[string]string `json:"services"`
}

func defaultConfig() Config {
	return Config{
		Jaeger: "127.0.0.1:6831",
		Mongo:  "127.0.0.1:27017",
		Memcached: map[string]string{
			"profile":     "127.0.0.1:11211",
			"rate":        "127.0.0.1:11212",
			"reservation": "127.0.0.1:11213",
			"review":      "127.0.0.1:11214",
		},
		Services: map[string]string{
			"frontend":           "127.0.0.1:5000",
			"srv-profile":        "127.0.0.1:8081",
			"srv-search":         "127.0.0.1:8082",
			"srv-geo":            "127.0.0.1:8083",
			"srv-rate":           "127.0.0.1:8084",
			"srv-recommendation": "127.0.0.1:8085",
			"srv-user":           "127.0.0.1:8086",
			"srv-reservation":    "127.0.0.1:8087",
			"srv-review":         "127.0.0.1:8088",
			"srv-attractions":    "127.0.0.1:8089",
		},
	}
}

// Load reads config.json from the working directory. Missing values keep the localhost defaults.
func Load() Config {
	cfg, err := LoadFile("config.json")
	if err != nil {
		return defaultConfig()
	}
	return cfg
}

// LoadFile reads a config file and fills any missing addresses from the localhost defaults.
func LoadFile(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()

	file := Config{}
	if err := json.NewDecoder(f).Decode(&file); err != nil {
		return Config{}, err
	}
	return apply(defaultConfig(), file), nil
}

func apply(base, file Config) Config {
	if file.Jaeger != "" {
		base.Jaeger = file.Jaeger
	}
	if file.Mongo != "" {
		base.Mongo = file.Mongo
	}
	for k, v := range file.Memcached {
		if v != "" {
			base.Memcached[k] = v
		}
	}
	for k, v := range file.Services {
		if v != "" {
			base.Services[k] = v
		}
	}
	return base
}

// Address is the host:port configured for a service name.
func (c Config) Address(name string) (string, error) {
	addr := c.Services[name]
	if addr == "" {
		return "", fmt.Errorf("no address for %s", name)
	}
	return addr, nil
}

// HostPort splits the configured address for name.
func (c Config) HostPort(name string) (string, int, error) {
	addr, err := c.Address(name)
	if err != nil {
		return "", 0, err
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("service %s: %w", name, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return "", 0, fmt.Errorf("service %s: invalid port %q", name, portStr)
	}
	return host, port, nil
}
