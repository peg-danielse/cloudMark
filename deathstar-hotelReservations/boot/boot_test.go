package boot

import (
	"path/filepath"
	"testing"
)

func TestConfigFileMatchesDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join("..", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := defaultConfig()
	if cfg.Jaeger != want.Jaeger || cfg.Mongo != want.Mongo {
		t.Fatalf("infra = jaeger %s mongo %s", cfg.Jaeger, cfg.Mongo)
	}
	for name, addr := range want.Services {
		if cfg.Services[name] != addr {
			t.Fatalf("%s = %s, want %s", name, cfg.Services[name], addr)
		}
	}
	for name, addr := range want.Memcached {
		if cfg.Memcached[name] != addr {
			t.Fatalf("memcached %s = %s, want %s", name, cfg.Memcached[name], addr)
		}
	}
}

func TestHostPort(t *testing.T) {
	host, port, err := defaultConfig().HostPort("srv-profile")
	if err != nil {
		t.Fatal(err)
	}
	if host != "127.0.0.1" || port != 8081 {
		t.Fatalf("profile = %s:%d", host, port)
	}
	if _, _, err := defaultConfig().HostPort("missing"); err == nil {
		t.Fatal("expected error for unknown service")
	}
}
