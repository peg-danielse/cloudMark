package dialer

import (
	"testing"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
)

func TestServiceAddrUsesConfig(t *testing.T) {
	cfg := boot.Config{Services: map[string]string{
		"srv-profile": "127.0.0.1:8081",
		"srv-geo":     "127.0.0.1:8083",
	}}
	got, err := ServiceAddr(cfg, "srv-profile")
	if err != nil {
		t.Fatal(err)
	}
	if got != "127.0.0.1:8081" {
		t.Fatalf("profile = %s, want 127.0.0.1:8081", got)
	}
	got, err = ServiceAddr(cfg, "srv-geo")
	if err != nil {
		t.Fatal(err)
	}
	if got != "127.0.0.1:8083" {
		t.Fatalf("geo = %s, want 127.0.0.1:8083", got)
	}
}

func TestServiceAddrRejectsUnknown(t *testing.T) {
	if _, err := ServiceAddr(boot.Config{}, "srv-missing"); err == nil {
		t.Fatal("expected error for unknown service")
	}
}
