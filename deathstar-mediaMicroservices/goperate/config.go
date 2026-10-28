package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// serviceConfig is config/service-config.json: one {addr, port} per key.
type serviceConfig map[string]json.RawMessage

func loadConfig(path string) (serviceConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg serviceConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func (c serviceConfig) address(key string) (string, error) {
	raw, ok := c[key]
	if !ok {
		return "", fmt.Errorf("config has no %s", key)
	}
	var ep struct {
		Addr string `json:"addr"`
		Port int    `json:"port"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil || ep.Port <= 0 {
		return "", fmt.Errorf("config %s has no port", key)
	}
	if ep.Addr == "" {
		ep.Addr = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", ep.Addr, ep.Port), nil
}

// mediaRoot is the deathstar-mediaMicroservices tree: $MEDIA_ROOT, else the checkout on the benchmark host.
func mediaRoot() string {
	if v := os.Getenv("MEDIA_ROOT"); v != "" {
		return v
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Mercury", "benchmarks", "deathstar-mediaMicroservices")
}

// movie is one entry of the seeded dataset. The page service keys movies by id; compose writes reviews by title.
type movie struct {
	id    string
	title string
}

// loadMovies returns the movies the seed registered (datasets/tmdb/movies.json).
func loadMovies(path string) ([]movie, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		ID    int64  `json:"id"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	movies := make([]movie, 0, len(raw))
	for _, m := range raw {
		movies = append(movies, movie{id: strconv.FormatInt(m.ID, 10), title: m.Title})
	}
	if len(movies) == 0 {
		return nil, fmt.Errorf("%s has no movies", path)
	}
	return movies, nil
}
