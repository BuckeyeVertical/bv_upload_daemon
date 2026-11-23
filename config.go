package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pelletier/go-toml/v2"
)

type config struct {
	Directory    string  `toml:"directory"`
	Bucket       string  `toml:"bucket"`
	Region       string  `toml:"region"`
	Prefix       string  `toml:"prefix"`
	SoftCapGB    float64 `toml:"soft_cap_gb"`
	ScanInterval string  `toml:"scan_interval"`
	SettleTime   string  `toml:"settle_time"`

	capBytes int64
	interval time.Duration
	settle   time.Duration
}

func readConfig(filename string) (config, error) {
	settings := config{ScanInterval: "15s", SettleTime: "30s"}
	file, err := os.Open(filename)
	if err != nil {
		return settings, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()
	if err := toml.NewDecoder(file).DisallowUnknownFields().Decode(&settings); err != nil {
		return settings, fmt.Errorf("read config: %w", err)
	}
	if strings.TrimSpace(settings.Directory) == "" || strings.TrimSpace(settings.Bucket) == "" ||
		strings.TrimSpace(settings.Region) == "" {
		return settings, fmt.Errorf("directory, bucket, and region are required")
	}
	if !filepath.IsAbs(settings.Directory) {
		settings.Directory = filepath.Join(filepath.Dir(filename), settings.Directory)
	}
	absolute, err := filepath.Abs(settings.Directory)
	if err != nil {
		return settings, fmt.Errorf("resolve directory: %w", err)
	}
	settings.Directory, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return settings, fmt.Errorf("resolve directory: %w", err)
	}
	settings.Prefix = strings.Trim(settings.Prefix, "/")
	if settings.Prefix != "" {
		for part := range strings.SplitSeq(settings.Prefix, "/") {
			if part == "" || part == "." || part == ".." || !utf8.ValidString(part) {
				return settings, fmt.Errorf("invalid S3 prefix %q", settings.Prefix)
			}
		}
	}
	bytes := settings.SoftCapGB * 1_000_000_000
	if math.IsNaN(bytes) || bytes < 0 || bytes >= math.Exp2(63) || (bytes > 0 && bytes < 1) {
		return settings, fmt.Errorf("soft_cap_gb must be zero or specify a limit between one byte and 2^63 bytes")
	}
	settings.capBytes = int64(bytes)
	settings.interval, err = time.ParseDuration(settings.ScanInterval)
	if err != nil || settings.interval <= 0 {
		return settings, fmt.Errorf("scan_interval must be a positive duration such as 15s")
	}
	settings.settle, err = time.ParseDuration(settings.SettleTime)
	if err != nil || settings.settle < 0 {
		return settings, fmt.Errorf("settle_time must be a nonnegative duration such as 30s")
	}
	return settings, nil
}
