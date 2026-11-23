package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	configPath := flag.String("config", "config.toml", "configuration file")
	once := flag.Bool("once", false, "scan once and exit")
	dryRun := flag.Bool("dry-run", false, "preview uploads and local cleanup without changing files")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return 0
	}
	if flag.NArg() > 0 {
		flag.Usage()
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := start(*configPath, *once, *dryRun, log); err != nil {
		log.Error(err.Error())
		return 1
	}
	return 0
}

func start(configPath string, once, dryRun bool, log *slog.Logger) error {
	settings, err := readConfig(configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	awsSettings, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(settings.Region))
	if err != nil {
		return fmt.Errorf("load AWS configuration: %w", err)
	}
	root, err := os.OpenRoot(settings.Directory)
	if err != nil {
		return fmt.Errorf("open watched directory: %w", err)
	}
	defer root.Close()
	configInfo, err := os.Stat(configPath)
	if err != nil {
		return fmt.Errorf("check config file: %w", err)
	}
	protected := []fs.FileInfo{configInfo}
	if executable, err := os.Executable(); err == nil {
		if info, err := os.Stat(executable); err == nil {
			protected = append(protected, info)
		}
	}
	if !dryRun {
		lock, err := lockDirectory(root)
		if err != nil {
			return err
		}
		defer lock.Close()
	}

	runner := &runner{
		root:      root,
		store:     &s3Store{s3.NewFromConfig(awsSettings), settings.Bucket, settings.Prefix},
		log:       log,
		capBytes:  settings.capBytes,
		settle:    settings.settle,
		dryRun:    dryRun,
		protected: protected,
	}
	log.Info("watching directory", "directory", settings.Directory, "bucket", settings.Bucket,
		"prefix", settings.Prefix, "soft_cap_bytes", settings.capBytes, "dry_run", dryRun)
	for {
		err := runner.scanOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if once || dryRun {
			return err
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("scan will retry", "error", err)
		}
		timer := time.NewTimer(settings.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
