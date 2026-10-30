package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/rachmanzz/buckboat/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Buckboat stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := server.DefaultConfig()
	cfg.S3Addr = envOr("BUCKBOAT_S3_ADDR", cfg.S3Addr)
	cfg.DocumentAddr = envOr("BUCKBOAT_DOCUMENT_ADDR", cfg.DocumentAddr)
	cfg.HealthAddr = envOr("BUCKBOAT_HEALTH_ADDR", cfg.HealthAddr)
	cfg.DataDir = envOr("BUCKBOAT_DATA_DIR", cfg.DataDir)
	cfg.CatalogAdapter = envOr("BUCKBOAT_CATALOG_ADAPTER", cfg.CatalogAdapter)
	cfg.AccessKeyID = os.Getenv("BUCKBOAT_ACCESS_KEY_ID")
	cfg.SecretAccessKey = os.Getenv("BUCKBOAT_SECRET_ACCESS_KEY")
	cfg.Region = envOr("BUCKBOAT_REGION", cfg.Region)
	if value := os.Getenv("BUCKBOAT_MAX_OBJECT_BYTES"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 {
			return errors.New("BUCKBOAT_MAX_OBJECT_BYTES must be a positive integer")
		}
		cfg.MaxObjectBytes = parsed
	}
	if value := os.Getenv("BUCKBOAT_MAX_CONCURRENT_PUTS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return errors.New("BUCKBOAT_MAX_CONCURRENT_PUTS must be a positive integer")
		}
		cfg.MaxConcurrentPuts = parsed
	}
	if value := os.Getenv("BUCKBOAT_MAX_CONCURRENT_READS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return errors.New("BUCKBOAT_MAX_CONCURRENT_READS must be a positive integer")
		}
		cfg.MaxConcurrentReads = parsed
	}

	runtime, err := server.New(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			log.Printf("close local object store: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf(
		"starting Buckboat single-node local profile: s3=%s document=%s health=%s region=%s data_dir=%s",
		cfg.S3Addr,
		cfg.DocumentAddr,
		cfg.HealthAddr,
		cfg.Region,
		cfg.DataDir,
	)

	if err := runtime.Run(ctx); err != nil {
		return fmt.Errorf("server stopped: %w", err)
	}
	return nil
}

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
