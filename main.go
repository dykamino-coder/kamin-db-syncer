// kamin-db-syncer: object-level MSSQL sync (schema -> git, schema publish, low-bandwidth table data sync)
// with a scheduler, worker pool, MongoDB state and a web UI. One binary; SqlPackage and git are
// called as external tools.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadSettings()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	st, err := newStore(ctx, cfg.MongoURI, cfg.MongoDB, cfg.RetentionDays)
	if err != nil {
		return err
	}
	if err := initCredentials(ctx, st); err != nil {
		return err
	}
	res := newResources()
	go res.Run(ctx.Done())
	eng := newEngine(cfg, st, res)
	if err := eng.SeedFromEnv(ctx); err != nil {
		return err
	}
	if err := eng.RefreshNextRuns(ctx); err != nil {
		return err
	}
	if cfg.UIPassword == "" {
		slog.Warn("UI_PASSWORD is not set: web UI is open without authentication")
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return err
	}
	eng.Start(ctx)

	srv := &http.Server{Addr: cfg.Listen, Handler: (&API{cfg: cfg, store: st, eng: eng, res: res}).routes(),
		ReadHeaderTimeout: 10 * time.Second}
	go func() {
		slog.Info("listening", "addr", cfg.Listen, "pod", cfg.Pod, "workers", cfg.MaxParallel)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	eng.Wait() // running syncs were cancelled with ctx and recorded as failed
	return nil
}
