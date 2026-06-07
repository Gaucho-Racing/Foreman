package service

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/pkg/logger"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestMain spins up a real Postgres via testcontainers-go and points
// the package's database.DB global at it. All service-level tests in
// this package run against the same container — cheaper than one
// container per test, and resetDB() between tests gives each test a
// clean slate.
//
// Requires a Docker daemon on the runner. GH Actions ubuntu-latest
// has one out of the box; locally use Colima / Docker Desktop / etc.
func TestMain(m *testing.M) {
	ctx := context.Background()

	logger.Init(false)

	container, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("foreman_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2). // logs twice during init; wait for the runtime one
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres testcontainer start failed:", err)
		os.Exit(1)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintln(os.Stderr, "container host:", err)
		os.Exit(1)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintln(os.Stderr, "container port:", err)
		os.Exit(1)
	}

	// Point the production code at the test container by setting the
	// same env vars main.go reads. Both paths flow through config.Verify
	// + database.Init so we exercise the same boot code under test.
	os.Setenv("ENV", "DEV")
	os.Setenv("DATABASE_HOST", host)
	os.Setenv("DATABASE_PORT", port.Port())
	os.Setenv("DATABASE_USER", "postgres")
	os.Setenv("DATABASE_PASSWORD", "password")
	os.Setenv("DATABASE_NAME", "foreman_test")
	config.DatabaseHost = host
	config.DatabasePort = port.Port()
	config.DatabaseUser = "postgres"
	config.DatabasePassword = "password"
	config.DatabaseName = "foreman_test"
	config.Verify()
	database.Init()

	code := m.Run()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// resetDB wipes all rows between tests. Faster + simpler than tearing
// the container down per-test. RESTART IDENTITY isn't strictly needed
// (we use ULIDs) but doesn't hurt.
func resetDB(t *testing.T) {
	t.Helper()
	sql := fmt.Sprintf("TRUNCATE %s, %s, %s RESTART IDENTITY CASCADE",
		model.TableJobs(), model.TableJobRuns(), model.TableSchedules())
	if err := database.DB.Exec(sql).Error; err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// mustEnqueue is a test helper that enqueues a vanilla job and fails
// the test on any error. Most tests just need "a pending job" — this
// keeps the boilerplate out of the test body.
func mustEnqueue(t *testing.T, kind string, opts ...func(*EnqueueParams)) model.Job {
	t.Helper()
	p := EnqueueParams{
		Kind:        kind,
		MaxAttempts: 1,
	}
	for _, opt := range opts {
		opt(&p)
	}
	job, err := Enqueue(p)
	if err != nil {
		t.Fatalf("enqueue %s: %v", kind, err)
	}
	return job
}

func withMaxAttempts(n int) func(*EnqueueParams) {
	return func(p *EnqueueParams) { p.MaxAttempts = n }
}

func withIdempotencyKey(k string) func(*EnqueueParams) {
	return func(p *EnqueueParams) { p.IdempotencyKey = &k }
}

func withScheduledAt(at time.Time) func(*EnqueueParams) {
	return func(p *EnqueueParams) { p.ScheduledAt = &at }
}
