package pgx

import (
	"context"
	"fmt"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Config struct {
	Schema   string
	Host     string
	Port     string
	User     string
	Password string
	Database string
	Trace    bool
}

func (c Config) Dsn() string {
	return fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		c.Schema,
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)
}

func Open(config Config) (*pgxpool.Pool, error) {
	var (
		conn *pgxpool.Pool
		err  error
	)

	cfg, err := pgxpool.ParseConfig(config.Dsn())
	if nil != err {
		return nil, err
	}

	cfg.MaxConns = 7
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = time.Minute * 30
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = time.Minute
	if config.Trace == true {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}
	currentWaitTime := 2
	trialCount := 0

	for conn == nil && trialCount < 5 {
		trialCount++
		conn, err = pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			if trialCount == 5 {
				return nil, fmt.Errorf("connect to database: %w", err)
			}
			fmt.Println("retrying in", currentWaitTime, "seconds...")
			time.Sleep(time.Duration(currentWaitTime) * time.Second)
			currentWaitTime = currentWaitTime * 1
			conn = nil
		}
	}

	return conn, nil
}
