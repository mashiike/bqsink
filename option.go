package bqsink

import (
	"errors"
	"fmt"
	"log/slog"

	gax "github.com/googleapis/gax-go/v2"
)

// Validator lets a strategy check its own settings. When a strategy implements
// it, the Option carrying that strategy calls Validate and New fails if it
// returns an error, so a misconfigured strategy is caught before any row is
// written rather than on the first Sink.
type Validator interface {
	Validate() error
}

func validateStrategy(name string, s any) error {
	v, ok := s.(Validator)
	if !ok {
		return nil
	}
	if err := v.Validate(); err != nil {
		return fmt.Errorf("bqsink: %s: %w", name, err)
	}
	return nil
}

type config struct {
	marshalers    *Marshalers
	strategy      MigrationStrategy
	writeStrategy WriteStrategy
	retryPolicy   func() gax.Retryer
	logger        *slog.Logger
}

// Option configures a Sinker at construction time.
//
// Options are the only place bqsink uses this pattern. A strategy is a struct
// instead, so that its settings never look like an Option.
//
// No Option describes the table. What the table looks like belongs to T, through
// its struct tags and its BigQueryTableMetadata method, so that one place answers
// what the table should be. The Options here settle how bqsink behaves around
// that declaration: what to do about a difference, how rows travel, what gets
// logged.
type Option func(*config) error

// WithMarshalers registers per-type overrides of how a Go type becomes a
// BigQuery column, built with MarshalFunc.
//
// They apply both to the schema derived from T's struct tags and to the values
// written, and they win over a type's own FieldMarshaler. Registering the same Go
// type more than once keeps the mapping registered last.
//
// This is not a way to describe the table from outside T. It exists because a
// field's type may come from another package, which cannot be given a
// FieldMarshaler method: the mapping is about encoding a value, not about what the
// table holds. Where a TableDefiner supplies the schema outright the overrides do
// not reach it, since nothing is derived in that case, and they still decide how
// the values are written.
func WithMarshalers(marshalers ...*Marshalers) Option {
	return func(c *config) error {
		joined := joinMarshalers(marshalers)
		if joined == nil {
			return errors.New("bqsink: WithMarshalers: no marshaler was given")
		}
		if c.marshalers == nil {
			c.marshalers = &Marshalers{}
		}
		c.marshalers.merge(joined)
		return nil
	}
}

// WithMigrationStrategy selects the migration strategy. The default is
// AppendNewColumns{CreateIfMissing: true}: keeping the table in step with the
// declaration is what bqsink is for, and adding a column is not destructive.
//
// Pass MigrationNone{} to leave an existing table alone, or SyncAllColumns{} to
// also drop columns the declaration no longer has.
//
// If s implements Validator, its Validate method decides whether the settings are
// usable and New fails when they are not.
func WithMigrationStrategy(s MigrationStrategy) Option {
	return func(c *config) error {
		if s == nil {
			return errors.New("bqsink: WithMigrationStrategy: strategy is nil")
		}
		if err := validateStrategy("WithMigrationStrategy", s); err != nil {
			return err
		}
		c.strategy = s
		return nil
	}
}

// WithWriteStrategy selects how rows reach BigQuery. The default is
// &StorageWrite{}, which uses the Storage Write API's default stream.
//
// If s implements Validator, its Validate method decides whether the settings are
// usable and New fails when they are not.
func WithWriteStrategy(s WriteStrategy) Option {
	return func(c *config) error {
		if s == nil {
			return errors.New("bqsink: WithWriteStrategy: strategy is nil")
		}
		if err := validateStrategy("WithWriteStrategy", s); err != nil {
			return err
		}
		c.writeStrategy = s
		return nil
	}
}

// WithLogger sends what bqsink has to say to logger, with the relation as an
// attribute on every record.
//
// Without it nothing is logged at all: the default discards every record, so
// bqsink does not write to an embedding program's slog.Default() uninvited.
//
// Three levels are used and Error is not among them, since a failure is returned
// rather than logged.
//
//	Debug  what a transport did: the stream it opened, the rows it flushed, the
//	       difference Migrate found
//	Info   a change bqsink made to the table, and a load job it ran
//	Warn   something bqsink had to let pass: a failure it could not return, or a
//	       difference the migration strategy chose not to reconcile
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) error {
		if logger == nil {
			return errors.New("bqsink: WithLogger: logger is nil")
		}
		c.logger = logger
		return nil
	}
}

// WithRetryPolicy replaces how Migrate retries, in place of DefaultRetryPolicy.
//
// newRetryer is called once per Migrate, because a gax.Retryer carries the state
// of its backoff and cannot be reused. Returning a bare gax.OnErrorFunc places
// no limit on the number of attempts, so Migrate would then keep retrying until
// its context is done; wrap it if a limit is wanted.
func WithRetryPolicy(newRetryer func() gax.Retryer) Option {
	return func(c *config) error {
		if newRetryer == nil {
			return errors.New("bqsink: WithRetryPolicy: newRetryer is nil")
		}
		c.retryPolicy = newRetryer
		return nil
	}
}
