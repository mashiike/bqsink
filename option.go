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
	marshalers     *Marshalers
	strategy       MigrationStrategy
	migrationRetry func() gax.Retryer
	logger         *slog.Logger
}

// Option configures a Sinker at construction time.
//
// Options are the only place bqsink uses this pattern. A migration strategy is a
// struct instead, so that its settings never look like an Option, and how rows
// travel is settled on the writer rather than here.
//
// No Option describes the table. What the table looks like belongs to the row
// type, through its struct tags and its BigQueryTableMetadata method, and reaches
// NewSinker as a Declaration, so that one place answers what the table should be.
// The Options here settle how bqsink behaves around that declaration: what to do
// about a difference from the real table, and what gets logged.
type Option func(*config) error

// WithMarshalers registers per-type overrides of how a Go type becomes a
// BigQuery column, built with MarshalFunc.
//
// They apply both to the schema derived from the row type's struct tags and to the values
// written, and they win over a type's own FieldMarshaler. Registering the same Go
// type more than once keeps the mapping registered last.
//
// This is not a way to describe the table from outside the row type. It exists because a
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

// WithMigrationStrategy selects the migration strategy and how the migration retries.
//
// Without it the strategy is AppendNewColumns{CreateIfMissing: true} and the
// retries are DefaultRetryPolicy's: keeping the table in step with the declaration
// is what bqsink is for, adding a column is not destructive, and two replicas
// deploying at once add the same column at once, which BigQuery reports as a failed
// precondition that only a retry gets past.
//
// Pass MigrationNone{} to leave an existing table alone, or SyncAllColumns{} to
// also drop columns the declaration no longer has.
//
// retryPolicy says what to do about a failure a later attempt could get past, such
// as that failed precondition. A nil retryPolicy means the migration attempts the change
// once; pass DefaultRetryPolicy to keep the retries bqsink would otherwise use.
// It is called once per migration, because a gax.Retryer carries the state of its
// backoff and cannot be reused, and returning a bare gax.OnErrorFunc places no
// limit on the number of attempts.
//
// Writing is not retried from here: that is the write strategy's own business,
// since it is the one holding the rows while they are in flight.
//
// If s implements Validator, its Validate method decides whether the settings are
// usable and New fails when they are not.
func WithMigrationStrategy(s MigrationStrategy, retryPolicy func() gax.Retryer) Option {
	return func(c *config) error {
		if s == nil {
			return errors.New("bqsink: WithMigrationStrategy: strategy is nil")
		}
		if err := validateStrategy("WithMigrationStrategy", s); err != nil {
			return err
		}
		c.strategy = s
		c.migrationRetry = retryPolicy
		return nil
	}
}

// WithLogger sends what bqsink has to say to logger, with the relation as an
// attribute on every record.
//
// Without it nothing is logged at all: the default discards every record, so
// bqsink does not write to an embedding program's slog.Default() uninvited.
//
// The writer is handed the same logger where it has something to log, so one call
// covers both sides.
//
// Three levels are used and Error is not among them, since a failure is returned
// rather than logged.
//
//	Debug  what a transport did: the stream it opened, the rows it wrote, the
//	       difference the migration found
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
