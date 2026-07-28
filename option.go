package bqsink

import (
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
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
	schema        bigquery.Schema
	metadata      *bigquery.TableMetadata
	marshalers    *Marshalers
	strategy      MigrationStrategy
	writeStrategy WriteStrategy
	retryPolicy   func() gax.Retryer
}

// Option configures a Sinker at construction time.
//
// Options are the only place bqsink uses this pattern. A strategy is a struct
// instead, so that its settings never look like an Option.
type Option func(*config) error

// WithSchema uses schema instead of deriving one from T's struct tags.
//
// Use it for columns struct tags cannot express, such as BIGNUMERIC precision,
// column descriptions or policy tags.
func WithSchema(schema bigquery.Schema) Option {
	return func(c *config) error {
		if len(schema) == 0 {
			return errors.New("bqsink: WithSchema: schema is empty")
		}
		c.schema = schema
		return nil
	}
}

// WithTableMetadata uses md for table level settings instead of T's
// BigQueryTableMetadata method.
//
// If md.Schema is set it also takes the place of the schema derived from struct
// tags, so WithSchema is unnecessary.
func WithTableMetadata(md *bigquery.TableMetadata) Option {
	return func(c *config) error {
		if md == nil {
			return errors.New("bqsink: WithTableMetadata: metadata is nil")
		}
		c.metadata = md
		return nil
	}
}

// WithMarshalers registers per-type overrides of how a Go type becomes a
// BigQuery column, built with MarshalFunc.
//
// They apply both to the schema derived from T's struct tags and to the values
// written, and they win over a type's own FieldMarshaler. Registering the same Go
// type more than once keeps the mapping registered last.
//
// The overrides have no effect when WithSchema or a TableDefiner supplies the
// schema outright, since nothing is derived in that case.
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

// WithMigration selects the migration strategy. The default is
// AppendNewColumns{CreateIfMissing: true}: keeping the table in step with the
// declaration is what bqsink is for, and adding a column is not destructive.
//
// Pass MigrationNone{} to leave an existing table alone, or SyncAllColumns{} to
// also drop columns the declaration no longer has.
//
// If s implements Validator, its Validate method decides whether the settings are
// usable and New fails when they are not.
func WithMigration(s MigrationStrategy) Option {
	return func(c *config) error {
		if s == nil {
			return errors.New("bqsink: WithMigration: strategy is nil")
		}
		if err := validateStrategy("WithMigration", s); err != nil {
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
