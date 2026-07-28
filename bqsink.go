// Package bqsink writes rows to BigQuery and keeps the destination table's
// schema in sync with a schema declared in Go code.
//
// The schema is declared up front, either by struct tags on T or by an explicit
// bigquery.Schema. The declaration is the source of truth: the real table
// follows it. bqsink never infers a schema from the data being written.
package bqsink

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
)

var (
	// ErrNotImplemented reports that a code path has not been implemented yet.
	ErrNotImplemented = errors.New("bqsink: not implemented")

	// ErrSchemaConflict reports that reconciling the declared schema with the
	// real table needs a change BigQuery does not allow, such as altering a
	// column's type or making a NULLABLE column REQUIRED.
	ErrSchemaConflict = errors.New("bqsink: schema conflict")

	// ErrTableMissing reports that the table does not exist and the migration
	// strategy did not ask for it to be created. Set CreateIfMissing on the
	// strategy to create it.
	ErrTableMissing = errors.New("bqsink: table does not exist")
)

// TableDefiner lets a row type declare table level settings such as
// partitioning, clustering, labels and expiration.
//
// The returned metadata's Schema field, if set, overrides the schema derived
// from struct tags. Read-only fields (ETag, CreationTime, LastModifiedTime,
// NumBytes, NumRows, FullID, Type) are ignored.
type TableDefiner interface {
	BigQueryTableMetadata() *bigquery.TableMetadata
}

// tableAPI is the part of *bigquery.Table that bqsink calls. Keeping it as an
// interface lets the migration path be tested without BigQuery.
type tableAPI interface {
	Metadata(ctx context.Context, opts ...bigquery.TableMetadataOption) (*bigquery.TableMetadata, error)
	Update(ctx context.Context, tm bigquery.TableMetadataToUpdate, etag string, opts ...bigquery.TableUpdateOption) (*bigquery.TableMetadata, error)
	Create(ctx context.Context, tm *bigquery.TableMetadata) error
}

// queryRunner runs a DDL statement, which dropping a column needs. It is an
// interface for the same reason tableAPI is.
type queryRunner interface {
	run(ctx context.Context, sql string) error
}

type clientQueryRunner struct {
	client *bigquery.Client
}

func (r clientQueryRunner) run(ctx context.Context, sql string) error {
	job, err := r.client.Query(sql).Run(ctx)
	if err != nil {
		return fmt.Errorf("run statement: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait for job %s: %w", job.ID(), err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("job %s failed: %w", job.ID(), err)
	}
	return nil
}

// Sinker writes rows of type T to a single BigQuery table.
//
// A Sinker is safe for concurrent use.
type Sinker[T any] struct {
	client        *bigquery.Client
	relation      Relation
	table         *bigquery.Table
	api           tableAPI
	query         queryRunner
	plan          *rowPlan
	schema        bigquery.Schema
	metadata      *bigquery.TableMetadata
	strategy      MigrationStrategy
	writeStrategy WriteStrategy
	retryPolicy   func() gax.Retryer

	sinkerID  string
	createdAt time.Time
	filler    fillerFunc[T]
	now       func() time.Time

	migrateOnce sync.Once
	migrateErr  error

	writerMu  sync.Mutex
	rowWriter RowWriter
	writerErr error
}

// New returns a Sinker that writes rows of type T to the table relation names.
//
// The schema comes from T's struct tags, following the rules described on
// InferSchema, unless WithSchema is given. Table level settings come from T's
// BigQueryTableMetadata method if T implements TableDefiner, unless
// WithTableMetadata is given.
//
// T must be mappable to a row even when the schema is given outright, since its
// fields are what gets written. When the schema is given, every column the struct
// would write has to be present in it; New fails otherwise, rather than letting
// BigQuery reject the first write.
//
// Without options the migration strategy is AppendNewColumns{CreateIfMissing:
// true}, which creates the table if it is absent and adds columns the declaration
// gained, and the write strategy is &StorageWrite{}. Pass MigrationNone{} to leave
// the table alone.
//
// New does not talk to BigQuery. Reconciliation with the real table happens in
// Migrate, which the first Sink also triggers.
func New[T any](client *bigquery.Client, relation Relation, opts ...Option) (*Sinker[T], error) {
	if client == nil {
		return nil, errors.New("bqsink: client is nil")
	}
	if err := relation.validate(); err != nil {
		return nil, err
	}
	if relation.ProjectID == "" {
		relation.ProjectID = client.Project()
	}
	var c config
	for _, o := range opts {
		if err := o(&c); err != nil {
			return nil, err
		}
	}
	metadata := c.metadata
	if metadata == nil {
		metadata = tableMetadataOf[T]()
	}
	plan, err := buildRowPlan(reflect.TypeFor[T](), c.marshalers)
	if err != nil {
		return nil, err
	}
	schema, err := resolveSchema(c, metadata, plan)
	if err != nil {
		return nil, err
	}
	metadata, err = resolveTableMetadata(metadata, plan)
	if err != nil {
		return nil, err
	}
	strategy := c.strategy
	if strategy == nil {
		strategy = AppendNewColumns{CreateIfMissing: true}
	}
	writeStrategy := c.writeStrategy
	if writeStrategy == nil {
		writeStrategy = &StorageWrite{}
	}
	retryPolicy := c.retryPolicy
	if retryPolicy == nil {
		retryPolicy = DefaultRetryPolicy
	}
	filler, err := rowFillerOf[T]()
	if err != nil {
		return nil, err
	}
	sinkerID, err := newID()
	if err != nil {
		return nil, fmt.Errorf("bqsink: %w", err)
	}
	table := relation.table(client)
	return &Sinker[T]{
		client:        client,
		relation:      relation,
		table:         table,
		api:           table,
		query:         clientQueryRunner{client: client},
		plan:          plan,
		schema:        schema,
		metadata:      metadata,
		strategy:      strategy,
		writeStrategy: writeStrategy,
		retryPolicy:   retryPolicy,
		sinkerID:      sinkerID,
		createdAt:     time.Now(),
		filler:        filler,
		now:           time.Now,
	}, nil
}

func resolveSchema(c config, metadata *bigquery.TableMetadata, plan *rowPlan) (bigquery.Schema, error) {
	declared := c.schema
	if declared == nil && metadata != nil {
		declared = metadata.Schema
	}
	if declared == nil {
		return plan.schema(), nil
	}
	if err := checkPlanAgainstSchema(plan, declared); err != nil {
		return nil, err
	}
	return declared, nil
}

// resolveTableMetadata folds the physical layout the tags describe into the table
// metadata.
//
// Where the tags and the metadata both settle the same thing, that is a
// contradiction rather than a precedence question, so it fails instead of quietly
// preferring one: the declaration is meant to be the single description of the
// table.
func resolveTableMetadata(md *bigquery.TableMetadata, plan *rowPlan) (*bigquery.TableMetadata, error) {
	if plan.partitioning == nil && plan.clustering == nil {
		return md, nil
	}
	out := &bigquery.TableMetadata{}
	if md != nil {
		copied := *md
		out = &copied
	}
	if plan.partitioning != nil {
		switch {
		case out.TimePartitioning != nil:
			return nil, fmt.Errorf("bqsink: %s tags a partitioning column, but the table metadata already sets TimePartitioning; drop one of them",
				plan.goType)
		case out.RangePartitioning != nil:
			return nil, fmt.Errorf("bqsink: %s tags a partitioning column, but the table metadata already sets RangePartitioning; drop one of them",
				plan.goType)
		}
		out.TimePartitioning = plan.partitioning
		if plan.requireFilter {
			out.RequirePartitionFilter = true
		}
	}
	if plan.clustering != nil {
		if out.Clustering != nil {
			return nil, fmt.Errorf("bqsink: %s tags clustering columns, but the table metadata already sets Clustering; drop one of them",
				plan.goType)
		}
		out.Clustering = plan.clustering
	}
	return out, nil
}

// checkPlanAgainstSchema fails when the struct would write a column the declared
// schema does not have. A schema declaring columns the struct does not write is
// allowed: those columns simply stay NULL.
func checkPlanAgainstSchema(plan *rowPlan, schema bigquery.Schema) error {
	declared := make(map[string]bool, len(schema))
	for _, f := range schema {
		declared[f.Name] = true
	}
	var missing []string
	for _, name := range plan.columnNames() {
		if !declared[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("bqsink: the declared schema of %s has no column for %s; add the column or drop the field with `%s:\"-\"`",
		plan.goType, strings.Join(missing, ", "), TagKey)
}

func tableMetadataOf[T any]() *bigquery.TableMetadata {
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Pointer {
		if d, ok := reflect.New(t.Elem()).Interface().(TableDefiner); ok {
			return d.BigQueryTableMetadata()
		}
		return nil
	}
	var zero T
	if d, ok := any(zero).(TableDefiner); ok {
		return d.BigQueryTableMetadata()
	}
	if d, ok := any(&zero).(TableDefiner); ok {
		return d.BigQueryTableMetadata()
	}
	return nil
}

// Schema returns the declared schema.
func (s *Sinker[T]) Schema() bigquery.Schema {
	return s.schema
}

// Table returns the destination table.
func (s *Sinker[T]) Table() *bigquery.Table {
	return s.table
}

// Relation returns the relation naming the destination table. Its ProjectID is
// filled in even when New was given an empty one.
func (s *Sinker[T]) Relation() Relation {
	return s.relation
}

// Migrate reconciles the real table with the declared schema.
//
// It reads the table's state, asks the migration strategy what to change, and
// applies the answer. Migrate returns an error wrapping ErrTableMissing if the
// table does not exist and the strategy did not ask to create it, and one
// wrapping ErrSchemaConflict if reconciling needs a change BigQuery does not
// allow.
//
// Migrate runs once per Sinker and caches its outcome, success or failure alike.
// A later call returns the same error without contacting BigQuery, so recovering
// from a failure means building a new Sinker. Since the work happens on the first
// call, ctx belongs to that caller; a ctx cancelled later has no effect.
//
// Another process changing the same table concurrently, which BigQuery reports
// as a failed ETag precondition or a conflict, is retried here according to the
// retry policy.
//
// Sink calls Migrate on its first invocation, so calling it directly is only
// necessary to apply schema changes ahead of time, such as during a deploy.
func (s *Sinker[T]) Migrate(ctx context.Context) error {
	s.migrateOnce.Do(func() { s.migrateErr = s.migrateWithRetry(ctx) })
	return s.migrateErr
}

// Sink hands v to the write strategy, running Migrate on the first call.
//
// If T implements RowFiller, FillRow is called on a copy of v first, so that the
// row can fill in columns such as a write timestamp. That happens once, before
// the conversion and before any retry.
//
// A transient failure is retried under the retry policy, so a row can reach
// BigQuery more than once. Neither transport bqsink ships deduplicates, so the
// guarantee is at-least-once; IngestionMetadata's _ingestion_row_id is what makes those
// duplicates identifiable.
func (s *Sinker[T]) Sink(ctx context.Context, v T) error {
	w, err := s.writer(ctx)
	if err != nil {
		return err
	}
	row, err := s.prepare(ctx, v)
	if err != nil {
		return err
	}
	return s.retrying(ctx, func(ctx context.Context) error {
		return w.Append(ctx, row)
	})
}

// SinkAll hands every element of vs to the write strategy in order.
//
// Each row is retried on its own, so a failure part way through leaves the rows
// before it handed over.
func (s *Sinker[T]) SinkAll(ctx context.Context, vs ...T) error {
	if len(vs) == 0 {
		return nil
	}
	w, err := s.writer(ctx)
	if err != nil {
		return err
	}
	for _, v := range vs {
		row, err := s.prepare(ctx, v)
		if err != nil {
			return err
		}
		err = s.retrying(ctx, func(ctx context.Context) error {
			return w.Append(ctx, row)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// prepare lets the row fill its own columns in and then converts it. Both happen
// once, before any retry, so a retried row carries the same values.
func (s *Sinker[T]) prepare(ctx context.Context, v T) (map[string]bigquery.Value, error) {
	if s.filler != nil {
		info, err := s.appendInfo()
		if err != nil {
			return nil, err
		}
		if filler := s.filler(&v); filler != nil {
			if err := filler.FillRow(ctx, info); err != nil {
				return nil, fmt.Errorf("bqsink: fill a row of %T: %w", v, err)
			}
		}
	}
	return s.toRow(v)
}

func (s *Sinker[T]) appendInfo() (AppendInfo, error) {
	rowID, err := newID()
	if err != nil {
		return AppendInfo{}, fmt.Errorf("bqsink: %w", err)
	}
	return AppendInfo{
		Relation:        s.relation,
		SinkerID:        s.sinkerID,
		SinkerCreatedAt: s.createdAt,
		RowID:           rowID,
		Time:            s.now(),
	}, nil
}

// retrying runs op under the configured retry policy, the same one Migrate uses.
func (s *Sinker[T]) retrying(ctx context.Context, op func(context.Context) error) error {
	call := func(ctx context.Context, _ gax.CallSettings) error {
		return op(ctx)
	}
	return gax.Invoke(ctx, call, gax.WithRetry(s.retryPolicy))
}

// toRow turns a row of type T into the columns to write, reading the same plan
// the declared schema was derived from.
func (s *Sinker[T]) toRow(v T) (map[string]bigquery.Value, error) {
	return s.plan.marshalRow(reflect.ValueOf(v))
}

// writer opens the RowWriter on first use, after Migrate has settled the schema.
// The outcome is cached the same way Migrate's is.
func (s *Sinker[T]) writer(ctx context.Context) (RowWriter, error) {
	if err := s.Migrate(ctx); err != nil {
		return nil, err
	}
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	if s.rowWriter == nil && s.writerErr == nil {
		s.rowWriter, s.writerErr = s.writeStrategy.Open(ctx, s.table, s.schema)
	}
	return s.rowWriter, s.writerErr
}

// Flush sends the rows the write strategy has buffered. It does nothing when no
// row has been handed over yet.
//
// A transient failure is retried under the retry policy.
//
// Its error reports rows that never reached BigQuery, so it must not be discarded.
func (s *Sinker[T]) Flush(ctx context.Context) error {
	w := s.openedWriter()
	if w == nil {
		return nil
	}
	return s.retrying(ctx, w.Flush)
}

// Close flushes and releases the write strategy's resources. It does nothing when
// no row has been handed over yet.
//
// Like Flush, its error reports rows that never reached BigQuery. A plain
// "defer s.Close(ctx)" throws that away and loses the rows silently; capture it
// through a named return value instead.
//
//	func write(ctx context.Context, s *bqsink.Sinker[Row]) (err error) {
//		defer func() {
//			if cerr := s.Close(ctx); cerr != nil && err == nil {
//				err = cerr
//			}
//		}()
//		...
//	}
func (s *Sinker[T]) Close(ctx context.Context) error {
	if w := s.openedWriter(); w != nil {
		return w.Close(ctx)
	}
	return nil
}

func (s *Sinker[T]) openedWriter() RowWriter {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return s.rowWriter
}
