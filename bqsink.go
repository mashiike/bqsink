// Package bqsink writes rows to BigQuery and keeps the destination table's
// schema in sync with a schema declared in Go code.
//
// The declaration is the source of truth: the real table follows it, and bqsink
// never infers a schema from the data being written. What the table looks like is
// said by the row type and nowhere else — its struct tags, and its
// BigQueryTableMetadata method for what tags cannot express. No Option describes
// the table, since a row type carries the domain knowledge that gives its columns
// meaning, and two places to say what a table is means two answers to keep
// agreeing.
//
// The Options settle how bqsink behaves around that declaration: what to do about a
// difference between it and the real table, how rows travel, what gets logged, and
// how a transient failure is retried.
package bqsink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
)

var (
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
// The returned metadata's Schema field, if set, overrides the schema derived from
// struct tags. That is where a column struct tags cannot describe belongs, such as
// BIGNUMERIC precision, a column description or a policy tag: bqsink has no Option
// for declaring a schema, since the row type is meant to be the one place that
// says what the table holds.
//
// Read-only fields (ETag, CreationTime, LastModifiedTime, NumBytes, NumRows,
// FullID, Type) are ignored.
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
	client         *bigquery.Client
	relation       Relation
	table          *bigquery.Table
	api            tableAPI
	query          queryRunner
	plan           *rowPlan
	schema         bigquery.Schema
	metadata       *bigquery.TableMetadata
	strategy       MigrationStrategy
	migrationRetry func() gax.Retryer
	writeStrategy  WriteStrategy
	logger         *slog.Logger

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
// What the table should look like is declared by T alone, and no Option changes
// that. The schema comes from its struct tags, following the rules described on
// InferSchema, and table level settings from its BigQueryTableMetadata method when
// T implements TableDefiner. That method's Schema field, when set, takes the place
// of the derived schema, which is how a column struct tags cannot describe gets
// declared.
//
// T must be mappable to a row even when it spells its schema out, since its fields
// are what gets written. Every column the struct would write has to be present in
// the spelled out schema; New fails otherwise, rather than letting BigQuery reject
// the first write.
//
// Without options the migration strategy is AppendNewColumns{CreateIfMissing:
// true}, which creates the table if it is absent and adds columns the declaration
// gained, its retries are DefaultRetryPolicy's, and the write strategy is
// &StorageWrite{}. Pass MigrationNone{} to leave the table alone.
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
	metadata := tableMetadataOf[T]()
	plan, err := buildRowPlan(reflect.TypeFor[T](), c.marshalers)
	if err != nil {
		return nil, err
	}
	schema, err := resolveSchema(metadata, plan)
	if err != nil {
		return nil, err
	}
	metadata, err = resolveTableMetadata(metadata, plan)
	if err != nil {
		return nil, err
	}
	strategy := c.strategy
	migrationRetry := c.migrationRetry
	if strategy == nil {
		// The default has to keep retrying: replicas deploying at the same time add
		// the same column at the same time, and BigQuery reports that as a failed
		// precondition which only a retry gets past.
		strategy = AppendNewColumns{CreateIfMissing: true}
		migrationRetry = DefaultRetryPolicy
	}
	writeStrategy := c.writeStrategy
	if writeStrategy == nil {
		writeStrategy = &StorageWrite{}
	}
	logger := c.logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(slog.String("relation", relation.String()))
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
		client:         client,
		relation:       relation,
		table:          table,
		api:            table,
		query:          clientQueryRunner{client: client},
		plan:           plan,
		schema:         schema,
		metadata:       metadata,
		strategy:       strategy,
		migrationRetry: migrationRetry,
		writeStrategy:  writeStrategy,
		logger:         logger,
		sinkerID:       sinkerID,
		createdAt:      time.Now(),
		filler:         filler,
		now:            time.Now,
	}, nil
}

// resolveSchema settles the declared schema: the one T's BigQueryTableMetadata
// spells out if it has one, and otherwise the one its struct tags describe.
func resolveSchema(metadata *bigquery.TableMetadata, plan *rowPlan) (bigquery.Schema, error) {
	if metadata == nil || metadata.Schema == nil {
		return plan.schema(), nil
	}
	declared := metadata.Schema
	if err := checkPlanAgainstSchema(plan, declared); err != nil {
		return nil, err
	}
	return declared, nil
}

// resolveTableMetadata folds what the tags describe about the table, its physical
// layout and what an embedded TableMeta says, into the table metadata.
//
// Where the tags and the metadata both settle the same thing, that is a
// contradiction rather than a precedence question, so it fails instead of quietly
// preferring one: the declaration is meant to be the single description of the
// table.
func resolveTableMetadata(md *bigquery.TableMetadata, plan *rowPlan) (*bigquery.TableMetadata, error) {
	if plan.partitioning == nil && plan.clustering == nil && plan.description == "" && len(plan.labels) == 0 {
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
	if plan.description != "" {
		if out.Description != "" {
			return nil, fmt.Errorf("bqsink: %s tags a table description, but the table metadata already sets Description; drop one of them",
				plan.goType)
		}
		out.Description = plan.description
	}
	if len(plan.labels) > 0 {
		if len(out.Labels) > 0 {
			return nil, fmt.Errorf("bqsink: %s tags table labels, but the table metadata already sets Labels; drop one of them",
				plan.goType)
		}
		out.Labels = plan.labels
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
// Another process changing the same table concurrently, which BigQuery reports as
// a failed ETag precondition or a conflict, is retried here according to the policy
// WithMigrationStrategy settled on. That is the only thing a Sinker retries: a
// write is retried by the write strategy, which is the one holding the rows.
//
// Sink calls Migrate on its first invocation, so calling it directly is only
// necessary to apply schema changes ahead of time, such as during a deploy.
func (s *Sinker[T]) Migrate(ctx context.Context) error {
	s.migrateOnce.Do(func() {
		s.migrateErr = retrying(ctx, s.logger, "migrate", s.migrationRetry, s.migrate)
	})
	return s.migrateErr
}

// Sink writes vs, running Migrate on the first call, and returns how many of them
// reached BigQuery.
//
// The rows travel as one batch, so how many are given here is what decides how
// much a load job carries or an append sends. Nothing is buffered between calls:
// giving LoadJobs one row at a time submits a load job each time.
//
// Sink returns a non-nil error whenever n < len(vs), so that vs[n:] are exactly
// the rows that did not land. The caller still holds them, which is what makes
// dealing with them the caller's choice; nothing else records what was lost.
// n counts rows written and not rows prepared: a row that cannot be converted
// leaves n at 0 even though the rows before it were converted.
//
// If T implements RowFiller, FillRow is called on a copy of each element first, so
// that the row can fill in columns such as a write timestamp. That happens once per
// row, before the conversion and before any retry.
//
// A transient failure is retried by the write strategy, so a row can reach
// BigQuery more than once. Neither transport bqsink ships deduplicates, so the
// guarantee is at-least-once; IngestionMetadata's _ingestion_row_id is what makes
// those duplicates identifiable.
func (s *Sinker[T]) Sink(ctx context.Context, vs ...T) (int, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	w, err := s.writer(ctx)
	if err != nil {
		return 0, err
	}
	rows := make([]Row, len(vs))
	for i, v := range vs {
		row, err := s.prepare(ctx, v)
		if err != nil {
			return 0, err
		}
		rows[i] = row
	}
	n, err := w.WriteRows(ctx, rows)
	if err == nil && n != len(rows) {
		// A writer that reports fewer rows than it was given owes an error saying so,
		// the way bufio turns the same silence into io.ErrShortWrite. Letting it pass
		// would be a row lost without a word, which is what bqsink exists to prevent.
		return n, fmt.Errorf("bqsink: %T wrote %d of %d row(s) but reported no error", w, n, len(rows))
	}
	return n, err
}

// prepare gives the row its id, lets it fill its own columns in and then converts
// it. All of that happens once, before any retry, so a retried row carries the same
// values.
func (s *Sinker[T]) prepare(ctx context.Context, v T) (Row, error) {
	rowID, err := newID()
	if err != nil {
		return Row{}, fmt.Errorf("bqsink: %w", err)
	}
	if s.filler != nil {
		if filler := s.filler(&v); filler != nil {
			info := AppendInfo{
				Relation:        s.relation,
				SinkerID:        s.sinkerID,
				SinkerCreatedAt: s.createdAt,
				RowID:           rowID,
				Time:            s.now(),
			}
			if err := filler.FillRow(ctx, info); err != nil {
				return Row{}, fmt.Errorf("bqsink: fill a row of %T: %w", v, err)
			}
		}
	}
	values, err := s.toRow(v)
	if err != nil {
		return Row{}, err
	}
	return Row{ID: rowID, Values: values}, nil
}

// retrying runs op, named by what for the log, under the policy newRetryer makes.
// A nil newRetryer means op is run once.
//
// A failure that is retried is logged, because a later attempt succeeding leaves
// the caller with no sign that it happened. It is logged on the way into the next
// attempt rather than when it is returned, so that the failure op finally returns
// is reported once, by the caller, and not logged here as well.
func retrying(ctx context.Context, logger *slog.Logger, what string, newRetryer func() gax.Retryer, op func(context.Context) error) error {
	if newRetryer == nil {
		return op(ctx)
	}
	var attempt int
	var lastErr error
	call := func(ctx context.Context, _ gax.CallSettings) error {
		if lastErr != nil {
			logger.WarnContext(ctx, "retrying after a failure a later attempt may get past",
				slog.String("operation", what),
				slog.Int("attempt", attempt),
				slog.Any("error", lastErr))
		}
		attempt++
		lastErr = op(ctx)
		return lastErr
	}
	return gax.Invoke(ctx, call, gax.WithRetry(newRetryer))
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
		s.rowWriter, s.writerErr = s.writeStrategy.Open(ctx, s.table, s.schema, s.logger)
	}
	return s.rowWriter, s.writerErr
}

// Close releases what the write strategy holds. It does nothing when no row has
// been handed over yet, since the writer is opened on first use.
//
// No row is waiting for it: Sink writes the rows it is given before it returns, so
// nothing is buffered for Close to send. Its error says that a connection did not
// shut down cleanly, which is worth reporting but has cost no rows.
func (s *Sinker[T]) Close(ctx context.Context) error {
	w := s.openedWriter()
	if w == nil {
		return nil
	}
	return w.Close(ctx)
}

func (s *Sinker[T]) openedWriter() RowWriter {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return s.rowWriter
}
