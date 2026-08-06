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

// Declaration says what the destination table should hold.
//
// It is read off a row type: the struct tags describe the columns, and a
// BigQueryTableMetadata method describes what tags cannot. Handing one to NewSinker
// settles the table's shape for good, which is why everything the declaration
// decides is reported there — a tag that cannot be parsed, a column its spelled out
// schema has no room for, a FillRow with a value receiver — rather than by the first
// row written.
//
// A constructor here reports nothing itself, so that it composes into the NewSinker
// call; what it could not make sense of comes back from NewSinker.
type Declaration struct {
	rowType  reflect.Type
	metadata *bigquery.TableMetadata
	err      error
}

// DeclarationOf returns what T declares about its table.
//
// This is the ordinary way to declare one. The row type carries the domain
// knowledge that gives its columns meaning, so it is the one place that can say
// what the table holds.
func DeclarationOf[T any]() Declaration {
	return DeclarationForType(reflect.TypeFor[T]())
}

// DeclarationForType returns what rt declares about its table, for a row type only
// settled at run time, such as one reflect.StructOf built from a schema fetched
// from somewhere else.
//
// A type built that way carries no methods of its own, so its table's settings come
// from its tags alone. It does promote the value receiver methods of what it embeds,
// which is enough for TableDefiner; a pointer receiver's are not promoted, so
// embedding IngestionMetadata in one would leave FillRow uncalled, and NewSinker
// turns that down rather than writing empty columns.
func DeclarationForType(rt reflect.Type) Declaration {
	if rt == nil {
		return Declaration{err: errors.New("bqsink: DeclarationForType: the type is nil")}
	}
	return Declaration{rowType: rt, metadata: tableMetadataOf(rt)}
}

// Sinker writes rows of one declared type to one BigQuery table.
//
// It holds what does not depend on how the rows travel: the declaration, bringing
// the real table in line with it, and turning a row into columns. Where the rows go
// and how they get there belongs to the RowsWriter it was built with, which is also
// the thing to close when there is nothing left to write.
//
// A Sinker is safe for concurrent use.
type Sinker struct {
	writer         RowsWriter
	relation       Relation
	api            tableAPI
	query          queryRunner
	strategy       MigrationStrategy
	migrationRetry func() gax.Retryer
	logger         *slog.Logger

	rowType  reflect.Type
	plan     *rowPlan
	schema   bigquery.Schema
	metadata *bigquery.TableMetadata

	sinkerID  string
	createdAt time.Time
	now       func() time.Time

	startOnce sync.Once
	startErr  error
}

// NewSinker returns a Sinker writing what decl declares through w.
//
// The declaration is read here, so a row type that cannot be mapped to a row, a
// column its spelled out schema has no room for, or a FillRow with a value receiver
// all fail now rather than on the first write. Nothing contacts BigQuery: the real
// table is read once the first batch arrives, since a Sinker with no rows to write
// has nothing to reconcile.
//
// Without options the migration strategy is AppendNewColumns{CreateIfMissing:
// true}, which creates the table if it is absent and adds the columns the
// declaration gained, and its retries are DefaultRetryPolicy's. Pass MigrationNone{}
// to leave the table alone.
//
// The table is reconciled through the client w reports. A writer with no client
// cannot be reconciled at all, so only MigrationNone is allowed with one, and it is
// then said in the log that nothing was checked.
//
// Where w has something to log, being LoggerBindable, it is handed the logger
// WithLogger settled on. Closing w is the caller's own business: it made it, and a
// Sinker buffers nothing that would need flushing first.
func NewSinker(w RowsWriter, decl Declaration, opts ...Option) (*Sinker, error) {
	if w == nil {
		return nil, errors.New("bqsink: NewSinker: writer is nil")
	}
	if decl.err != nil {
		return nil, decl.err
	}
	if decl.rowType == nil {
		return nil, errors.New("bqsink: NewSinker: the declaration says nothing; build it with DeclarationOf or DeclarationForType")
	}
	relation := w.Relation()
	if err := relation.validate(); err != nil {
		return nil, fmt.Errorf("bqsink: NewSinker: the relation %T writes to: %w", w, err)
	}
	var c config
	for _, o := range opts {
		if err := o(&c); err != nil {
			return nil, err
		}
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
	logger := c.logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	logger = logger.With(slog.String("relation", relation.String()))

	plan, schema, metadata, err := resolveDeclaration(decl, c.marshalers)
	if err != nil {
		return nil, err
	}
	sinkerID, err := newID()
	if err != nil {
		return nil, fmt.Errorf("bqsink: %w", err)
	}
	s := &Sinker{
		writer:         w,
		relation:       relation,
		strategy:       strategy,
		migrationRetry: migrationRetry,
		logger:         logger,
		rowType:        decl.rowType,
		plan:           plan,
		schema:         schema,
		metadata:       metadata,
		sinkerID:       sinkerID,
		createdAt:      time.Now(),
		now:            time.Now,
	}
	if client := w.Client(); client != nil {
		table := relation.table(client)
		s.api = table
		s.query = clientQueryRunner{client: client}
	} else if err := checkStrategyWithoutAClient(w, relation, strategy); err != nil {
		return nil, err
	}
	if b, ok := w.(LoggerBindable); ok {
		b.BindLogger(logger)
	}
	if s.api == nil {
		logger.Warn("the writer reports no BigQuery client, so the table is never checked against the declaration")
	}
	return s, nil
}

// checkStrategyWithoutAClient turns down a strategy that cannot do its job through
// a writer not connected to BigQuery.
//
// Reconciling needs the table, so only MigrationNone gets through, and its
// CreateIfMissing does not: a table that cannot be read cannot be created either,
// and letting that pass would leave the caller believing it had been.
func checkStrategyWithoutAClient(w RowsWriter, relation Relation, strategy MigrationStrategy) error {
	none, ok := strategy.(MigrationNone)
	if p, isPointer := strategy.(*MigrationNone); isPointer && p != nil {
		none, ok = *p, true
	}
	if !ok {
		return fmt.Errorf("bqsink: NewSinker: %T is not connected to BigQuery, so %s cannot be reconciled with the declaration; pass MigrationNone{} to write without reconciling it",
			w, relation)
	}
	if none.CreateIfMissing {
		return fmt.Errorf("bqsink: NewSinker: %T is not connected to BigQuery, so %s cannot be created if it is missing; drop CreateIfMissing or give the writer a client",
			w, relation)
	}
	return nil
}

// resolveDeclaration works out what to write, what the table should look like, and
// what settings it should have, from what the row type declares.
func resolveDeclaration(decl Declaration, marshalers *Marshalers) (*rowPlan, bigquery.Schema, *bigquery.TableMetadata, error) {
	plan, err := buildRowPlan(decl.rowType, marshalers)
	if err != nil {
		return nil, nil, nil, err
	}
	schema, err := resolveSchema(decl.metadata, plan)
	if err != nil {
		return nil, nil, nil, err
	}
	metadata, err := resolveTableMetadata(decl.metadata, plan)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := checkRowFiller(decl.rowType); err != nil {
		return nil, nil, nil, err
	}
	return plan, schema, metadata, nil
}

// resolveSchema settles the declared schema: the one the row type's
// BigQueryTableMetadata spells out if it has one, and otherwise the one its struct
// tags describe.
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

// tableMetadataOf asks the row type what its table should look like, on a zero
// value: the answer belongs to the type, so it must not depend on what a particular
// row holds. Nothing here ever sees a row the caller passed.
func tableMetadataOf(rt reflect.Type) *bigquery.TableMetadata {
	if rt.Kind() == reflect.Pointer {
		if d, ok := reflect.New(rt.Elem()).Interface().(TableDefiner); ok {
			return d.BigQueryTableMetadata()
		}
		return nil
	}
	zero := reflect.New(rt)
	if d, ok := zero.Elem().Interface().(TableDefiner); ok {
		return d.BigQueryTableMetadata()
	}
	if d, ok := zero.Interface().(TableDefiner); ok {
		return d.BigQueryTableMetadata()
	}
	return nil
}

// Relation returns the relation naming the destination table, which is the one the
// writer reports.
func (s *Sinker) Relation() Relation {
	return s.relation
}

// start brings the real table in line with the declaration and hands the writer the
// schema, on the way to writing the first batch.
//
// It runs once and caches its outcome, success or failure alike. A later call
// returns the same error without contacting BigQuery, so recovering from a failure
// means building a new Sinker. Since the work happens on the first Sink, ctx belongs
// to that caller; a ctx cancelled later has no effect.
//
// Another process changing the same table at the same time, which BigQuery reports
// as a failed ETag precondition or a conflict, is retried here according to the
// policy WithMigrationStrategy settled on. That is the only thing a Sinker retries:
// a write is retried by the writer, which is the one holding the rows.
func (s *Sinker) start(ctx context.Context) error {
	s.startOnce.Do(func() {
		if s.api != nil {
			if err := retrying(ctx, s.logger, "migrate", s.migrationRetry, s.migrate); err != nil {
				s.startErr = err
				return
			}
		}
		s.startErr = s.writer.BindSchema(ctx, s.schema)
	})
	return s.startErr
}

// Sink writes rows and returns how many of them reached BigQuery.
//
// A slice is a batch of its elements and anything else is a single row, so a
// []AccessLog and one AccessLog go through the same call. An empty or nil slice
// writes nothing and reports no error.
//
// Every row has to be of the type the declaration named, which a slice of a
// concrete type gives for free and a []any can break. A nil row is refused.
//
// The first call is what reconciles the real table with the declaration and hands
// the writer the settled schema. Its outcome is kept: a failure there is returned by
// every later call as well, so recovering from one means building a new Sinker.
//
// The rows travel as one batch, so how many are given here is what decides how much
// a load job carries or an append sends. Nothing is buffered here between calls,
// though the writer may hold rows back when it was asked to.
//
// Sink returns a non-nil error whenever n is fewer than the rows it was given, so
// that rows[n:] are exactly the ones that did not land. The caller still holds them,
// which is what makes dealing with them the caller's choice; nothing else records
// what was lost. n counts rows written and not rows prepared: a row that cannot be
// converted leaves n at 0 even though the rows before it were converted.
//
// If the row type implements RowFiller, FillRow is called on a copy of each element
// first, so that the row can fill in columns such as a write timestamp. That happens
// once per row, before the conversion and before any retry.
//
// A transient failure is retried by the writer, so a row can reach BigQuery more
// than once. Neither transport bqsink ships deduplicates, so the guarantee is
// at-least-once; IngestionMetadata's _ingestion_row_id is what makes those
// duplicates identifiable.
func (s *Sinker) Sink(ctx context.Context, rows any) (int, error) {
	vs, err := rowsOf(rows)
	if err != nil {
		return 0, err
	}
	if len(vs) == 0 {
		return 0, nil
	}
	rt, err := batchRowType(vs)
	if err != nil {
		return 0, err
	}
	if rt != s.rowType {
		return 0, fmt.Errorf("bqsink: this Sinker writes %s, which its declaration settled; %s has to go to a Sinker of its own",
			s.rowType, rt)
	}
	if err := s.start(ctx); err != nil {
		return 0, err
	}
	out := make([]Row, len(vs))
	for i, v := range vs {
		row, err := s.prepare(ctx, v)
		if err != nil {
			return 0, err
		}
		out[i] = row
	}
	result, err := s.writer.WriteRows(ctx, out)
	if err != nil {
		return 0, err
	}
	if result == nil {
		return 0, fmt.Errorf("bqsink: %T took %d row(s) and returned neither a result nor an error", s.writer, len(out))
	}
	n, err := result.Wait(ctx)
	if err == nil && n != len(out) {
		// A writer that reports fewer rows than it was given owes an error saying so,
		// the way bufio turns the same silence into io.ErrShortWrite. Letting it pass
		// would be a row lost without a word, which is what bqsink exists to prevent.
		return n, fmt.Errorf("bqsink: %T wrote %d of %d row(s) but reported no error", s.writer, n, len(out))
	}
	return n, err
}

// rowsOf reads the rows out of what Sink was handed. A slice or an array is a batch
// of its elements, and anything else is a single row.
//
// Nothing is ambiguous about that: buildRowPlan maps a struct and nothing else, so a
// row can never be a slice itself. A type that cannot be a row at all is turned down
// by the declaration, before any of this.
//
// An element of a []any holds its row boxed, so the row is what the element holds
// rather than the element.
func rowsOf(rows any) ([]reflect.Value, error) {
	rv := reflect.ValueOf(rows)
	if !rv.IsValid() {
		return nil, errors.New("bqsink: rows is nil")
	}
	if k := rv.Kind(); k != reflect.Slice && k != reflect.Array {
		return []reflect.Value{rv}, nil
	}
	out := make([]reflect.Value, rv.Len())
	for i := range out {
		v := rv.Index(i)
		if v.Kind() == reflect.Interface {
			v = v.Elem()
		}
		if !v.IsValid() {
			return nil, fmt.Errorf("bqsink: row %d is nil", i)
		}
		out[i] = v
	}
	return out, nil
}

// batchRowType reads the row type out of a batch, refusing one that mixes types. A
// slice of a concrete type cannot, but a []any can, and a Sinker writes one type.
func batchRowType(vs []reflect.Value) (reflect.Type, error) {
	rt := vs[0].Type()
	for i, v := range vs[1:] {
		if v.Type() != rt {
			return nil, fmt.Errorf("bqsink: row %d is %s but row 0 is %s; every row in a batch has to be of one type",
				i+1, v.Type(), rt)
		}
	}
	return rt, nil
}

// prepare gives the row its id, lets it fill its own columns in and then converts
// it. All of that happens once, before any retry, so a retried row carries the same
// values.
func (s *Sinker) prepare(ctx context.Context, v reflect.Value) (Row, error) {
	rowID, err := newID()
	if err != nil {
		return Row{}, fmt.Errorf("bqsink: %w", err)
	}
	rv, err := fillable(v)
	if err != nil {
		return Row{}, err
	}
	if filler, ok := rv.Interface().(RowFiller); ok {
		info := AppendInfo{
			Relation:        s.relation,
			SinkerID:        s.sinkerID,
			SinkerCreatedAt: s.createdAt,
			RowID:           rowID,
			Time:            s.now(),
		}
		if err := filler.FillRow(ctx, info); err != nil {
			return Row{}, fmt.Errorf("bqsink: fill a row of %s: %w", v.Type(), err)
		}
	}
	values, err := s.plan.marshalRow(rv)
	if err != nil {
		return Row{}, err
	}
	return Row{ID: rowID, Values: values}, nil
}

// fillable returns a pointer to the row, which is what RowFiller writes through. A
// row handed over by value is copied first, leaving the caller's own value untouched;
// a row handed over as a pointer is that pointer, so filling reaches what the caller
// holds.
//
// A nil pointer is refused here rather than by the conversion, since FillRow would
// otherwise be called on a nil receiver first.
func fillable(v reflect.Value) (reflect.Value, error) {
	if v.Kind() != reflect.Pointer {
		rv := reflect.New(v.Type())
		rv.Elem().Set(v)
		return rv, nil
	}
	if v.IsNil() {
		return reflect.Value{}, fmt.Errorf("bqsink: cannot write a nil %s", v.Type())
	}
	return v, nil
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
