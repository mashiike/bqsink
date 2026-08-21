package bqsink

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type updateCall struct {
	tm   bigquery.TableMetadataToUpdate
	etag string
}

// fakeTable stands in for *bigquery.Table so that the migration path can be
// exercised without BigQuery.
type fakeTable struct {
	mu sync.Mutex

	metadata    *bigquery.TableMetadata
	metadataErr error

	createErr error
	created   []*bigquery.TableMetadata

	// updateErrs is consulted by attempt index, so a value of
	// []error{err, nil} fails the first Update and lets the second through.
	updateErrs []error
	updates    []updateCall

	metadataCalls int
	metadataCtx   context.Context
}

func (f *fakeTable) Metadata(ctx context.Context, opts ...bigquery.TableMetadataOption) (*bigquery.TableMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadataCalls++
	f.metadataCtx = ctx
	if f.metadataErr != nil {
		return nil, f.metadataErr
	}
	return f.metadata, nil
}

func (f *fakeTable) lastMetadataCtx() context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.metadataCtx
}

func (f *fakeTable) Create(ctx context.Context, tm *bigquery.TableMetadata) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, tm)
	return nil
}

func (f *fakeTable) Update(ctx context.Context, tm bigquery.TableMetadataToUpdate, etag string, opts ...bigquery.TableUpdateOption) (*bigquery.TableMetadata, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := len(f.updates)
	f.updates = append(f.updates, updateCall{tm: tm, etag: etag})
	if i < len(f.updateErrs) && f.updateErrs[i] != nil {
		return nil, f.updateErrs[i]
	}
	return f.metadata, nil
}

func (f *fakeTable) snapshot() (updates []updateCall, created []*bigquery.TableMetadata, metadataCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updates, f.created, f.metadataCalls
}

// fakeQueryRunner records the DDL statements a Sinker would run.
type fakeQueryRunner struct {
	mu   sync.Mutex
	sqls []string
	errs []error
}

func (r *fakeQueryRunner) run(ctx context.Context, sql string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := len(r.sqls)
	r.sqls = append(r.sqls, sql)
	if i < len(r.errs) && r.errs[i] != nil {
		return r.errs[i]
	}
	return nil
}

func (r *fakeQueryRunner) statements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sqls
}

func notFoundErr() error { return &googleapi.Error{Code: http.StatusNotFound, Message: "not found"} }
func etagErr() error {
	return &googleapi.Error{Code: http.StatusPreconditionFailed, Message: "etag mismatch"}
}
func forbiddenErr() error { return &googleapi.Error{Code: http.StatusForbidden, Message: "denied"} }

// fastRetryPolicy decides what to retry exactly as DefaultRetryPolicy does, but
// without waiting between attempts.
func fastRetryPolicy() gax.Retryer {
	return &attemptLimiter{
		retryer: gax.OnErrorFunc(
			gax.Backoff{Initial: time.Nanosecond, Max: time.Nanosecond, Multiplier: 1},
			isRetryable,
		),
		max: migrateMaxRetries,
	}
}

func newTestSinker[T any](t *testing.T, fake *fakeTable, w RowsWriter, opts ...Option) *Sinker {
	t.Helper()
	if w == nil {
		w = newFakeWriter(t)
	}
	s, err := NewSinker(w, DeclarationOf[T](), opts...)
	if err != nil {
		t.Fatalf("NewSinker() error = %v", err)
	}
	s.api = fake
	s.query = &fakeQueryRunner{}
	return s
}

// queriesOf reaches the fake DDL runner newTestSinker installed.
func queriesOf(t *testing.T, s *Sinker) *fakeQueryRunner {
	t.Helper()
	runner, ok := s.query.(*fakeQueryRunner)
	if !ok {
		t.Fatalf("query = %T, want the fake installed by newTestSinker", s.query)
	}
	return runner
}

func TestMigrateCreatesMissingTable(t *testing.T) {
	t.Parallel()

	t.Run("the default strategy creates it", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: notFoundErr()}
		s := newTestSinker[simpleRow](t, fake, nil)

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		_, created, _ := fake.snapshot()
		if len(created) != 1 {
			t.Fatalf("Create was called %d times, want 1", len(created))
		}
		if !reflect.DeepEqual(created[0].Schema, s.schema) {
			t.Errorf("Create schema = %s, want %s", formatSchema(created[0].Schema), formatSchema(s.schema))
		}
	})

	t.Run("a strategy without CreateIfMissing reports it missing", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: notFoundErr()}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(MigrationNone{}, nil))

		err := s.start(context.Background())
		if !errors.Is(err, ErrTableMissing) {
			t.Fatalf("start() error = %v, want one wrapping ErrTableMissing", err)
		}
		if _, created, _ := fake.snapshot(); len(created) != 0 {
			t.Errorf("Create was called %d times, want 0", len(created))
		}
	})

	t.Run("CreateIfMissing creates it with the declared schema and metadata", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: notFoundErr()}
		s := newTestSinker[definedRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{CreateIfMissing: true}, nil))

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		_, created, _ := fake.snapshot()
		if len(created) != 1 {
			t.Fatalf("Create was called %d times, want 1", len(created))
		}
		if !reflect.DeepEqual(created[0].Schema, s.schema) {
			t.Errorf("Create schema = %s, want %s", formatSchema(created[0].Schema), formatSchema(s.schema))
		}
		if got := created[0].Labels["receiver"]; got != "value" {
			t.Errorf("Create Labels[receiver] = %q, want the value from BigQueryTableMetadata", got)
		}
	})

	t.Run("a create failure surfaces", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: notFoundErr(), createErr: forbiddenErr()}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(MigrationNone{CreateIfMissing: true}, nil))

		if err := s.start(context.Background()); err == nil {
			t.Fatal("start() error = nil, want the create failure")
		}
	})
}

func TestMigratePatchesSchema(t *testing.T) {
	t.Parallel()

	t.Run("a new column is appended with the etag", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag:   "etag-1",
			Schema: bigquery.Schema{{Name: "Name", Type: bigquery.StringFieldType}},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, nil))

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		updates, _, _ := fake.snapshot()
		if len(updates) != 1 {
			t.Fatalf("Update was called %d times, want 1", len(updates))
		}
		if updates[0].etag != "etag-1" {
			t.Errorf("Update etag = %q, want %q", updates[0].etag, "etag-1")
		}
		want := bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
		}
		if !reflect.DeepEqual(updates[0].tm.Schema, want) {
			t.Errorf("Update schema = %s, want %s", formatSchema(updates[0].tm.Schema), formatSchema(want))
		}
	})

	t.Run("a matching table is left alone", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, nil))

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		if updates, _, _ := fake.snapshot(); len(updates) != 0 {
			t.Errorf("Update was called %d times, want 0", len(updates))
		}
	})

	t.Run("a conflict stops the patch", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.IntegerFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, nil))

		err := s.start(context.Background())
		if !errors.Is(err, ErrSchemaConflict) {
			t.Fatalf("start() error = %v, want one wrapping ErrSchemaConflict", err)
		}
		if updates, _, _ := fake.snapshot(); len(updates) != 0 {
			t.Errorf("Update was called %d times, want 0", len(updates))
		}
	})

	t.Run("a REQUIRED column cannot be added to an existing table", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag:   "etag-1",
			Schema: bigquery.Schema{{Name: "user_id", Type: bigquery.StringFieldType, Required: true}},
		}}
		s := newTestSinker[taggedRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, nil))

		err := s.start(context.Background())
		if !errors.Is(err, ErrSchemaConflict) {
			t.Fatalf("start() error = %v, want one wrapping ErrSchemaConflict", err)
		}
		if updates, _, _ := fake.snapshot(); len(updates) != 0 {
			t.Errorf("Update was called %d times, want 0", len(updates))
		}
	})

	t.Run("a stray column is dropped with DDL", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(SyncAllColumns{}, nil))

		if err := s.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		statements := queriesOf(t, s).statements()
		if len(statements) != 1 {
			t.Fatalf("%d statement(s) were run, want 1", len(statements))
		}
		want := "ALTER TABLE `test-project.test_dataset.test_table` DROP COLUMN `legacy`"
		if statements[0] != want {
			t.Errorf("statement = %q, want %q", statements[0], want)
		}
	})

	// A plan with both additions and drops has to apply both. Applying only the
	// drops and reporting success would leave the table silently diverged.
	t.Run("adds and drops are both applied, adds first", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(SyncAllColumns{}, nil))

		if err := s.start(t.Context()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		updates, _, _ := fake.snapshot()
		if len(updates) != 1 {
			t.Fatalf("Update was called %d times, want 1 to add the missing column", len(updates))
		}
		added := false
		for _, f := range updates[0].tm.Schema {
			if f.Name == "Count" {
				added = true
			}
		}
		if !added {
			t.Errorf("the patch did not add Count: %s", formatSchema(updates[0].tm.Schema))
		}
		if statements := queriesOf(t, s).statements(); len(statements) != 1 {
			t.Errorf("%d statement(s) were run, want 1 to drop legacy", len(statements))
		}
	})

	t.Run("a column name outside the allowed characters is refused", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
				{Name: "bad`name", Type: bigquery.StringFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(SyncAllColumns{}, nil))

		if err := s.start(t.Context()); err == nil {
			t.Fatal("start() error = nil, want a refusal to interpolate the name")
		}
		if statements := queriesOf(t, s).statements(); len(statements) != 0 {
			t.Errorf("%d statement(s) were run, want 0", len(statements))
		}
	})

	t.Run("ignoring the stray column lets the patch through", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(SyncAllColumns{IgnoreColumns: []string{"legacy"}}, nil))

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		updates, _, _ := fake.snapshot()
		if len(updates) != 1 {
			t.Fatalf("Update was called %d times, want 1", len(updates))
		}
		want := bigquery.Schema{
			{Name: "Name", Type: bigquery.StringFieldType},
			{Name: "legacy", Type: bigquery.StringFieldType},
			{Name: "Count", Type: bigquery.IntegerFieldType},
		}
		if !reflect.DeepEqual(updates[0].tm.Schema, want) {
			t.Errorf("Update schema = %s, want %s", formatSchema(updates[0].tm.Schema), formatSchema(want))
		}
	})
}

func TestMigrateRetriesConcurrentChange(t *testing.T) {
	t.Parallel()

	driftedTable := func() *fakeTable {
		return &fakeTable{metadata: &bigquery.TableMetadata{
			ETag:   "etag-1",
			Schema: bigquery.Schema{{Name: "Name", Type: bigquery.StringFieldType}},
		}}
	}

	t.Run("an etag mismatch is retried until it succeeds", func(t *testing.T) {
		t.Parallel()
		fake := driftedTable()
		fake.updateErrs = []error{etagErr(), etagErr(), nil}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, fastRetryPolicy))

		if err := s.start(context.Background()); err != nil {
			t.Fatalf("start() error = %v", err)
		}
		updates, _, metadataCalls := fake.snapshot()
		if len(updates) != 3 {
			t.Errorf("Update was called %d times, want 3", len(updates))
		}
		if metadataCalls != 3 {
			t.Errorf("Metadata was called %d times, want 3; every attempt must reread the etag", metadataCalls)
		}
	})

	t.Run("retrying gives up after the limit", func(t *testing.T) {
		t.Parallel()
		fake := driftedTable()
		fake.updateErrs = []error{etagErr(), etagErr(), etagErr(), etagErr(), etagErr(), etagErr()}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, fastRetryPolicy))

		if err := s.start(context.Background()); err == nil {
			t.Fatal("start() error = nil, want the last etag failure")
		}
		if updates, _, _ := fake.snapshot(); len(updates) != migrateMaxRetries+1 {
			t.Errorf("Update was called %d times, want %d", len(updates), migrateMaxRetries+1)
		}
	})

	t.Run("other errors are not retried", func(t *testing.T) {
		t.Parallel()
		fake := driftedTable()
		fake.updateErrs = []error{forbiddenErr(), nil}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, fastRetryPolicy))

		if err := s.start(context.Background()); err == nil {
			t.Fatal("start() error = nil, want the permission failure")
		}
		if updates, _, _ := fake.snapshot(); len(updates) != 1 {
			t.Errorf("Update was called %d times, want 1", len(updates))
		}
	})
}

func TestMigrateRunsOnce(t *testing.T) {
	t.Parallel()

	t.Run("a success is not repeated", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadata: &bigquery.TableMetadata{
			ETag: "etag-1",
			Schema: bigquery.Schema{
				{Name: "Name", Type: bigquery.StringFieldType},
				{Name: "Count", Type: bigquery.IntegerFieldType},
			},
		}}
		s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(AppendNewColumns{}, nil))

		for i := range 3 {
			if err := s.start(context.Background()); err != nil {
				t.Fatalf("start() call %d error = %v", i+1, err)
			}
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1", metadataCalls)
		}
	})

	t.Run("a failure is cached and not retried on a later call", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: forbiddenErr()}
		s := newTestSinker[simpleRow](t, fake, nil)

		first := s.start(context.Background())
		if first == nil {
			t.Fatal("start() error = nil, want the permission failure")
		}
		second := s.start(context.Background())
		if !errors.Is(second, first) && second.Error() != first.Error() {
			t.Errorf("the second start() returned %v, want the cached %v", second, first)
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; the failure must be cached", metadataCalls)
		}
	})
}

func TestSinkRequiresMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fake := &fakeTable{metadataErr: notFoundErr()}
	s := newTestSinker[simpleRow](t, fake, nil, WithMigrationStrategy(MigrationNone{}, nil))

	if _, err := s.Sink(ctx, simpleRow{}); !errors.Is(err, ErrTableMissing) {
		t.Errorf("Sink() error = %v, want the migration failure wrapping ErrTableMissing", err)
	}
}

func unavailableErr() error {
	return status.Error(codes.Unavailable, "the service is unavailable")
}

func rateLimitErr() error {
	return &googleapi.Error{Code: http.StatusTooManyRequests, Message: "rate limit exceeded"}
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "a plain error", err: errors.New("boom")},
		{name: "an etag mismatch", err: etagErr(), want: true},
		{name: "a conflict", err: &googleapi.Error{Code: http.StatusConflict}, want: true},
		{name: "a rate limit", err: rateLimitErr(), want: true},
		{name: "a server error", err: &googleapi.Error{Code: http.StatusInternalServerError}, want: true},
		{name: "service unavailable over HTTP", err: &googleapi.Error{Code: http.StatusServiceUnavailable}, want: true},
		{name: "a gateway timeout", err: &googleapi.Error{Code: http.StatusGatewayTimeout}, want: true},
		{name: "unavailable over gRPC", err: unavailableErr(), want: true},
		{name: "a deadline over gRPC", err: status.Error(codes.DeadlineExceeded, "too slow"), want: true},
		{name: "resources exhausted over gRPC", err: status.Error(codes.ResourceExhausted, "quota"), want: true},
		{name: "forbidden", err: forbiddenErr()},
		{name: "not found", err: notFoundErr()},
		{name: "invalid argument over gRPC", err: status.Error(codes.InvalidArgument, "bad row")},
		{
			name: "a wrapped transient failure",
			err:  fmt.Errorf("append: %w", unavailableErr()),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetryable(tt.err); got != tt.want {
				t.Errorf("isRetryable(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
