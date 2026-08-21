package bqsink

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

func testClient(t *testing.T) *bigquery.Client {
	t.Helper()
	client, err := bigquery.NewClient(context.Background(), "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("bigquery.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client.Close() error = %v", err)
		}
	})
	return client
}

func testRelation() Relation {
	return Relation{DatasetID: "test_dataset", TableID: "test_table"}
}

type definedRow struct {
	Name string
}

func (definedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Labels: map[string]string{"receiver": "value"}}
}

type ptrDefinedRow struct {
	Name string
}

func (*ptrDefinedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Labels: map[string]string{"receiver": "pointer"}}
}

type schemaDefinedRow struct {
	Amount string `bqsink:"explicit"`
}

func (schemaDefinedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "explicit", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
		},
	}
}

// testSinker builds a Sinker over a fake writer, for a test that only looks at
// what the declaration settled.
func testSinker[T any](t *testing.T, opts ...Option) (*Sinker, error) {
	t.Helper()
	return NewSinker(newFakeWriter(t), DeclarationOf[T](), opts...)
}

// metadataOf builds a Sinker for T and returns what its declaration settled on
// BigQueryTableMetadata.
func metadataOf[T any](t *testing.T) (*bigquery.TableMetadata, error) {
	t.Helper()
	s, err := testSinker[T](t)
	if err != nil {
		return nil, err
	}
	return s.metadata, nil
}

func TestNewSinkerRejectsNilWriter(t *testing.T) {
	t.Parallel()

	if _, err := NewSinker(nil, DeclarationOf[simpleRow]()); err == nil {
		t.Fatal("NewSinker() with a nil writer should fail")
	}
}

func TestNewSinkerRejectsEmptyDeclaration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		decl Declaration
	}{
		{name: "a zero value Declaration", decl: Declaration{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewSinker(newFakeWriter(t), tt.decl); err == nil {
				t.Fatalf("NewSinker() with %s should fail", tt.name)
			}
		})
	}
}

// TestNewSinkerRejectsAWriterWithAnInvalidRelation checks the relation
// validation New used to run on its own relation argument: the relation now
// comes from the writer, so NewSinker validates what it reports instead.
func TestNewSinkerRejectsAWriterWithAnInvalidRelation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		relation Relation
	}{
		{name: "no dataset", relation: Relation{TableID: "t"}},
		{name: "no table", relation: Relation{DatasetID: "d"}},
		{name: "neither", relation: Relation{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newFakeWriter(t)
			w.relation = tt.relation
			if _, err := NewSinker(w, DeclarationOf[simpleRow]()); err == nil {
				t.Fatalf("NewSinker() with relation %+v should fail", tt.relation)
			}
		})
	}
}

// TestNewSinkerRequiresMigrationNoneWithoutAClient checks the safeguard around
// a writer that reports no BigQuery client: the table can never be read
// through it, so every strategy but MigrationNone is refused rather than
// silently skipping reconciliation.
func TestNewSinkerRequiresMigrationNoneWithoutAClient(t *testing.T) {
	t.Parallel()

	t.Run("the default strategy fails without a client", func(t *testing.T) {
		t.Parallel()
		w := newFakeWriter(t)
		w.client = nil
		if _, err := NewSinker(w, DeclarationOf[simpleRow]()); err == nil {
			t.Fatal("NewSinker() with a client-less writer and the default strategy should fail")
		}
	})

	t.Run("MigrationNone allows a client-less writer", func(t *testing.T) {
		t.Parallel()
		w := newFakeWriter(t)
		w.client = nil
		s, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(MigrationNone{}, nil))
		if err != nil {
			t.Fatalf("NewSinker() error = %v, want MigrationNone to be allowed without a client", err)
		}
		if s.api != nil {
			t.Errorf("api = %v, want nil since the writer has no client to reconcile through", s.api)
		}
	})
}

// TestSinkRejectsATypeThatDoesNotMatchTheDeclaration checks that the type the
// declaration named at NewSinker time holds for every Sink call: the
// declaration is settled at construction now, so there is no first Sink to
// settle on a type instead.
func TestSinkRejectsATypeThatDoesNotMatchTheDeclaration(t *testing.T) {
	t.Parallel()

	s, err := testSinker[nestedRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), simpleRow{}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection: this Sinker's declaration is nestedRow")
	}
}

func TestNewSinkerInfersSchemaFromTags(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "Name", Type: bigquery.StringFieldType},
		{Name: "Count", Type: bigquery.IntegerFieldType},
	}
	if !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want %s", formatSchema(s.schema), formatSchema(want))
	}
	if s.metadata != nil {
		t.Errorf("metadata = %+v, want nil for a type that does not implement TableDefiner", s.metadata)
	}
}

func TestNewSinkerReadsTableDefiner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*testing.T) (*bigquery.TableMetadata, error)
		want string
	}{
		{name: "a value receiver implementation", fn: metadataOf[definedRow], want: "value"},
		{name: "a pointer receiver implementation", fn: metadataOf[ptrDefinedRow], want: "pointer"},
		{name: "a pointer type parameter", fn: metadataOf[*ptrDefinedRow], want: "pointer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			md, err := tt.fn(t)
			if err != nil {
				t.Fatalf("testSinker() error = %v", err)
			}
			if md == nil {
				t.Fatal("metadata = nil, want the value from BigQueryTableMetadata")
			}
			if got := md.Labels["receiver"]; got != tt.want {
				t.Errorf("Labels[receiver] = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewSinkerPrefersSchemaFromTableDefiner(t *testing.T) {
	t.Parallel()

	s, err := testSinker[schemaDefinedRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "explicit", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
	}
	if !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want %s", formatSchema(s.schema), formatSchema(want))
	}
}

func TestNewSinkerUsesDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if s.migrationRetry == nil {
		t.Fatal("migrationRetry = nil, want DefaultRetryPolicy")
	}
	if s.migrationRetry() == nil {
		t.Error("migrationRetry() = nil, want a gax.Retryer")
	}
}

func TestWithMigrationStrategyReplacesTheDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	built := 0
	replacement := func() gax.Retryer {
		built++
		return gax.OnErrorFunc(gax.Backoff{}, func(error) bool { return false })
	}
	s, err := testSinker[simpleRow](t,
		WithMigrationStrategy(AppendNewColumns{CreateIfMissing: true}, replacement))
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if built != 0 {
		t.Errorf("the policy was built %d times during NewSinker, want 0", built)
	}
	if _, ok := s.migrationRetry().Retry(errors.New("boom")); ok {
		t.Error("Retry() = true, want the replacement policy's false")
	}
	if built != 1 {
		t.Errorf("the policy was built %d times, want 1", built)
	}
}

func TestNewSinkerRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{name: "a nil migration strategy", opt: WithMigrationStrategy(nil, nil)},
		{
			name: "a SyncAllColumns ignoring an empty name",
			opt:  WithMigrationStrategy(SyncAllColumns{IgnoreColumns: []string{""}}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testSinker[simpleRow](t, tt.opt); err == nil {
				t.Fatal("NewSinker() should reject the option")
			}
		})
	}
}

func TestNewSinkerFailsOnUninferableType(t *testing.T) {
	t.Parallel()

	if _, err := testSinker[unsupportedRow](t); err == nil {
		t.Fatal("NewSinker() should fail when the schema cannot be inferred")
	}
}

func TestNewSinkerUsesDefaultStrategies(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	strategy, ok := s.strategy.(AppendNewColumns)
	if !ok {
		t.Fatalf("strategy = %T, want AppendNewColumns", s.strategy)
	}
	if !strategy.CreateIfMissing {
		t.Error("the default strategy does not create a missing table, want it to")
	}
	if s.migrationRetry == nil {
		t.Error("migrationRetry = nil, want DefaultRetryPolicy")
	}
	if s.api == nil {
		t.Error("api = nil, want the destination table")
	}
}

func TestSinkWithNoRowsDoesNothing(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), []simpleRow{}); err != nil {
		t.Errorf("Sink() with no rows error = %v, want nil without contacting BigQuery", err)
	}
}

func TestSinkRejectsNilRows(t *testing.T) {
	t.Parallel()

	t.Run("nil rows", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.Sink(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "rows is nil") {
			t.Errorf("Sink() error = %v, want it to say rows is nil", err)
		}
	})

	t.Run("a nil element inside a []any batch", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.Sink(context.Background(), []any{nil})
		if err == nil || !strings.Contains(err.Error(), "row 0 is nil") {
			t.Errorf("Sink() error = %v, want it to say row 0 is nil", err)
		}
	})
}

func TestSinkRejectsMixedTypesInABatch(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), []any{simpleRow{}, nestedRow{}}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection of the mixed types")
	}
}

// TestSinkRejectsALaterBatchOfAnotherType checks that the type the
// declaration named holds for the rest of the Sinker's life, even once a
// batch of the right type has already been written.
func TestSinkRejectsALaterBatchOfAnotherType(t *testing.T) {
	t.Parallel()

	s := newTestSinker[nestedRow](t, migratedTable(), nil)

	if _, err := s.Sink(context.Background(), nestedRow{A: "a", B: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), simpleRow{}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection: this Sinker declares nestedRow")
	}
}

func TestSinkWithAnEmptyOrNilSliceDoesNotMigrateOrWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows any
	}{
		{name: "an empty slice", rows: []simpleRow{}},
		{name: "a nil slice", rows: []simpleRow(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeTable{metadataErr: notFoundErr()}
			writer := newFakeWriter(t)
			s := newTestSinker[simpleRow](t, fake, writer)

			n, err := s.Sink(context.Background(), tt.rows)
			if err != nil {
				t.Errorf("Sink() error = %v, want nil", err)
			}
			if n != 0 {
				t.Errorf("Sink() n = %d, want 0", n)
			}
			if _, _, metadataCalls := fake.snapshot(); metadataCalls != 0 {
				t.Errorf("Metadata was called %d times, want 0; an empty batch must not migrate", metadataCalls)
			}
			writer.mu.Lock()
			calls := writer.calls
			writer.mu.Unlock()
			if calls != 0 {
				t.Errorf("WriteRows was called %d times, want 0", calls)
			}
		})
	}
}

// TestNewSinkerAcceptsAPointerMigrationNoneWithoutAClient checks that the strategy
// is recognised by what it is rather than by how it was written, since Plan has a
// value receiver and *MigrationNone satisfies MigrationStrategy just as well.
func TestNewSinkerAcceptsAPointerMigrationNoneWithoutAClient(t *testing.T) {
	t.Parallel()

	w := &fakeWriter{relation: testRelation()}
	if _, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(&MigrationNone{}, nil)); err != nil {
		t.Errorf("NewSinker() error = %v, want *MigrationNone to count as MigrationNone", err)
	}
}

// TestNewSinkerRejectsCreateIfMissingWithoutAClient checks that a table which
// cannot be read is not quietly left uncreated: without a client there is nothing
// to create it with, so asking for it is refused rather than ignored.
func TestNewSinkerRejectsCreateIfMissingWithoutAClient(t *testing.T) {
	t.Parallel()

	w := &fakeWriter{relation: testRelation()}
	_, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(MigrationNone{CreateIfMissing: true}, nil))
	if err == nil {
		t.Fatal("NewSinker() error = nil, want CreateIfMissing without a client to be refused")
	}
	if !strings.Contains(err.Error(), "CreateIfMissing") {
		t.Errorf("NewSinker() error = %v, want it to name the setting it cannot honour", err)
	}
}

// trackingResult wraps a WriteResult and records whether Wait has been called
// on it, so a test can tell SinkAsync's own return apart from a caller later
// waiting on it.
type trackingResult struct {
	WriteResult

	mu     sync.Mutex
	waited bool
}

func (r *trackingResult) Wait(ctx context.Context) (int, error) {
	r.mu.Lock()
	r.waited = true
	r.mu.Unlock()
	return r.WriteResult.Wait(ctx)
}

func (r *trackingResult) waitCalled() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.waited
}

// trackingWriter wraps a fakeWriter's WriteRows so each WriteResult it hands
// back is a trackingResult, letting a test see whether Wait reached it.
//
// Its own lock is named apart from the embedded fakeWriter's mu, which still
// guards fakeWriter's own fields (binds, calls, rows): reusing the name mu
// here would shadow that lock rather than share it.
type trackingWriter struct {
	*fakeWriter

	resultsMu sync.Mutex
	results   []*trackingResult
}

func newTrackingWriter(t *testing.T) *trackingWriter {
	t.Helper()
	return &trackingWriter{fakeWriter: newFakeWriter(t)}
}

func (w *trackingWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	result, err := w.fakeWriter.WriteRows(ctx, rows)
	if err != nil {
		return nil, err
	}
	tr := &trackingResult{WriteResult: result}
	w.resultsMu.Lock()
	w.results = append(w.results, tr)
	w.resultsMu.Unlock()
	return tr, nil
}

// lastResult reaches the trackingResult from the most recent WriteRows call.
func (w *trackingWriter) lastResult(t *testing.T) *trackingResult {
	t.Helper()
	w.resultsMu.Lock()
	defer w.resultsMu.Unlock()
	if len(w.results) == 0 {
		t.Fatal("WriteRows has not been called yet")
	}
	return w.results[len(w.results)-1]
}

// TestSinkAsyncReturnsAnUnwaitedResult checks that SinkAsync hands the
// writer's WriteResult back without calling Wait on it itself: the caller
// decides when to wait, and gets the same n and error Wait would have given
// Sink, on both a successful write and a failed one.
func TestSinkAsyncReturnsAnUnwaitedResult(t *testing.T) {
	t.Parallel()

	t.Run("a successful write", func(t *testing.T) {
		t.Parallel()
		writer := newTrackingWriter(t)
		s := newTestSinker[nestedRow](t, migratedTable(), writer)

		result, err := s.SinkAsync(t.Context(), nestedRow{A: "a", B: 1})
		if err != nil {
			t.Fatalf("SinkAsync() error = %v", err)
		}
		if writer.lastResult(t).waitCalled() {
			t.Fatal("Wait() was already called before SinkAsync returned, want the caller to decide when")
		}

		n, err := result.Wait(t.Context())
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		if n != 1 {
			t.Errorf("Wait() n = %d, want 1", n)
		}
		if !writer.lastResult(t).waitCalled() {
			t.Error("Wait() on the returned result did not reach the writer's own WriteResult")
		}
	})

	t.Run("a failed write", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("write refused")
		writer := newTrackingWriter(t)
		writer.writeErr = sentinel
		s := newTestSinker[nestedRow](t, migratedTable(), writer)

		result, err := s.SinkAsync(t.Context(), nestedRow{})
		if err != nil {
			t.Fatalf("SinkAsync() error = %v, want nil: the rows were handed to the writer", err)
		}
		if writer.lastResult(t).waitCalled() {
			t.Fatal("Wait() was already called before SinkAsync returned, want the caller to decide when")
		}

		n, err := result.Wait(t.Context())
		if !errors.Is(err, sentinel) {
			t.Errorf("Wait() error = %v, want the write failure", err)
		}
		if n != 0 {
			t.Errorf("Wait() n = %d, want 0", n)
		}
	})

	t.Run("a positive control: Sink does call Wait", func(t *testing.T) {
		t.Parallel()
		writer := newTrackingWriter(t)
		s := newTestSinker[nestedRow](t, migratedTable(), writer)

		if _, err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		if !writer.lastResult(t).waitCalled() {
			t.Fatal("Wait() was not observed on Sink's own write, want the tracking writer to detect it")
		}
	})
}

// TestSinkAsyncStartsOnlyOnce checks that the first-call reconciliation and
// BindSchema, shared with Sink through start, still run exactly once no
// matter how many times SinkAsync is called, and whether Sink and SinkAsync
// are mixed.
func TestSinkAsyncStartsOnlyOnce(t *testing.T) {
	t.Parallel()

	t.Run("several SinkAsync calls", func(t *testing.T) {
		t.Parallel()
		fake := migratedTable()
		writer := newFakeWriter(t)
		s := newTestSinker[nestedRow](t, fake, writer)

		for i := range 3 {
			result, err := s.SinkAsync(t.Context(), nestedRow{A: "a", B: int64(i)})
			if err != nil {
				t.Fatalf("SinkAsync() call %d error = %v", i+1, err)
			}
			if _, err := result.Wait(t.Context()); err != nil {
				t.Fatalf("Wait() call %d error = %v", i+1, err)
			}
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; start must run once", metadataCalls)
		}
		writer.mu.Lock()
		binds := writer.binds
		writer.mu.Unlock()
		if binds != 1 {
			t.Errorf("BindSchema was called %d times, want 1", binds)
		}
	})

	t.Run("Sink and SinkAsync mixed", func(t *testing.T) {
		t.Parallel()
		fake := migratedTable()
		writer := newFakeWriter(t)
		s := newTestSinker[nestedRow](t, fake, writer)

		if _, err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		result, err := s.SinkAsync(t.Context(), nestedRow{A: "b", B: 2})
		if err != nil {
			t.Fatalf("SinkAsync() error = %v", err)
		}
		if _, err := result.Wait(t.Context()); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; start must run once even mixing Sink and SinkAsync", metadataCalls)
		}
		writer.mu.Lock()
		binds := writer.binds
		writer.mu.Unlock()
		if binds != 1 {
			t.Errorf("BindSchema was called %d times, want 1", binds)
		}
	})
}

// TestStart checks that the exported Start does the same first-call
// reconciliation Sink and SinkAsync trigger implicitly, that it shares the
// same startOnce so a later Sink does not repeat it, and that it caches a
// failure the same way.
func TestStart(t *testing.T) {
	t.Parallel()

	t.Run("Start reconciles once and Sink does not repeat it", func(t *testing.T) {
		t.Parallel()
		fake := migratedTable()
		writer := newFakeWriter(t)
		s := newTestSinker[nestedRow](t, fake, writer)

		if err := s.Start(t.Context()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		if _, err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; Sink must not repeat what Start already did", metadataCalls)
		}
		writer.mu.Lock()
		binds := writer.binds
		writer.mu.Unlock()
		if binds != 1 {
			t.Errorf("BindSchema was called %d times, want 1", binds)
		}
	})

	t.Run("a failed Start is cached and returned by Sink without retrying", func(t *testing.T) {
		t.Parallel()
		fake := &fakeTable{metadataErr: errors.New("boom")}
		writer := newFakeWriter(t)
		s := newTestSinker[nestedRow](t, fake, writer)

		if err := s.Start(t.Context()); err == nil {
			t.Fatal("Start() error = nil, want the Metadata failure")
		}
		if _, err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err == nil {
			t.Fatal("Sink() error = nil, want Start's cached failure")
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; the failure must be cached, not retried by Sink", metadataCalls)
		}
		writer.mu.Lock()
		binds := writer.binds
		writer.mu.Unlock()
		if binds != 0 {
			t.Errorf("BindSchema was called %d times, want 0; migrate failed so BindSchema must not run", binds)
		}
	})

	t.Run("calling Start again returns the cached outcome without contacting BigQuery", func(t *testing.T) {
		t.Parallel()
		fake := migratedTable()
		writer := newFakeWriter(t)
		s := newTestSinker[nestedRow](t, fake, writer)

		if err := s.Start(t.Context()); err != nil {
			t.Fatalf("first Start() error = %v", err)
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatalf("second Start() error = %v", err)
		}
		if _, _, metadataCalls := fake.snapshot(); metadataCalls != 1 {
			t.Errorf("Metadata was called %d times, want 1; a second Start must not repeat it", metadataCalls)
		}
		writer.mu.Lock()
		binds := writer.binds
		writer.mu.Unlock()
		if binds != 1 {
			t.Errorf("BindSchema was called %d times, want 1; a second Start must not repeat it", binds)
		}
	})
}

// TestMigrateCtxIsTheCallersUnlikeBindSchema checks that migrate, unlike
// BindSchema, keeps running on the caller's own ctx: migrate retries within
// that ctx's deadline (see start's doc comment), so it must observe the
// caller's cancellation rather than a context.WithoutCancel copy.
func TestMigrateCtxIsTheCallersUnlikeBindSchema(t *testing.T) {
	t.Parallel()
	fake := migratedTable()
	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, fake, writer)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancel()

	_ = s.Start(ctx)

	migrateCtx := fake.lastMetadataCtx()
	if migrateCtx == nil {
		t.Fatal("Metadata was never called")
	}
	if err := migrateCtx.Err(); err == nil {
		t.Error("migrate's ctx.Err() = nil after the caller cancelled, want context.Canceled")
	}
}

// TestBindSchemaCtxOutlivesTheCallersCancellation checks that BindSchema gets
// a ctx that context.WithoutCancel has detached from the caller's: managedwriter
// retains the ctx BindSchema is given for the lifetime of its background
// connection, so a request-scoped caller cancelling its own ctx, or one whose
// deadline later elapses, after the call must not be able to tear that
// connection down for every later caller.
func TestBindSchemaCtxOutlivesTheCallersCancellation(t *testing.T) {
	t.Parallel()
	fake := migratedTable()
	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, fake, writer)

	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	cancel()

	writer.mu.Lock()
	boundCtx := writer.boundCtx
	writer.mu.Unlock()
	if err := boundCtx.Err(); err != nil {
		t.Errorf("BindSchema's ctx.Err() = %v after the caller cancelled, want nil", err)
	}
	if _, ok := boundCtx.Deadline(); ok {
		t.Error("BindSchema's ctx has a Deadline after context.WithoutCancel, want none")
	}
}

// TestSinkAsyncRejectsATypeThatDoesNotMatchTheDeclaration checks that
// SinkAsync applies the same type check Sink does, and that it does so
// without handing back a WriteResult.
func TestSinkAsyncRejectsATypeThatDoesNotMatchTheDeclaration(t *testing.T) {
	t.Parallel()

	s, err := testSinker[nestedRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	result, err := s.SinkAsync(context.Background(), simpleRow{})
	if err == nil {
		t.Fatal("SinkAsync() error = nil, want a rejection: this Sinker's declaration is nestedRow")
	}
	if result != nil {
		t.Errorf("SinkAsync() result = %v, want nil on a non-nil error", result)
	}
}

// TestSinkAsyncRejectsNilRows checks that SinkAsync applies the same nil
// checks Sink does.
func TestSinkAsyncRejectsNilRows(t *testing.T) {
	t.Parallel()

	t.Run("nil rows", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.SinkAsync(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "rows is nil") {
			t.Errorf("SinkAsync() error = %v, want it to say rows is nil", err)
		}
	})

	t.Run("a nil element inside a []any batch", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.SinkAsync(context.Background(), []any{nil})
		if err == nil || !strings.Contains(err.Error(), "row 0 is nil") {
			t.Errorf("SinkAsync() error = %v, want it to say row 0 is nil", err)
		}
	})
}

// TestSinkAsyncWithAnEmptyOrNilSliceDoesNotMigrateOrWrite checks that
// SinkAsync short-circuits on an empty batch exactly as Sink does: start
// never runs and the writer never sees WriteRows.
// TestPrepareFillsFlagReflectsWhetherTheRowTypeImplementsRowFiller checks the
// branch prepare takes on s.fills: a RowFiller row settles it true and a row
// without one settles it false. What FillRow itself does once fills is true
// is already covered in fill_test.go, so this only checks the flag.
func TestPrepareFillsFlagReflectsWhetherTheRowTypeImplementsRowFiller(t *testing.T) {
	t.Parallel()

	t.Run("a struct implementing RowFiller", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[customFillRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		if !s.fills {
			t.Error("fills = false, want true: customFillRow implements RowFiller")
		}
	})

	t.Run("a struct not implementing RowFiller", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		if s.fills {
			t.Error("fills = true, want false: simpleRow does not implement RowFiller")
		}
	})
}

// TestPrepareWithoutAFillerConvertsTheRowCorrectly checks that the fills ==
// false branch of prepare, which skips both fillable's copy and the RowFiller
// type assertion, still hands marshalRow a row that converts to the same
// values a filled row would.
func TestPrepareWithoutAFillerConvertsTheRowCorrectly(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, migratedTable(), writer)
	if s.fills {
		t.Fatal("fills = true, want false: nestedRow does not implement RowFiller")
	}

	if _, err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	rows := appendedRows(writer)
	if len(rows) != 1 {
		t.Fatalf("%d row(s) were appended, want 1", len(rows))
	}
	want := map[string]bigquery.Value{"A": "a", "B": int64(1)}
	if !reflect.DeepEqual(rows[0], want) {
		t.Errorf("row = %#v, want %#v", rows[0], want)
	}
}

// TestPrepareRejectsANilPointerRowWhenFillsIsFalse checks that skipping
// fillable's copy for a row type that does not fill its own columns still
// refuses a nil pointer, rather than handing it on to marshalRow.
func TestPrepareRejectsANilPointerRowWhenFillsIsFalse(t *testing.T) {
	t.Parallel()

	s, err := testSinker[*simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if s.fills {
		t.Fatal("fills = true, want false: *simpleRow does not implement RowFiller")
	}

	var nilRow *simpleRow
	if _, err := s.prepare(context.Background(), reflect.ValueOf(nilRow)); err == nil {
		t.Fatal("prepare() error = nil, want a nil row to be refused")
	}
}

// TestSinkAsyncLeavesShortWriteDetectionToTheCaller checks the manual half of
// the guard Sink runs on its own: SinkAsync hands back the writer's
// WriteResult as is, without Sink's own n-versus-sent comparison, so a writer
// that under-reports with no error of its own resolves cleanly through Wait.
// Comparing Wait's n against what was handed to SinkAsync is what a caller
// runs in Sink's place to catch the same short write
// TestSinkRejectsAWriterThatUnderReports has Sink catch by itself.
func TestSinkAsyncLeavesShortWriteDetectionToTheCaller(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	writer.underReport = 1
	s := newTestSinker[nestedRow](t, migratedTable(), writer,
		WithMigrationStrategy(AppendNewColumns{}, nil))

	rows := []nestedRow{{}, {}}
	result, err := s.SinkAsync(t.Context(), rows)
	if err != nil {
		t.Fatalf("SinkAsync() error = %v", err)
	}

	n, err := result.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v, want nil: SinkAsync's WriteResult carries none of Sink's own short-write guard", err)
	}
	if want := len(rows) - 1; n != want {
		t.Fatalf("Wait() n = %d, want %d from the writer's underReport", n, want)
	}

	if n == len(rows) {
		t.Fatal("n equals the rows sent, want it short so the caller's comparison catches something")
	}
}

func TestSinkAsyncWithAnEmptyOrNilSliceDoesNotMigrateOrWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows any
	}{
		{name: "an empty slice", rows: []simpleRow{}},
		{name: "a nil slice", rows: []simpleRow(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeTable{metadataErr: notFoundErr()}
			writer := newFakeWriter(t)
			s := newTestSinker[simpleRow](t, fake, writer)

			result, err := s.SinkAsync(context.Background(), tt.rows)
			if err != nil {
				t.Fatalf("SinkAsync() error = %v, want nil", err)
			}
			n, err := result.Wait(context.Background())
			if err != nil {
				t.Errorf("Wait() error = %v, want nil", err)
			}
			if n != 0 {
				t.Errorf("Wait() n = %d, want 0", n)
			}
			if _, _, metadataCalls := fake.snapshot(); metadataCalls != 0 {
				t.Errorf("Metadata was called %d times, want 0; an empty batch must not migrate", metadataCalls)
			}
			writer.mu.Lock()
			calls := writer.calls
			writer.mu.Unlock()
			if calls != 0 {
				t.Errorf("WriteRows was called %d times, want 0", calls)
			}
		})
	}
}
