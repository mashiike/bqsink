package bqsink

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
)

// fakeWriter stands in for a transport, recording what it is given.
type fakeWriter struct {
	relation Relation
	client   *bigquery.Client

	mu sync.Mutex

	binds       int
	boundSchema bigquery.Schema
	boundLogger *slog.Logger
	boundCtx    context.Context
	bindErr     error

	rows       []map[string]bigquery.Value
	batchSizes []int
	calls      int

	// takeErr makes WriteRows itself refuse the rows, the way a writer that
	// cannot even accept a call must report it.
	takeErr error

	// writeErr makes WriteRows take the rows but report that none of them
	// landed, the way a load job or an append that fails outright must.
	writeErr error

	// underReport makes WriteRows claim it wrote this many fewer rows than it was
	// given while returning no error, which the contract forbids.
	underReport int

	closes int
}

// newFakeWriter builds a fakeWriter whose Relation and Client match what
// NewSinker requires: a client that reconciling the table can go through, and a
// relation with its ProjectID filled the way a real writer's NewWriter fills it.
func newFakeWriter(t *testing.T) *fakeWriter {
	t.Helper()
	client := testClient(t)
	relation := testRelation()
	relation.ProjectID = client.Project()
	return &fakeWriter{relation: relation, client: client}
}

// Relation implements RowsWriter.
func (w *fakeWriter) Relation() Relation { return w.relation }

// Client implements RowsWriter.
func (w *fakeWriter) Client() *bigquery.Client { return w.client }

// BindLogger implements LoggerBindable.
func (w *fakeWriter) BindLogger(logger *slog.Logger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.boundLogger = logger
}

// BindSchema implements RowsWriter. It keeps the ctx it was given so a test
// can check whether the caller's later cancellation reached it.
func (w *fakeWriter) BindSchema(ctx context.Context, schema bigquery.Schema) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.binds++
	w.boundSchema = schema
	w.boundCtx = ctx
	return w.bindErr
}

// WriteRows implements RowsWriter. It returns 0 and writeErr when set, and
// leaves the rows out of what it recorded, the way a real writer that took
// nothing from a failed batch would.
func (w *fakeWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	w.batchSizes = append(w.batchSizes, len(rows))
	if w.takeErr != nil {
		return nil, w.takeErr
	}
	if w.writeErr != nil {
		return ResolvedResult(0, w.writeErr), nil
	}
	for _, r := range rows {
		w.rows = append(w.rows, r.Values)
	}
	return ResolvedResult(len(rows)-w.underReport, nil), nil
}

// Close implements RowsWriter.
func (w *fakeWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closes++
	return nil
}

// TestSinkRejectsAWriterThatUnderReports covers the guard that turns a writer's
// silence into an error, the way bufio turns a short underlying write into
// io.ErrShortWrite. Without it a custom RowsWriter could lose a row and have Sink
// report success, which is the failure this whole interface exists to rule out.
func TestSinkRejectsAWriterThatUnderReports(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	writer.underReport = 1
	s := newTestSinker[nestedRow](t, migratedTable(), writer,
		WithMigrationStrategy(AppendNewColumns{}, nil))

	n, err := s.Sink(t.Context(), []nestedRow{{}, {}})
	if err == nil {
		t.Fatal("Sink() error = nil, want a report that the writer left a row unaccounted for")
	}
	if n != 1 {
		t.Errorf("Sink() n = %d, want the count the writer itself reported", n)
	}
}

// TestBindSchemaErrorIsCached checks that a failure to bind the schema is
// cached along with the rest of start's outcome: a later Sink returns the same
// failure without the writer being asked to bind the schema again.
func TestBindSchemaErrorIsCached(t *testing.T) {
	t.Parallel()

	fake := &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
	sentinel := errors.New("cannot bind schema")
	writer := newFakeWriter(t)
	writer.bindErr = sentinel
	s := newTestSinker[nestedRow](t, fake, writer, WithMigrationStrategy(AppendNewColumns{}, nil))

	ctx := t.Context()
	for i := range 2 {
		if _, err := s.Sink(ctx, nestedRow{}); !errors.Is(err, sentinel) {
			t.Fatalf("Sink() call %d error = %v, want the bind failure", i+1, err)
		}
	}
	writer.mu.Lock()
	binds := writer.binds
	writer.mu.Unlock()
	if binds != 1 {
		t.Errorf("BindSchema was called %d times, want 1; the failure must be cached", binds)
	}
}

func TestSinkReturnsTheWriteError(t *testing.T) {
	t.Parallel()

	fake := &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
	sentinel := errors.New("write refused")
	writer := newFakeWriter(t)
	writer.writeErr = sentinel
	s := newTestSinker[nestedRow](t, fake, writer, WithMigrationStrategy(AppendNewColumns{}, nil))

	n, err := s.Sink(t.Context(), nestedRow{})
	if !errors.Is(err, sentinel) {
		t.Errorf("Sink() error = %v, want the write failure", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

// TestSinkReturnsTheTakeError checks that a writer refusing a batch outright,
// rather than taking it and then failing, is reported the same way: no rows
// written and the writer's own error returned.
func TestSinkReturnsTheTakeError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("writer refused the rows")
	writer := newFakeWriter(t)
	writer.takeErr = sentinel
	s := newTestSinker[nestedRow](t, migratedTable(), writer)

	n, err := s.Sink(t.Context(), nestedRow{})
	if !errors.Is(err, sentinel) {
		t.Errorf("Sink() error = %v, want the take failure", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

// TestSinkDoesNotRetryAWriteFailure checks that the Sinker leaves retrying a
// write to the writer: a writer that fails once is called exactly once and its
// failure is returned as is, even for an error that looks transient.
func TestSinkDoesNotRetryAWriteFailure(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	writer.writeErr = unavailableErr()
	s := newTestSinker[nestedRow](t, migratedTable(), writer)

	if _, err := s.Sink(t.Context(), nestedRow{}); err == nil {
		t.Fatal("Sink() error = nil, want the write failure")
	}
	writer.mu.Lock()
	calls := writer.calls
	writer.mu.Unlock()
	if calls != 1 {
		t.Errorf("WriteRows was called %d times, want 1; the Sinker must not retry a write", calls)
	}
}

// TestSinkSendsAllRowsInOneWriteRowsCall checks that the rows given to one Sink
// call travel together, rather than one at a time.
func TestSinkSendsAllRowsInOneWriteRowsCall(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, migratedTable(), writer)

	n, err := s.Sink(t.Context(), []nestedRow{{A: "one"}, {A: "two"}, {A: "three"}})
	if err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if n != 3 {
		t.Errorf("Sink() n = %d, want 3", n)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.calls != 1 {
		t.Errorf("WriteRows was called %d times, want 1", writer.calls)
	}
	if len(writer.batchSizes) != 1 || writer.batchSizes[0] != 3 {
		t.Errorf("batch sizes = %v, want a single call of 3", writer.batchSizes)
	}
}

func TestSinkWithNoRowsReturnsZero(t *testing.T) {
	t.Parallel()

	s := newTestSinker[nestedRow](t, migratedTable(), nil)

	n, err := s.Sink(t.Context(), []nestedRow{})
	if err != nil {
		t.Errorf("Sink() with no rows error = %v, want nil without contacting BigQuery", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

// TestSinkTreatsASingleRowAndABatchOfOneTheSame checks that Sink's dynamic
// dispatch on the shape of rows does not change what actually gets written.
func TestSinkTreatsASingleRowAndABatchOfOneTheSame(t *testing.T) {
	t.Parallel()

	single := newFakeWriter(t)
	s1 := newTestSinker[nestedRow](t, migratedTable(), single)
	if _, err := s1.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
		t.Fatalf("Sink() with a single row error = %v", err)
	}

	batch := newFakeWriter(t)
	s2 := newTestSinker[nestedRow](t, migratedTable(), batch)
	if _, err := s2.Sink(t.Context(), []nestedRow{{A: "a", B: 1}}); err != nil {
		t.Fatalf("Sink() with a batch of one error = %v", err)
	}

	if !reflect.DeepEqual(single.rows, batch.rows) {
		t.Errorf("a single row wrote %#v, a batch of one wrote %#v, want them the same", single.rows, batch.rows)
	}
}

// TestSinkTreatsAnArrayAsABatch checks that an array, not only a slice, is read
// as a batch of its elements.
func TestSinkTreatsAnArrayAsABatch(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, migratedTable(), writer)

	n, err := s.Sink(t.Context(), [2]nestedRow{{A: "one", B: 1}, {A: "two", B: 2}})
	if err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if n != 2 {
		t.Errorf("Sink() n = %d, want 2", n)
	}
	if len(writer.batchSizes) != 1 || writer.batchSizes[0] != 2 {
		t.Errorf("batch sizes = %v, want a single call of 2", writer.batchSizes)
	}
}

// TestSinkTreatsAUniformAnySliceAsABatch checks that a []any holding rows of
// the same underlying type is unboxed and written as a batch, not rejected
// the way a mixed-type []any is.
func TestSinkTreatsAUniformAnySliceAsABatch(t *testing.T) {
	t.Parallel()

	writer := newFakeWriter(t)
	s := newTestSinker[nestedRow](t, migratedTable(), writer)

	n, err := s.Sink(t.Context(), []any{nestedRow{A: "one", B: 1}, nestedRow{A: "two", B: 2}})
	if err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if n != 2 {
		t.Errorf("Sink() n = %d, want 2", n)
	}
	if len(writer.batchSizes) != 1 || writer.batchSizes[0] != 2 {
		t.Errorf("batch sizes = %v, want a single call of 2", writer.batchSizes)
	}
}

func TestLoadJobsNewWriterDoesNotFail(t *testing.T) {
	t.Parallel()

	w, err := (&LoadJobs{}).NewWriter(testClient(t), testRelation())
	if err != nil {
		t.Fatalf("NewWriter() error = %v, want nil since it contacts nothing", err)
	}
	if w == nil {
		t.Fatal("NewWriter() returned a nil writer")
	}
}

func TestTransportValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy Validator
		wantErr  bool
	}{
		{name: "an empty StorageWrite is fine", strategy: &StorageWrite{}},
		{name: "an empty LoadJobs is fine", strategy: &LoadJobs{}},
		{
			name:     "a negative FlushRows is rejected, since it is not a number of rows",
			strategy: &LoadJobs{FlushRows: -1},
			wantErr:  true,
		},
		{
			name:     "a stream type alone is fine",
			strategy: &StorageWrite{StreamType: managedwriter.CommittedStream},
		},
		{
			name:     "a stream name alone is fine",
			strategy: &StorageWrite{StreamName: "projects/p/datasets/d/tables/t/streams/s"},
		},
		{
			name: "both together conflict",
			strategy: &StorageWrite{
				StreamName: "projects/p/datasets/d/tables/t/streams/s",
				StreamType: managedwriter.CommittedStream,
			},
			wantErr: true,
		},
		{
			name:     "the default stream is fine",
			strategy: &StorageWrite{StreamType: managedwriter.DefaultStream},
		},
		{
			name:     "a pending stream is rejected, since bqsink never commits it",
			strategy: &StorageWrite{StreamType: managedwriter.PendingStream},
			wantErr:  true,
		},
		{
			name:     "a buffered stream is rejected, since bqsink never flushes its offset",
			strategy: &StorageWrite{StreamType: managedwriter.BufferedStream},
			wantErr:  true,
		},
		{
			name:     "an unknown stream type is rejected",
			strategy: &StorageWrite{StreamType: managedwriter.StreamType("NOPE")},
			wantErr:  true,
		},
		{name: "an empty LoadJobs is fine", strategy: &LoadJobs{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.strategy.Validate()
			if tt.wantErr && err == nil {
				t.Error("Validate() error = nil, want an error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() error = %v, want nil", err)
			}
		})
	}
}

// migratedTable builds a fake table whose schema already matches, so that
// Migrate has nothing to do.
func migratedTable() *fakeTable {
	return &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
}

// migratedTableFor builds a fake table whose schema already matches, so that
// Migrate has nothing to do.
func migratedTableFor(schema bigquery.Schema) *fakeTable {
	return &fakeTable{metadata: &bigquery.TableMetadata{ETag: "etag-1", Schema: schema}}
}

// appendedRows reaches the rows a fakeWriter recorded.
func appendedRows(w *fakeWriter) []map[string]bigquery.Value {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rows
}

// TestNewWriterSettlesTheRelation checks what a writer reports as its own
// relation: an empty ProjectID is filled in from the client, an explicit one is
// kept, and a nil client is refused. Every migration goes through the relation a
// writer reports, so getting it wrong points the reconciliation at another table.
func TestNewWriterSettlesTheRelation(t *testing.T) {
	t.Parallel()

	newWriters := map[string]func(*bigquery.Client, Relation) (RowsWriter, error){
		"LoadJobs": func(c *bigquery.Client, r Relation) (RowsWriter, error) {
			return (&LoadJobs{}).NewWriter(c, r)
		},
		"StorageWrite": func(c *bigquery.Client, r Relation) (RowsWriter, error) {
			return (&StorageWrite{}).NewWriter(c, r)
		},
	}
	tests := []struct {
		name     string
		relation Relation
		want     string
	}{
		{
			name:     "an empty ProjectID comes from the client",
			relation: Relation{DatasetID: "test_dataset", TableID: "test_table"},
			want:     "test-project",
		},
		{
			name:     "an explicit ProjectID is kept",
			relation: Relation{ProjectID: "other-project", DatasetID: "test_dataset", TableID: "test_table"},
			want:     "other-project",
		},
	}
	for transport, newWriter := range newWriters {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()

			client := testClient(t)
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					w, err := newWriter(client, tt.relation)
					if err != nil {
						t.Fatalf("NewWriter() error = %v", err)
					}
					if got := w.Relation().ProjectID; got != tt.want {
						t.Errorf("Relation().ProjectID = %q, want %q", got, tt.want)
					}
					if w.Client() != client {
						t.Error("Client() did not report the client the writer was built with")
					}
				})
			}
			t.Run("a nil client is refused", func(t *testing.T) {
				if _, err := newWriter(nil, testRelation()); err == nil {
					t.Error("NewWriter() error = nil, want a nil client to be refused")
				}
			})
		})
	}
}

// TestConcurrentSinksAcceptEveryRowAndDeliverItExactlyOnce checks the writer
// under the only arrangement where FlushRows gathers rows across calls:
// several goroutines sinking through one shared LoadJobsWriter at once.
//
// Acceptance resolves the moment WriteRows returns: whichever goroutine
// happens to fill the buffer to FlushRows submits it synchronously and
// blocks until that job finishes, but every goroutine's WriteResult —
// including that one's — was already resolved with acceptance before its
// call returned. So counts[i] only checks that every goroutine's row was
// taken into the buffer, not that it was delivered; delivery is what the
// loader is checked for below, once every goroutine and the Close after them
// have finished.
//
// With goroutines chosen to divide evenly by FlushRows, the number of load
// jobs does not depend on how the goroutines happen to interleave: mu
// serializes every append and threshold check into one total order, so the
// buffer fills exactly goroutines/flushRows times whatever that order is.
func TestConcurrentSinksAcceptEveryRowAndDeliverItExactlyOnce(t *testing.T) {
	t.Parallel()

	const goroutines = 8
	const flushRows = 4
	loader := &fakeLoader{}
	w, err := (&LoadJobs{FlushRows: flushRows}).NewWriter(testClient(t), testRelation())
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	w.loader = loader
	s := newTestSinker[simpleRow](t, migratedTableFor(bigquery.Schema{
		{Name: "Name", Type: bigquery.StringFieldType},
		{Name: "Count", Type: bigquery.IntegerFieldType},
	}), w)

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	counts := make([]int, goroutines)
	start := make(chan struct{})
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			counts[i], errs[i] = s.Sink(t.Context(), []simpleRow{{Name: "a", Count: int64(i)}})
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Sink() from goroutine %d error = %v", i, err)
		}
		if counts[i] != 1 {
			t.Errorf("Sink() from goroutine %d n = %d, want 1 (acceptance, not delivery)", i, counts[i])
		}
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	calls := loader.snapshot()
	var lines int
	for _, call := range calls {
		lines += strings.Count(call.rows, "\n")
	}
	if lines != goroutines {
		t.Errorf("%d row(s) reached a load job, want %d: every row has to land exactly once", lines, goroutines)
	}
	if jobs, want := len(calls), goroutines/flushRows; jobs != want {
		t.Errorf("%d load job(s) were submitted, want exactly %d", jobs, want)
	}
}

// nilResultWriter breaks the RowsWriter contract by taking rows and reporting
// neither a result nor an error.
type nilResultWriter struct{ fakeWriter }

func (w *nilResultWriter) WriteRows(context.Context, []Row) (WriteResult, error) {
	return nil, nil
}

// TestSinkRejectsAWriterThatReportsNothing covers the guard on a writer that
// takes rows and hands back neither a result nor an error. Letting that pass
// would be rows gone with Sink reporting success.
func TestSinkRejectsAWriterThatReportsNothing(t *testing.T) {
	t.Parallel()

	writer := &nilResultWriter{fakeWriter: *newFakeWriter(t)}
	s := newTestSinker[nestedRow](t, migratedTable(), writer, WithMigrationStrategy(AppendNewColumns{}, nil))

	n, err := s.Sink(t.Context(), nestedRow{})
	if err == nil {
		t.Fatal("Sink() error = nil, want a writer reporting nothing to be refused")
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

// TestSinkWithoutAClientWritesUnderMigrationNone checks the whole path for a
// writer not connected to BigQuery: nothing is reconciled, the schema still
// reaches the writer, and the rows are written.
func TestSinkWithoutAClientWritesUnderMigrationNone(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{relation: testRelation()}
	s, err := NewSinker(writer, DeclarationOf[simpleRow](), WithMigrationStrategy(MigrationNone{}, nil))
	if err != nil {
		t.Fatalf("NewSinker() error = %v", err)
	}
	n, err := s.Sink(t.Context(), simpleRow{Name: "a", Count: 1})
	if err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if n != 1 {
		t.Errorf("Sink() n = %d, want 1", n)
	}
	if writer.binds != 1 {
		t.Errorf("BindSchema was called %d time(s), want 1 even with nothing to reconcile", writer.binds)
	}
	if rows := appendedRows(writer); len(rows) != 1 {
		t.Errorf("%d row(s) were appended, want 1", len(rows))
	}
}
