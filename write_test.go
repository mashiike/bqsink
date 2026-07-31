package bqsink

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
)

// fakeWriteStrategy hands out a writer that records what it is given.
type fakeWriteStrategy struct {
	writer  *fakeRowWriter
	openErr error
}

func (s *fakeWriteStrategy) Open(_ context.Context, table *bigquery.Table, schema bigquery.Schema, logger *slog.Logger) (RowWriter, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	s.writer.mu.Lock()
	defer s.writer.mu.Unlock()
	s.writer.opens++
	s.writer.openedSchema = schema
	s.writer.openedTable = table
	s.writer.openedLogger = logger
	return s.writer, nil
}

// fakeRowWriter records what it is given, one WriteRows call at a time. It
// returns 0 and writeErr when set, the way a real writer must when it lands
// none of the rows it was given.
type fakeRowWriter struct {
	mu sync.Mutex

	opens        int
	openedSchema bigquery.Schema
	openedTable  *bigquery.Table
	openedLogger *slog.Logger

	rows       []map[string]bigquery.Value
	batchSizes []int
	calls      int
	writeErr   error

	// underReport makes WriteRows claim it wrote this many fewer rows than it was
	// given while returning no error, which the contract forbids.
	underReport int

	closes int
}

func (w *fakeRowWriter) WriteRows(ctx context.Context, rows []Row) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	w.batchSizes = append(w.batchSizes, len(rows))
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	for _, r := range rows {
		w.rows = append(w.rows, r.Values)
	}
	return len(rows) - w.underReport, nil
}

func (w *fakeRowWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closes++
	return nil
}

// TestSinkRejectsAWriterThatUnderReports covers the guard that turns a writer's
// silence into an error, the way bufio turns a short underlying write into
// io.ErrShortWrite. Without it a custom RowWriter could lose a row and have Sink
// report success, which is the failure this whole interface exists to rule out.
func TestSinkRejectsAWriterThatUnderReports(t *testing.T) {
	t.Parallel()

	writer := &fakeRowWriter{underReport: 1}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigrationStrategy(AppendNewColumns{}, nil),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

	n, err := s.Sink(t.Context(), nestedRow{}, nestedRow{})
	if err == nil {
		t.Fatal("Sink() error = nil, want a report that the writer left a row unaccounted for")
	}
	if n != 1 {
		t.Errorf("Sink() n = %d, want the count the writer itself reported", n)
	}
}

func TestOpenErrorIsCached(t *testing.T) {
	t.Parallel()

	fake := &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
	sentinel := errors.New("cannot open")
	strategy := &fakeWriteStrategy{writer: &fakeRowWriter{}, openErr: sentinel}
	s := newTestSinker[nestedRow](t, fake, WithMigrationStrategy(AppendNewColumns{}, nil), WithWriteStrategy(strategy))

	ctx := t.Context()
	for i := range 2 {
		if _, err := s.Sink(ctx, nestedRow{}); !errors.Is(err, sentinel) {
			t.Fatalf("Sink() call %d error = %v, want the open failure", i+1, err)
		}
	}
	if err := s.Close(ctx); err != nil {
		t.Errorf("Close() error = %v, want nil while no writer opened", err)
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
	writer := &fakeRowWriter{writeErr: sentinel}
	s := newTestSinker[nestedRow](t, fake,
		WithMigrationStrategy(AppendNewColumns{}, nil),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
	)

	n, err := s.Sink(t.Context(), nestedRow{})
	if !errors.Is(err, sentinel) {
		t.Errorf("Sink() error = %v, want the write failure", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

// TestSinkDoesNotRetryAWriteFailure checks that the Sinker leaves retrying a
// write to the write strategy: a writer that fails once is called exactly once
// and its failure is returned as is, even for an error that looks transient.
func TestSinkDoesNotRetryAWriteFailure(t *testing.T) {
	t.Parallel()

	writer := &fakeRowWriter{writeErr: unavailableErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
	)

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

	writer := &fakeRowWriter{}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
	)

	n, err := s.Sink(t.Context(), nestedRow{A: "one"}, nestedRow{A: "two"}, nestedRow{A: "three"})
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

	s := newTestSinker[nestedRow](t, migratedTable())

	n, err := s.Sink(t.Context())
	if err != nil {
		t.Errorf("Sink() with no rows error = %v, want nil without contacting BigQuery", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
}

func TestLoadJobsOpenDoesNotFail(t *testing.T) {
	t.Parallel()

	table := &bigquery.Table{ProjectID: "p", DatasetID: "d", TableID: "t"}
	schema := bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}
	w, err := (&LoadJobs{}).Open(t.Context(), table, schema, discardLogger())
	if err != nil {
		t.Fatalf("Open() error = %v, want nil since it contacts nothing", err)
	}
	if w == nil {
		t.Fatal("Open() returned a nil writer")
	}
}

func TestWriteStrategyValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy Validator
		wantErr  bool
	}{
		{name: "an empty StorageWrite is fine", strategy: &StorageWrite{}},
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

func appendedRows(w *fakeRowWriter) []map[string]bigquery.Value {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rows
}
