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

type fakeRowWriter struct {
	mu sync.Mutex

	opens        int
	openedSchema bigquery.Schema
	openedTable  *bigquery.Table
	openedLogger *slog.Logger

	rows      []map[string]bigquery.Value
	appendErr error

	flushes int
	closes  int
}

func (w *fakeRowWriter) Append(ctx context.Context, row map[string]bigquery.Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.appendErr != nil {
		return w.appendErr
	}
	w.rows = append(w.rows, row)
	return nil
}

func (w *fakeRowWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushes++
	return nil
}

func (w *fakeRowWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closes++
	return nil
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
	s := newTestSinker[nestedRow](t, fake, WithMigration(AppendNewColumns{}), WithWriteStrategy(strategy))

	ctx := t.Context()
	for i := range 2 {
		if err := s.Sink(ctx, nestedRow{}); !errors.Is(err, sentinel) {
			t.Fatalf("Sink() call %d error = %v, want the open failure", i+1, err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Errorf("Flush() error = %v, want nil while no writer opened", err)
	}
}

func TestAppendErrorSurfaces(t *testing.T) {
	t.Parallel()

	fake := &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
	sentinel := errors.New("append refused")
	writer := &fakeRowWriter{appendErr: sentinel}
	s := newTestSinker[nestedRow](t, fake,
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
	)

	if err := s.Sink(t.Context(), nestedRow{}); !errors.Is(err, sentinel) {
		t.Errorf("Sink() error = %v, want the append failure", err)
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

func TestLoadJobsFlushRowsDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  int
		want int
	}{
		{name: "unset means the default", set: 0, want: DefaultFlushRows},
		{name: "a positive value is kept", set: 250, want: 250},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := (&LoadJobs{FlushRows: tt.set}).flushRows(); got != tt.want {
				t.Errorf("flushRows() = %d, want %d", got, tt.want)
			}
		})
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
		{name: "a positive threshold is fine", strategy: &LoadJobs{FlushRows: 500}},
		{name: "a negative threshold is rejected", strategy: &LoadJobs{FlushRows: -1}, wantErr: true},
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

// flakyWriteStrategy hands out a writer that fails a set number of times before
// succeeding, so the Sinker's retry can be observed.
type flakyWriteStrategy struct {
	writer *flakyRowWriter
}

func (s *flakyWriteStrategy) Open(_ context.Context, _ *bigquery.Table, _ bigquery.Schema, _ *slog.Logger) (RowWriter, error) {
	return s.writer, nil
}

type flakyRowWriter struct {
	mu sync.Mutex

	// failAppends is how many Append calls fail before one succeeds.
	failAppends int
	appendErr   error

	appends int
	rows    []map[string]bigquery.Value

	failFlushes int
	flushes     int
}

func (w *flakyRowWriter) Append(ctx context.Context, row map[string]bigquery.Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.appends++
	if w.failAppends > 0 {
		w.failAppends--
		return w.appendErr
	}
	w.rows = append(w.rows, row)
	return nil
}

func (w *flakyRowWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushes++
	if w.failFlushes > 0 {
		w.failFlushes--
		return w.appendErr
	}
	return nil
}

func (w *flakyRowWriter) Close(ctx context.Context) error { return nil }

func (w *flakyRowWriter) counts() (appends, flushes, rows int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appends, w.flushes, len(w.rows)
}

func migratedTable() *fakeTable {
	return &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
}

func TestSinkRetriesATransientAppendFailure(t *testing.T) {
	t.Parallel()

	writer := &flakyRowWriter{failAppends: 2, appendErr: unavailableErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
	)

	if err := s.Sink(t.Context(), nestedRow{A: "a", B: 1}); err != nil {
		t.Fatalf("Sink() error = %v, want the retry to get through", err)
	}
	appends, _, rows := writer.counts()
	if appends != 3 {
		t.Errorf("Append was called %d times, want 3 (two failures then a success)", appends)
	}
	if rows != 1 {
		t.Errorf("%d row(s) landed, want 1", rows)
	}
}

func TestSinkDoesNotRetryAPermanentFailure(t *testing.T) {
	t.Parallel()

	writer := &flakyRowWriter{failAppends: 1, appendErr: forbiddenErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
	)

	if err := s.Sink(t.Context(), nestedRow{}); err == nil {
		t.Fatal("Sink() error = nil, want the permission failure")
	}
	if appends, _, _ := writer.counts(); appends != 1 {
		t.Errorf("Append was called %d times, want 1; a permission failure must not be retried", appends)
	}
}

func TestSinkGivesUpAfterTheRetryLimit(t *testing.T) {
	t.Parallel()

	writer := &flakyRowWriter{failAppends: 99, appendErr: unavailableErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
	)

	if err := s.Sink(t.Context(), nestedRow{}); err == nil {
		t.Fatal("Sink() error = nil, want the last failure")
	}
	if appends, _, _ := writer.counts(); appends != migrateMaxRetries+1 {
		t.Errorf("Append was called %d times, want %d", appends, migrateMaxRetries+1)
	}
}

func TestFlushRetriesATransientFailure(t *testing.T) {
	t.Parallel()

	writer := &flakyRowWriter{failFlushes: 1, appendErr: unavailableErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
	)

	ctx := t.Context()
	if err := s.Sink(ctx, nestedRow{}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v, want the retry to get through", err)
	}
	if _, flushes, _ := writer.counts(); flushes != 2 {
		t.Errorf("Flush was called %d times, want 2 (one failure then a success)", flushes)
	}
}

func TestSinkAllRetriesEachRow(t *testing.T) {
	t.Parallel()

	writer := &flakyRowWriter{failAppends: 1, appendErr: unavailableErr()}
	s := newTestSinker[nestedRow](t, migratedTable(),
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&flakyWriteStrategy{writer: writer}),
	)

	err := s.SinkAll(t.Context(), nestedRow{A: "one"}, nestedRow{A: "two"})
	if err != nil {
		t.Fatalf("SinkAll() error = %v", err)
	}
	appends, _, rows := writer.counts()
	if appends != 3 {
		t.Errorf("Append was called %d times, want 3 (a retry on the first row)", appends)
	}
	if rows != 2 {
		t.Errorf("%d row(s) landed, want 2", rows)
	}
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

// recordingFlakyStrategy hands out a writer that records every attempt, including
// the failed ones, so a retry can be inspected.
type recordingFlakyStrategy struct {
	writer *recordingFlakyWriter
}

func (s *recordingFlakyStrategy) Open(_ context.Context, _ *bigquery.Table, _ bigquery.Schema, _ *slog.Logger) (RowWriter, error) {
	return s.writer, nil
}

type recordingFlakyWriter struct {
	mu sync.Mutex

	failAppends int
	appendErr   error
	attempts    []map[string]bigquery.Value
}

func (w *recordingFlakyWriter) Append(ctx context.Context, row map[string]bigquery.Value) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts = append(w.attempts, row)
	if w.failAppends > 0 {
		w.failAppends--
		return w.appendErr
	}
	return nil
}

func (w *recordingFlakyWriter) Flush(ctx context.Context) error { return nil }
func (w *recordingFlakyWriter) Close(ctx context.Context) error { return nil }

func (w *recordingFlakyWriter) seenRows() []map[string]bigquery.Value {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.attempts
}
