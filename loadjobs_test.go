package bqsink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

type loadCall struct {
	rows   string
	schema bigquery.Schema
}

// fakeLoader records the load jobs a writer would submit.
type fakeLoader struct {
	mu    sync.Mutex
	calls []loadCall
	errs  []error
}

func (l *fakeLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := len(l.calls)
	l.calls = append(l.calls, loadCall{rows: string(rows), schema: schema})
	if i < len(l.errs) && l.errs[i] != nil {
		return l.errs[i]
	}
	return nil
}

func (l *fakeLoader) snapshot() []loadCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

// testLoadWriter builds a writer whose size threshold is out of the way, so that
// a test setting flushRows exercises the row count alone.
func testLoadWriter(loader jobLoader, flushRows int, schema bigquery.Schema) *loadJobsWriter {
	return &loadJobsWriter{
		loader:     loader,
		schema:     schema,
		flushRows:  flushRows,
		flushBytes: DefaultFlushBytes,
		logger:     discardLogger(),
	}
}

func abSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "A", Type: bigquery.StringFieldType},
		{Name: "B", Type: bigquery.IntegerFieldType},
	}
}

func TestLoadJobsBuffersUntilTheThreshold(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := testLoadWriter(loader, 3, abSchema())
	ctx := t.Context()

	for i := range 2 {
		if err := w.Append(ctx, map[string]bigquery.Value{"A": "x", "B": int64(i)}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Fatalf("a load job was submitted after 2 of 3 rows: %d call(s)", len(calls))
	}

	if err := w.Append(ctx, map[string]bigquery.Value{"A": "x", "B": int64(2)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1 once the threshold was reached", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("the load job carried %d lines, want 3", len(lines))
	}
	if !reflect.DeepEqual(calls[0].schema, abSchema()) {
		t.Errorf("the load job was given %s, want the declared schema", formatSchema(calls[0].schema))
	}
}

func TestLoadJobsFlushSubmitsWhatIsHeld(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := testLoadWriter(loader, 1000, abSchema())
	ctx := t.Context()

	if err := w.Append(ctx, map[string]bigquery.Value{"A": "only", "B": int64(1)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1", len(calls))
	}
	if want := `{"A":"only","B":1}` + "\n"; calls[0].rows != want {
		t.Errorf("rows = %q, want %q", calls[0].rows, want)
	}
}

func TestLoadJobsFlushWithNothingHeldDoesNothing(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := testLoadWriter(loader, 10, abSchema())
	ctx := t.Context()

	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Errorf("load was called %d times with nothing buffered, want 0", len(calls))
	}
}

func TestLoadJobsCloseFlushes(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := testLoadWriter(loader, 1000, abSchema())
	ctx := t.Context()

	if err := w.Append(ctx, map[string]bigquery.Value{"A": "x", "B": int64(1)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if calls := loader.snapshot(); len(calls) != 1 {
		t.Errorf("load was called %d times, want 1 from Close", len(calls))
	}
}

// TestLoadJobsDropsRowsOnFailure guards against the buffer growing without bound
// when a table keeps rejecting loads.
func TestLoadJobsDropsRowsOnFailure(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("load refused")
	loader := &fakeLoader{errs: []error{sentinel, nil}}
	w := testLoadWriter(loader, 1, abSchema())
	ctx := t.Context()

	err := w.Append(ctx, map[string]bigquery.Value{"A": "lost", "B": int64(1)})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Append() error = %v, want the load failure", err)
	}
	if !strings.Contains(err.Error(), "dropped") {
		t.Errorf("Append() error = %v, want it to say the rows were dropped", err)
	}

	if err := w.Append(ctx, map[string]bigquery.Value{"A": "kept", "B": int64(2)}); err != nil {
		t.Fatalf("the second Append() error = %v", err)
	}
	calls := loader.snapshot()
	if len(calls) != 2 {
		t.Fatalf("load was called %d times, want 2", len(calls))
	}
	if strings.Contains(calls[1].rows, "lost") {
		t.Errorf("the second load carried the dropped row: %q", calls[1].rows)
	}
	if !strings.Contains(calls[1].rows, "kept") {
		t.Errorf("the second load did not carry the new row: %q", calls[1].rows)
	}
}

func TestLoadJobsRejectsARowTheSchemaDoesNotCover(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := testLoadWriter(loader, 1, abSchema())
	err := w.Append(t.Context(), map[string]bigquery.Value{"A": "x", "B": int64(1), "C": "extra"})
	if err == nil {
		t.Fatal("Append() error = nil, want a rejection of the unknown column")
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Errorf("load was called %d times, want 0", len(calls))
	}
}

func TestEncodeJSONRow(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name   string
		row    map[string]bigquery.Value
		schema bigquery.Schema
		want   string
	}{
		{
			name:   "a timestamp is RFC3339",
			row:    map[string]bigquery.Value{"at": at},
			schema: bigquery.Schema{{Name: "at", Type: bigquery.TimestampFieldType}},
			want:   `{"at":"2026-07-28T12:30:00Z"}`,
		},
		{
			name: "a date, time and datetime use their text form",
			row:  map[string]bigquery.Value{"d": civil.DateOf(at), "t": civil.TimeOf(at), "dt": civil.DateTimeOf(at)},
			schema: bigquery.Schema{
				{Name: "d", Type: bigquery.DateFieldType},
				{Name: "t", Type: bigquery.TimeFieldType},
				{Name: "dt", Type: bigquery.DateTimeFieldType},
			},
			want: `{"d":"2026-07-28","dt":"2026-07-28T12:30:00","t":"12:30:00"}`,
		},
		{
			name:   "a fractional NUMERIC becomes decimal text, not a fraction",
			row:    map[string]bigquery.Value{"n": big.NewRat(25, 2)},
			schema: bigquery.Schema{{Name: "n", Type: bigquery.NumericFieldType}},
			want:   `{"n":"12.500000000"}`,
		},
		{
			name:   "an integral NUMERIC keeps every digit",
			row:    map[string]bigquery.Value{"n": new(big.Rat).SetUint64(18446744073709551615)},
			schema: bigquery.Schema{{Name: "n", Type: bigquery.NumericFieldType}},
			want:   `{"n":"18446744073709551615"}`,
		},
		{
			name:   "a BIGNUMERIC keeps 38 fractional digits",
			row:    map[string]bigquery.Value{"n": big.NewRat(1, 3)},
			schema: bigquery.Schema{{Name: "n", Type: bigquery.BigNumericFieldType}},
			want:   `{"n":"0.33333333333333333333333333333333333333"}`,
		},
		{
			name:   "an explicit scale wins",
			row:    map[string]bigquery.Value{"n": big.NewRat(1, 3)},
			schema: bigquery.Schema{{Name: "n", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2}},
			want:   `{"n":"0.33"}`,
		},
		{
			name:   "bytes are base64",
			row:    map[string]bigquery.Value{"b": []byte("bytes")},
			schema: bigquery.Schema{{Name: "b", Type: bigquery.BytesFieldType}},
			want:   `{"b":"Ynl0ZXM="}`,
		},
		{
			name:   "a NULL stays null",
			row:    map[string]bigquery.Value{"a": nil},
			schema: bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}},
			want:   `{"a":null}`,
		},
		{
			name:   "a repeated column becomes an array",
			row:    map[string]bigquery.Value{"tags": []bigquery.Value{"x", "y"}},
			schema: bigquery.Schema{{Name: "tags", Type: bigquery.StringFieldType, Repeated: true}},
			want:   `{"tags":["x","y"]}`,
		},
		{
			name: "a RECORD becomes a nested object",
			row: map[string]bigquery.Value{
				"inner": map[string]bigquery.Value{"A": "a", "B": int64(1)},
			},
			schema: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: abSchema()},
			},
			want: `{"inner":{"A":"a","B":1}}`,
		},
		{
			name: "a repeated RECORD becomes an array of objects",
			row: map[string]bigquery.Value{
				"items": []bigquery.Value{
					map[string]bigquery.Value{"A": "a", "B": int64(1)},
					map[string]bigquery.Value{"A": "b", "B": int64(2)},
				},
			},
			schema: bigquery.Schema{
				{Name: "items", Type: bigquery.RecordFieldType, Repeated: true, Schema: abSchema()},
			},
			want: `{"items":[{"A":"a","B":1},{"A":"b","B":2}]}`,
		},
		{
			// Quoting the text would store the string `{"k":"v"}` rather than the
			// object it stands for.
			name:   "a JSON column embeds its value rather than quoting it",
			row:    map[string]bigquery.Value{"j": `{"k":"v"}`},
			schema: bigquery.Schema{{Name: "j", Type: bigquery.JSONFieldType}},
			want:   `{"j":{"k":"v"}}`,
		},
		{
			name:   "a json.RawMessage in a JSON column is embedded too",
			row:    map[string]bigquery.Value{"j": json.RawMessage(`[1,2]`)},
			schema: bigquery.Schema{{Name: "j", Type: bigquery.JSONFieldType}},
			want:   `{"j":[1,2]}`,
		},
		{
			name:   "HTML is not escaped",
			row:    map[string]bigquery.Value{"a": "https://x/?a=1&b=2"},
			schema: bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}},
			want:   `{"a":"https://x/?a=1&b=2"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := encodeJSONRow(tt.row, tt.schema)
			if err != nil {
				t.Fatalf("encodeJSONRow() error = %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("encodeJSONRow() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestEncodeJSONRowError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		row    map[string]bigquery.Value
		schema bigquery.Schema
	}{
		{
			name:   "a column the schema lacks",
			row:    map[string]bigquery.Value{"nope": "x"},
			schema: bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}},
		},
		{
			name:   "a repeated column given a scalar",
			row:    map[string]bigquery.Value{"tags": "x"},
			schema: bigquery.Schema{{Name: "tags", Type: bigquery.StringFieldType, Repeated: true}},
		},
		{
			name:   "a RECORD given a scalar",
			row:    map[string]bigquery.Value{"inner": "x"},
			schema: bigquery.Schema{{Name: "inner", Type: bigquery.RecordFieldType, Schema: abSchema()}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := encodeJSONRow(tt.row, tt.schema); err == nil {
				t.Fatal("encodeJSONRow() error = nil, want an error")
			}
		})
	}
}

// fakeStager records what it was asked to stage.
type fakeStager struct {
	mu       sync.Mutex
	staged   []string
	cleanups int
	stageErr error
	validate error
}

func (s *fakeStager) Stage(ctx context.Context, rows []byte) (string, func(context.Context) error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stageErr != nil {
		return "", nil, s.stageErr
	}
	s.staged = append(s.staged, string(rows))
	uri := fmt.Sprintf("gs://bucket/staged-%d.json", len(s.staged))
	return uri, func(context.Context) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cleanups++
		return nil
	}, nil
}

func (s *fakeStager) Validate() error { return s.validate }

func (s *fakeStager) snapshot() (staged []string, cleanups int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.staged, s.cleanups
}

func TestLoadJobsFlushesOnSize(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	// One rendered row of abSchema is around 20 bytes, so a small threshold makes
	// the size trigger rather than the row count.
	w := &loadJobsWriter{
		loader:     loader,
		schema:     abSchema(),
		flushRows:  1000,
		flushBytes: 30,
		logger:     discardLogger(),
	}
	ctx := t.Context()

	if err := w.Append(ctx, map[string]bigquery.Value{"A": "first", "B": int64(1)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Fatalf("a load job was submitted before the size threshold: %d call(s)", len(calls))
	}
	if err := w.Append(ctx, map[string]bigquery.Value{"A": "second", "B": int64(2)}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1 once the buffer passed %d bytes", len(calls), 30)
	}
	if !strings.Contains(calls[0].rows, "first") || !strings.Contains(calls[0].rows, "second") {
		t.Errorf("the load job carried %q, want both rows", calls[0].rows)
	}
}

func TestLoadJobsFlushBytesDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  int
		want int
	}{
		{name: "unset means the default", set: 0, want: DefaultFlushBytes},
		{name: "a positive value is kept", set: 4096, want: 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := (&LoadJobs{FlushBytes: tt.set}).flushBytes(); got != tt.want {
				t.Errorf("flushBytes() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLoadJobsStagesThroughAStager(t *testing.T) {
	t.Parallel()

	stager := &fakeStager{}
	table := &bigquery.Table{ProjectID: "p", DatasetID: "d", TableID: "t"}
	w, err := (&LoadJobs{FlushRows: 1, Staging: stager}).Open(t.Context(), table, abSchema(), discardLogger())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	writer, ok := w.(*loadJobsWriter)
	if !ok {
		t.Fatalf("Open() returned %T, want a *loadJobsWriter", w)
	}
	if _, ok := writer.loader.(stagedLoader); !ok {
		t.Errorf("loader = %T, want a stagedLoader once Staging is set", writer.loader)
	}
}

func TestLoadJobsRejectsAnInvalidStager(t *testing.T) {
	t.Parallel()

	stager := &fakeStager{validate: errors.New("no bucket")}
	if _, err := New[nestedRow](testClient(t), testRelation(), WithWriteStrategy(&LoadJobs{Staging: stager})); err == nil {
		t.Fatal("New() error = nil, want the stager's own rejection")
	}
}
