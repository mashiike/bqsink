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
	gax "github.com/googleapis/gax-go/v2"
)

type loadCall struct {
	rows   string
	schema bigquery.Schema
}

// fakeLoader records the load jobs a writer would submit, failing the calls
// errs names by index and succeeding the rest.
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

func abSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "A", Type: bigquery.StringFieldType},
		{Name: "B", Type: bigquery.IntegerFieldType},
	}
}

func rowOf(id string, values map[string]bigquery.Value) Row {
	return Row{ID: id, Values: values}
}

// noRetryPolicy never lets an attempt be retried, unlike fastRetryPolicy which
// still retries a retryable error.
func noRetryPolicy() gax.Retryer {
	return gax.OnErrorFunc(gax.Backoff{}, func(error) bool { return false })
}

func TestLoadJobsWriteRowsSubmitsOneJob(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &loadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
		rowOf("r3", map[string]bigquery.Value{"A": "x", "B": int64(2)}),
	}
	n, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	if n != len(rows) {
		t.Errorf("WriteRows() n = %d, want %d", n, len(rows))
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("the load job carried %d lines, want 3", len(lines))
	}
	if !reflect.DeepEqual(calls[0].schema, abSchema()) {
		t.Errorf("the load job was given %s, want the declared schema", formatSchema(calls[0].schema))
	}
}

func TestLoadJobsWriteRowsWithNoRowsDoesNothing(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &loadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	n, err := w.WriteRows(t.Context(), nil)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	if n != 0 {
		t.Errorf("WriteRows() n = %d, want 0", n)
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Errorf("load was called %d times with nothing to write, want 0", len(calls))
	}
}

// TestLoadJobsWriteRowsRetriesAndResendsEveryRow is the regression test for the
// bug where a retried load submitted after the buffer had been cleared, so the
// second attempt sent nothing and returned nil while the rows were never
// written. It checks not just that load was called twice, but that the
// successful call still carried every row, byte for byte the same as the first.
func TestLoadJobsWriteRowsRetriesAndResendsEveryRow(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{errs: []error{unavailableErr(), nil}}
	w := &loadJobsWriter{loader: loader, schema: abSchema(), retryPolicy: fastRetryPolicy, logger: discardLogger()}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "y", "B": int64(1)}),
		rowOf("r3", map[string]bigquery.Value{"A": "z", "B": int64(2)}),
	}
	n, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v, want the retry to get through", err)
	}
	if n != len(rows) {
		t.Errorf("WriteRows() n = %d, want %d", n, len(rows))
	}
	calls := loader.snapshot()
	if len(calls) != 2 {
		t.Fatalf("load was called %d times, want 2 (one failure then a success)", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[1].rows, "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("the successful load carried %d lines, want all 3 rows", len(lines))
	}
	if calls[1].rows != calls[0].rows {
		t.Errorf("the retried load carried a different payload:\nfirst:  %q\nsecond: %q", calls[0].rows, calls[1].rows)
	}
}

func TestLoadJobsWriteRowsGivesUpAfterExhaustingRetries(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("load refused")
	loader := &fakeLoader{errs: []error{sentinel}}
	w := &loadJobsWriter{loader: loader, retryPolicy: noRetryPolicy, schema: abSchema(), logger: discardLogger()}

	n, err := w.WriteRows(t.Context(), []Row{rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1)})})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WriteRows() error = %v, want the load failure", err)
	}
	if n != 0 {
		t.Errorf("WriteRows() n = %d, want 0", n)
	}
	if calls := loader.snapshot(); len(calls) != 1 {
		t.Errorf("load was called %d times, want 1; a policy that retries nothing must not retry", len(calls))
	}
}

func TestLoadJobsWriteRowsRetryPolicyControlsWhetherItRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		retryPolicy func() gax.Retryer
		wantCalls   int
	}{
		{name: "nil means one attempt", retryPolicy: nil, wantCalls: 1},
		{name: "a policy that retries nothing means one attempt", retryPolicy: noRetryPolicy, wantCalls: 1},
		{name: "a retryable failure is retried", retryPolicy: fastRetryPolicy, wantCalls: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loader := &fakeLoader{errs: []error{unavailableErr(), nil}}
			w := &loadJobsWriter{loader: loader, retryPolicy: tt.retryPolicy, schema: abSchema(), logger: discardLogger()}

			_, _ = w.WriteRows(t.Context(), []Row{rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1)})})
			if calls := loader.snapshot(); len(calls) != tt.wantCalls {
				t.Errorf("load was called %d times, want %d", len(calls), tt.wantCalls)
			}
		})
	}
}

func TestLoadJobsWriteRowsRejectsARowTheSchemaDoesNotCover(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &loadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	n, err := w.WriteRows(t.Context(), []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1), "C": "extra"}),
	})
	if err == nil {
		t.Fatal("WriteRows() error = nil, want a rejection of the unknown column")
	}
	if n != 0 {
		t.Errorf("WriteRows() n = %d, want 0", n)
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Errorf("load was called %d times, want 0", len(calls))
	}
}

func TestLoadJobsRetryPolicyDefault(t *testing.T) {
	t.Parallel()

	if (&LoadJobs{}).retryPolicy() == nil {
		t.Error("retryPolicy() = nil, want DefaultRetryPolicy when RetryPolicy is unset")
	}
	built := 0
	replacement := func() gax.Retryer {
		built++
		return noRetryPolicy()
	}
	if got := (&LoadJobs{RetryPolicy: replacement}).retryPolicy(); got == nil {
		t.Fatal("retryPolicy() = nil, want the replacement")
	} else if got() == nil {
		t.Error("the replacement policy was not reached")
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

func TestLoadJobsStagesThroughAStager(t *testing.T) {
	t.Parallel()

	stager := &fakeStager{}
	table := &bigquery.Table{ProjectID: "p", DatasetID: "d", TableID: "t"}
	w, err := (&LoadJobs{Staging: stager}).Open(t.Context(), table, abSchema(), discardLogger())
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
