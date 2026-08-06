package bqsink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

func (l *fakeLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema, logger *slog.Logger) error {
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
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
		rowOf("r3", map[string]bigquery.Value{"A": "x", "B": int64(2)}),
	}
	res, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	n, err := res.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if n != len(rows) {
		t.Errorf("Wait() n = %d, want %d", n, len(rows))
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
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	res, err := w.WriteRows(t.Context(), nil)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	n, err := res.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if n != 0 {
		t.Errorf("Wait() n = %d, want 0", n)
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
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), retryPolicy: fastRetryPolicy, logger: discardLogger()}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "y", "B": int64(1)}),
		rowOf("r3", map[string]bigquery.Value{"A": "z", "B": int64(2)}),
	}
	res, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	n, err := res.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v, want the retry to get through", err)
	}
	if n != len(rows) {
		t.Errorf("Wait() n = %d, want %d", n, len(rows))
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
	w := &LoadJobsWriter{loader: loader, retryPolicy: noRetryPolicy, schema: abSchema(), logger: discardLogger()}

	res, err := w.WriteRows(t.Context(), []Row{rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1)})})
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	n, err := res.Wait(t.Context())
	if !errors.Is(err, sentinel) {
		t.Fatalf("Wait() error = %v, want the load failure", err)
	}
	if n != 0 {
		t.Errorf("Wait() n = %d, want 0", n)
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
			w := &LoadJobsWriter{loader: loader, retryPolicy: tt.retryPolicy, schema: abSchema(), logger: discardLogger()}

			res, err := w.WriteRows(t.Context(), []Row{rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1)})})
			if err != nil {
				t.Fatalf("WriteRows() error = %v", err)
			}
			_, _ = res.Wait(t.Context())
			if calls := loader.snapshot(); len(calls) != tt.wantCalls {
				t.Errorf("load was called %d times, want %d", len(calls), tt.wantCalls)
			}
		})
	}
}

// TestLoadJobsFlushRowsGroupsMultipleWriteRows checks that rows from separate
// WriteRows calls share one load job once FlushRows has gathered enough of
// them, and that each WriteResult still reports only the rows its own call
// contributed.
func TestLoadJobsFlushRowsGroupsMultipleWriteRows(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger(), flushRows: 4}

	first := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	second := []Row{
		rowOf("r3", map[string]bigquery.Value{"A": "x", "B": int64(2)}),
		rowOf("r4", map[string]bigquery.Value{"A": "x", "B": int64(3)}),
	}
	res1, err := w.WriteRows(t.Context(), first)
	if err != nil {
		t.Fatalf("WriteRows() (first) error = %v", err)
	}
	res2, err := w.WriteRows(t.Context(), second)
	if err != nil {
		t.Fatalf("WriteRows() (second) error = %v", err)
	}
	n1, err := res1.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() (first) error = %v", err)
	}
	if n1 != len(first) {
		t.Errorf("Wait() (first) n = %d, want %d", n1, len(first))
	}
	n2, err := res2.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() (second) error = %v", err)
	}
	if n2 != len(second) {
		t.Errorf("Wait() (second) n = %d, want %d", n2, len(second))
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 4 {
		t.Errorf("the load job carried %d lines, want 4", len(lines))
	}
}

// TestLoadJobsWaitBeforeFlushRowsSubmitsAtOnce checks that waiting on a
// WriteResult before FlushRows has gathered enough rows submits the batch
// right away, which is what makes waiting at once equivalent to writing
// without a buffer at all.
func TestLoadJobsWaitBeforeFlushRowsSubmitsAtOnce(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger(), flushRows: 100}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	res, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	if calls := loader.snapshot(); len(calls) != 0 {
		t.Fatalf("load was called %d times before Wait, want 0", len(calls))
	}
	n, err := res.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if n != len(rows) {
		t.Errorf("Wait() n = %d, want %d", n, len(rows))
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("the load job carried %d lines, want 2", len(lines))
	}
}

// TestLoadJobsFlushSubmitsPendingRows checks that Flush sends what FlushRows is
// still holding back, and that waiting afterwards on the WriteResult from
// before the Flush does not submit the same batch a second time.
func TestLoadJobsFlushSubmitsPendingRows(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger(), flushRows: 100}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	res, err := w.WriteRows(t.Context(), rows)
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	if err := w.Flush(t.Context()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if calls := loader.snapshot(); len(calls) != 1 {
		t.Fatalf("load was called %d times after Flush, want 1", len(calls))
	}
	n, err := res.Wait(t.Context())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if n != len(rows) {
		t.Errorf("Wait() n = %d, want %d", n, len(rows))
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Errorf("load was called %d times after Wait, want still 1 (the same batch, not sent twice)", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("the load job carried %d lines, want 2", len(lines))
	}
}

// TestLoadJobsCloseSubmitsPendingRows checks that Close sends what FlushRows is
// still holding back, and that a WriteRows call after Close is rejected.
func TestLoadJobsCloseSubmitsPendingRows(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger(), flushRows: 100}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	if _, err := w.WriteRows(t.Context(), rows); err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	if err := w.Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	calls := loader.snapshot()
	if len(calls) != 1 {
		t.Fatalf("load was called %d times, want 1", len(calls))
	}
	lines := strings.Split(strings.TrimSuffix(calls[0].rows, "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("the load job carried %d lines, want 2", len(lines))
	}
	if _, err := w.WriteRows(t.Context(), rows); err == nil {
		t.Error("WriteRows() after Close error = nil, want a rejection")
	}
}

// TestLoadJobsBatchFailureReachesEveryWriteResultSharingIt checks that when a
// batch's load job fails, every WriteResult of a WriteRows call that
// contributed rows to it reports the same failure and no rows landed.
func TestLoadJobsBatchFailureReachesEveryWriteResultSharingIt(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("load refused")
	loader := &fakeLoader{errs: []error{sentinel}}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), retryPolicy: noRetryPolicy, logger: discardLogger(), flushRows: 4}

	first := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	second := []Row{
		rowOf("r3", map[string]bigquery.Value{"A": "x", "B": int64(2)}),
		rowOf("r4", map[string]bigquery.Value{"A": "x", "B": int64(3)}),
	}
	res1, err := w.WriteRows(t.Context(), first)
	if err != nil {
		t.Fatalf("WriteRows() (first) error = %v", err)
	}
	res2, err := w.WriteRows(t.Context(), second)
	if err != nil {
		t.Fatalf("WriteRows() (second) error = %v", err)
	}
	n1, err1 := res1.Wait(t.Context())
	if !errors.Is(err1, sentinel) {
		t.Errorf("Wait() (first) error = %v, want %v", err1, sentinel)
	}
	if n1 != 0 {
		t.Errorf("Wait() (first) n = %d, want 0", n1)
	}
	n2, err2 := res2.Wait(t.Context())
	if !errors.Is(err2, sentinel) {
		t.Errorf("Wait() (second) error = %v, want %v", err2, sentinel)
	}
	if n2 != 0 {
		t.Errorf("Wait() (second) n = %d, want 0", n2)
	}
}

// TestLoadJobsCloseReportsAFailureNobodyWaitedFor checks that a batch which
// fails without any caller ever waiting on its WriteResult is not lost: Close
// reports it, naming how many rows did not land. This is the core guarantee
// that rows are never dropped silently.
func TestLoadJobsCloseReportsAFailureNobodyWaitedFor(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("load refused")
	loader := &fakeLoader{errs: []error{sentinel}}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), retryPolicy: noRetryPolicy, logger: discardLogger(), flushRows: 2}

	rows := []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(0)}),
		rowOf("r2", map[string]bigquery.Value{"A": "x", "B": int64(1)}),
	}
	if _, err := w.WriteRows(t.Context(), rows); err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	err := w.Close(t.Context())
	if err == nil {
		t.Fatal("Close() error = nil, want the failure nobody waited for")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("Close() error = %v, want it to wrap %v", err, sentinel)
	}
	if !strings.Contains(err.Error(), "2 row") {
		t.Errorf("Close() error = %q, want it to name the 2 rows that did not land", err.Error())
	}
}

// TestLoadJobsConcurrentWriteRowsShareBatchesSafely checks, under -race, that
// concurrent WriteRows calls claim and settle a shared batch correctly: every
// call gets back exactly the rows it sent, and the rows of every call land in
// some load job even though several goroutines are filling batches at once.
//
// None of the writing goroutines waits on its WriteResult, so a batch can only
// close by reaching FlushRows; that is what makes the count of load jobs below
// exact instead of depending on how the goroutines happen to interleave.
func TestLoadJobsConcurrentWriteRowsShareBatchesSafely(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger(), flushRows: 2}

	const calls = 50
	results := make([]WriteResult, calls)
	writeErrs := make([]error, calls)

	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], writeErrs[i] = w.WriteRows(t.Context(), []Row{
				rowOf(fmt.Sprintf("r%d", i), map[string]bigquery.Value{"A": "x", "B": int64(i)}),
			})
		}(i)
	}
	wg.Wait()
	for i, err := range writeErrs {
		if err != nil {
			t.Fatalf("WriteRows() (call %d) error = %v", i, err)
		}
	}

	jobs := loader.snapshot()
	if len(jobs) != calls/2 {
		t.Fatalf("load was called %d times, want %d (batches of 2)", len(jobs), calls/2)
	}
	rowsSeen := 0
	for _, c := range jobs {
		rowsSeen += len(strings.Split(strings.TrimSuffix(c.rows, "\n"), "\n"))
	}
	if rowsSeen != calls {
		t.Errorf("the load jobs carried %d rows in total, want %d", rowsSeen, calls)
	}

	ns := make([]int, calls)
	waitErrs := make([]error, calls)
	for i := range calls {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ns[i], waitErrs[i] = results[i].Wait(t.Context())
		}(i)
	}
	wg.Wait()

	total := 0
	for i := range calls {
		if waitErrs[i] != nil {
			t.Fatalf("Wait() (call %d) error = %v", i, waitErrs[i])
		}
		if ns[i] != 1 {
			t.Errorf("Wait() (call %d) n = %d, want 1", i, ns[i])
		}
		total += ns[i]
	}
	if total != calls {
		t.Errorf("total rows landed = %d, want %d", total, calls)
	}
	if got := len(loader.snapshot()); got != calls/2 {
		t.Errorf("load was called %d times after Wait, want still %d (no batch sent twice)", got, calls/2)
	}
}

func TestLoadJobsWriteRowsRejectsARowTheSchemaDoesNotCover(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{}
	w := &LoadJobsWriter{loader: loader, schema: abSchema(), logger: discardLogger()}

	res, err := w.WriteRows(t.Context(), []Row{
		rowOf("r1", map[string]bigquery.Value{"A": "x", "B": int64(1), "C": "extra"}),
	})
	if err != nil {
		t.Fatalf("WriteRows() error = %v", err)
	}
	n, err := res.Wait(t.Context())
	if err == nil {
		t.Fatal("Wait() error = nil, want a rejection of the unknown column")
	}
	if n != 0 {
		t.Errorf("Wait() n = %d, want 0", n)
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
	w, err := (&LoadJobs{Staging: stager}).NewWriter(testClient(t), testRelation())
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	if _, ok := w.loader.(stagedLoader); !ok {
		t.Errorf("loader = %T, want a stagedLoader once Staging is set", w.loader)
	}
}

func TestLoadJobsRejectsAnInvalidStager(t *testing.T) {
	t.Parallel()

	stager := &fakeStager{validate: errors.New("no bucket")}
	if _, err := (&LoadJobs{Staging: stager}).NewWriter(testClient(t), testRelation()); err == nil {
		t.Fatal("NewWriter() error = nil, want the stager's own rejection")
	}
}

// TestLoadJobsBindSchemaGuards covers what BindSchema turns down. Nothing can be
// written before a schema arrives, and a second one would leave the rows already
// written described by something else.
func TestLoadJobsBindSchemaGuards(t *testing.T) {
	t.Parallel()

	w := &LoadJobsWriter{loader: &fakeLoader{}, logger: slog.New(slog.DiscardHandler)}
	if err := w.BindSchema(t.Context(), nil); err == nil {
		t.Error("BindSchema(nil) error = nil, want an empty schema to be refused")
	}
	if err := w.BindSchema(t.Context(), abSchema()); err != nil {
		t.Fatalf("BindSchema() error = %v", err)
	}
	if err := w.BindSchema(t.Context(), abSchema()); err == nil {
		t.Error("BindSchema() error = nil on the second call, want a second schema to be refused")
	}
}

// TestLoadJobsWriteRowsBeforeBindSchema checks that rows are turned down rather
// than written against a schema the writer does not have yet.
func TestLoadJobsWriteRowsBeforeBindSchema(t *testing.T) {
	t.Parallel()

	w := &LoadJobsWriter{loader: &fakeLoader{}, logger: slog.New(slog.DiscardHandler)}
	if _, err := w.WriteRows(t.Context(), []Row{{ID: "1", Values: map[string]bigquery.Value{"A": "a"}}}); err == nil {
		t.Error("WriteRows() error = nil before BindSchema, want the rows to be refused")
	}
}

// TestLoadJobsCloseReportsAFailureOnlySomeCallersWaitedFor checks that one caller
// asking what became of a batch does not answer for another. Three calls share the
// batch, one waits, and Close still owes the other two an answer.
func TestLoadJobsCloseReportsAFailureOnlySomeCallersWaitedFor(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("the load job failed")
	loader := &fakeLoader{errs: []error{sentinel}}
	w := &LoadJobsWriter{
		loader:    loader,
		schema:    abSchema(),
		flushRows: 3,
		logger:    slog.New(slog.DiscardHandler),
	}
	ctx := t.Context()
	var results []WriteResult
	for i := range 3 {
		res, err := w.WriteRows(ctx, []Row{{ID: string(rune('a' + i)), Values: map[string]bigquery.Value{"A": "a", "B": int64(i)}}})
		if err != nil {
			t.Fatalf("WriteRows() error = %v", err)
		}
		results = append(results, res)
	}
	if _, err := results[0].Wait(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("Wait() error = %v, want the load failure", err)
	}
	err := w.Close(ctx)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Close() error = %v, want the failure the other two callers were never told", err)
	}
}

// TestLoadJobsCloseWaitsForAJobAnotherCallerStarted checks that closing while a
// job is still running does not return before its outcome is known: a failure
// recorded after Close had already looked would reach nobody.
func TestLoadJobsCloseWaitsForAJobAnotherCallerStarted(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("the load job failed")
	release := make(chan struct{})
	loader := &blockingLoader{release: release, err: sentinel}
	w := &LoadJobsWriter{
		loader:    loader,
		schema:    abSchema(),
		flushRows: 1,
		logger:    slog.New(slog.DiscardHandler),
	}
	ctx := t.Context()
	go func() {
		if _, err := w.WriteRows(ctx, []Row{{ID: "1", Values: map[string]bigquery.Value{"A": "a", "B": int64(1)}}}); err != nil {
			t.Errorf("WriteRows() error = %v", err)
		}
	}()
	<-loader.entered()
	close(release)
	if err := w.Close(ctx); !errors.Is(err, sentinel) {
		t.Fatalf("Close() error = %v, want the failure of the job it had to wait for", err)
	}
}

// blockingLoader holds a load until it is released, so that a test can close a
// writer while a job is in flight.
type blockingLoader struct {
	release chan struct{}
	err     error

	once sync.Once
	in   chan struct{}
}

func (l *blockingLoader) entered() chan struct{} {
	l.once.Do(func() { l.in = make(chan struct{}) })
	return l.in
}

func (l *blockingLoader) load(context.Context, []byte, bigquery.Schema, *slog.Logger) error {
	close(l.entered())
	<-l.release
	return l.err
}
