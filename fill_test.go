package bqsink

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
)

// metadataRow embeds the ready-made columns.
type metadataRow struct {
	IngestionMetadata
	UserID string `bqsink:"user_id"`
}

// customFillRow fills columns of its own naming.
type customFillRow struct {
	Table   string `bqsink:"table"`
	Sinker  string `bqsink:"sinker"`
	Started string `bqsink:"started"`
	UserID  string `bqsink:"user_id"`
}

func (r *customFillRow) FillRow(_ context.Context, info AppendInfo) error {
	r.Table = info.Relation.String()
	r.Sinker = info.SinkerID
	r.Started = info.SinkerCreatedAt.Format(time.RFC3339)
	return nil
}

// valueReceiverRow implements RowFiller the wrong way round: filling a copy that is
// then discarded.
type valueReceiverRow struct {
	Filled string `bqsink:"filled"`
}

func (r valueReceiverRow) FillRow(_ context.Context, info AppendInfo) error {
	r.Filled = info.RowID
	return nil
}

// failingFillRow reports a failure from FillRow.
type failingFillRow struct {
	Name string `bqsink:"name"`
}

var errFillFailed = errors.New("cannot fill")

func (r *failingFillRow) FillRow(_ context.Context, info AppendInfo) error {
	return errFillFailed
}

func TestIngestionMetadataColumns(t *testing.T) {
	t.Parallel()

	schema, err := inferSchema[metadataRow]()
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	}
	if !schemasEqual(schema, want) {
		t.Errorf("Schema() = %s, want %s", formatSchema(schema), formatSchema(want))
	}
}

func schemasEqual(got, want bigquery.Schema) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Name != want[i].Name || got[i].Type != want[i].Type {
			return false
		}
	}
	return true
}

func TestIngestionMetadataIsFilledIn(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[metadataRow](t, fake, writer)

	before := time.Now()
	if _, err := s.Sink(t.Context(), metadataRow{UserID: "u1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	rows := appendedRows(writer)
	if len(rows) != 1 {
		t.Fatalf("%d row(s) were appended, want 1", len(rows))
	}
	row := rows[0]

	if got := row["user_id"]; got != "u1" {
		t.Errorf("user_id = %#v, want u1", got)
	}
	at, ok := row["_ingestion_at"].(time.Time)
	if !ok {
		t.Fatalf("_ingestion_at = %#v, want a time.Time", row["_ingestion_at"])
	}
	if at.Before(before) || at.After(time.Now()) {
		t.Errorf("_ingestion_at = %v, want a time from during the call", at)
	}
	jobID, ok := row["_ingestion_id"].(string)
	if !ok || jobID != s.sinkerID {
		t.Errorf("_ingestion_id = %#v, want the Sinker's id %q", row["_ingestion_id"], s.sinkerID)
	}
	rowID, ok := row["_ingestion_row_id"].(string)
	if !ok {
		t.Fatalf("_ingestion_row_id = %#v, want a string", row["_ingestion_row_id"])
	}
	if _, err := uuid.Parse(rowID); err != nil {
		t.Errorf("_ingestion_row_id = %q, want a UUID: %v", rowID, err)
	}
}

func TestIDsAreVersion7(t *testing.T) {
	t.Parallel()

	s, err := testSinker[metadataRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	id, err := uuid.Parse(s.sinkerID)
	if err != nil {
		t.Fatalf("the Sinker's id %q is not a UUID: %v", s.sinkerID, err)
	}
	if got := id.Version(); got != 7 {
		t.Errorf("the Sinker's id is version %d, want 7 so that ids sort by time", got)
	}

	row, err := s.prepare(t.Context(), reflect.ValueOf(metadataRow{}))
	if err != nil {
		t.Fatalf("prepare() error = %v", err)
	}
	rowID, err := uuid.Parse(row.ID)
	if err != nil {
		t.Fatalf("the row id %q is not a UUID: %v", row.ID, err)
	}
	if got := rowID.Version(); got != 7 {
		t.Errorf("the row id is version %d, want 7", got)
	}
}

// TestRowIDsSortByTime is what version 7 buys: ids handed out later compare higher,
// which is what makes them usable as a clustering key.
func TestRowIDsSortByTime(t *testing.T) {
	t.Parallel()

	s, err := testSinker[metadataRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	var previous string
	for i := range 50 {
		row, err := s.prepare(t.Context(), reflect.ValueOf(metadataRow{}))
		if err != nil {
			t.Fatalf("prepare() error = %v", err)
		}
		if previous != "" && row.ID <= previous {
			t.Fatalf("row id %d (%q) does not sort after %q", i, row.ID, previous)
		}
		previous = row.ID
	}
}

func TestEachRowGetsItsOwnID(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[metadataRow](t, fake, writer)

	_, err := s.Sink(t.Context(), []metadataRow{
		{UserID: "u1"},
		{UserID: "u2"},
		{UserID: "u3"},
	})
	if err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	rows := appendedRows(writer)
	if len(rows) != 3 {
		t.Fatalf("%d row(s) were appended, want 3", len(rows))
	}
	seen := map[string]bool{}
	for i, row := range rows {
		id, ok := row["_ingestion_row_id"].(string)
		if !ok {
			t.Fatalf("row %d has no _ingestion_row_id", i)
		}
		if seen[id] {
			t.Errorf("row %d reuses the id %q", i, id)
		}
		seen[id] = true
		if got := row["_ingestion_id"]; got != s.sinkerID {
			t.Errorf("row %d sinker id = %#v, want them all to share the Sinker's id", i, got)
		}
	}
}

func TestCustomFillRow(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "table", Type: bigquery.StringFieldType},
		{Name: "sinker", Type: bigquery.StringFieldType},
		{Name: "started", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[customFillRow](t, fake, writer)

	if _, err := s.Sink(t.Context(), customFillRow{UserID: "u1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	rows := appendedRows(writer)
	if len(rows) != 1 {
		t.Fatalf("%d row(s) were appended, want 1", len(rows))
	}
	if got, want := rows[0]["table"], "test-project.test_dataset.test_table"; got != want {
		t.Errorf("table = %#v, want %q", got, want)
	}
	if got := rows[0]["sinker"]; got != s.sinkerID {
		t.Errorf("sinker = %#v, want the Sinker's id", got)
	}
	if got, ok := rows[0]["started"].(string); !ok || got == "" {
		t.Errorf("started = %#v, want the Sinker's creation time", rows[0]["started"])
	}
}

func TestValueReceiverFillRowIsRejected(t *testing.T) {
	t.Parallel()

	_, err := testSinker[valueReceiverRow](t)
	if err == nil {
		t.Fatal("NewSinker() error = nil, want a rejection of the value receiver")
	}
	if !strings.Contains(err.Error(), "pointer receiver") {
		t.Errorf("NewSinker() error = %v, want it to say a pointer receiver is needed", err)
	}
}

// TestUnpromotedFillRowIsRejected checks the other shape checkRowFiller turns down:
// a type built at run time with reflect.StructOf. Go promotes an embedded type's
// value receiver methods to such a type but not a pointer receiver's, so embedding
// IngestionMetadata this way would otherwise leave its columns silently empty.
func TestUnpromotedFillRowIsRejected(t *testing.T) {
	t.Parallel()

	rt := reflect.StructOf([]reflect.StructField{
		{Name: "IngestionMetadata", Type: reflect.TypeFor[IngestionMetadata](), Anonymous: true},
		{Name: "UserID", Type: reflect.TypeFor[string](), Tag: `bqsink:"user_id"`},
	})

	_, err := NewSinker(newFakeWriter(t), DeclarationForType(rt))
	if err == nil {
		t.Fatal("NewSinker() error = nil, want a rejection of the unpromoted FillRow")
	}
	if !strings.Contains(err.Error(), "does not promote its FillRow") {
		t.Errorf("NewSinker() error = %v, want it to name the unpromoted FillRow", err)
	}
}

func TestPointerTypeParameterCanFill(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "table", Type: bigquery.StringFieldType},
		{Name: "sinker", Type: bigquery.StringFieldType},
		{Name: "started", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[*customFillRow](t, fake, writer)

	row := &customFillRow{UserID: "u1"}
	if _, err := s.Sink(t.Context(), row); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	// With a pointer type parameter the fill reaches the caller's own value, which
	// the GoDoc on RowFiller says.
	if row.Sinker != s.sinkerID {
		t.Errorf("the caller's row was not filled: Sinker = %q, want %q", row.Sinker, s.sinkerID)
	}
}

func TestFillRowFailureStopsTheWrite(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{{Name: "name", Type: bigquery.StringFieldType}})
	writer := newFakeWriter(t)
	s := newTestSinker[failingFillRow](t, fake, writer)

	n, err := s.Sink(t.Context(), failingFillRow{})
	if !errors.Is(err, errFillFailed) {
		t.Errorf("Sink() error = %v, want the fill failure", err)
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0; a row that failed to prepare must not count as written", n)
	}
	if rows := appendedRows(writer); len(rows) != 0 {
		t.Errorf("%d row(s) were appended, want 0", len(rows))
	}
}

// TestRowWithoutAFillerIsUntouched checks that a row type not implementing
// RowFiller is written exactly as given: prepare has nothing of its own to add.
func TestRowWithoutAFillerIsUntouched(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "Name", Type: bigquery.StringFieldType},
		{Name: "Count", Type: bigquery.IntegerFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[simpleRow](t, fake, writer)

	if _, err := s.Sink(t.Context(), simpleRow{Name: "a", Count: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	rows := appendedRows(writer)
	if len(rows) != 1 {
		t.Fatalf("%d row(s) were appended, want 1", len(rows))
	}
	want := map[string]bigquery.Value{"Name": "a", "Count": int64(1)}
	if !reflect.DeepEqual(rows[0], want) {
		t.Errorf("row = %#v, want %#v: a type without RowFiller must be written unchanged", rows[0], want)
	}
}

// TestPrepareAlwaysGeneratesARowID checks that Row.ID is set even for a type
// that does not implement RowFiller, since it is what a transport names the row
// by and what _ingestion_row_id gets when the row type does fill it in.
func TestPrepareAlwaysGeneratesARowID(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	row, err := s.prepare(t.Context(), reflect.ValueOf(simpleRow{Name: "a"}))
	if err != nil {
		t.Fatalf("prepare() error = %v", err)
	}
	if _, err := uuid.Parse(row.ID); err != nil {
		t.Errorf("Row.ID = %q is not a UUID: %v", row.ID, err)
	}
}

// TestRowIDMatchesTheIngestionRowIDColumn checks the other half of Row.ID's
// contract: a transport naming a row by it, as describeRowErrors does, is naming
// the same row the _ingestion_row_id column identifies.
func TestRowIDMatchesTheIngestionRowIDColumn(t *testing.T) {
	t.Parallel()

	s, err := testSinker[metadataRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	row, err := s.prepare(t.Context(), reflect.ValueOf(metadataRow{UserID: "u1"}))
	if err != nil {
		t.Fatalf("prepare() error = %v", err)
	}
	column, ok := row.Values["_ingestion_row_id"].(string)
	if !ok {
		t.Fatalf("_ingestion_row_id = %#v, want a string", row.Values["_ingestion_row_id"])
	}
	if column != row.ID {
		t.Errorf("_ingestion_row_id = %q, want Row.ID %q", column, row.ID)
	}
}

// TestSinkDoesNotMutateTheCallersValueTypeRow checks that a row handed over by
// value is filled on a copy, so that the caller's own value is left alone. The
// pointer case is the documented exception and is covered by the GoDoc contract
// rather than here, since filling through a pointer is what reaches the caller.
func TestSinkDoesNotMutateTheCallersValueTypeRow(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "user_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[metadataRow](t, fake, writer)

	row := metadataRow{UserID: "u1"}
	if _, err := s.Sink(t.Context(), row); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if row.IngestionRowID != "" || row.IngestionID != "" || !row.IngestionAt.IsZero() {
		t.Errorf("the caller's row was filled in: %+v, want it untouched", row.IngestionMetadata)
	}
	rows := appendedRows(writer)
	if len(rows) != 1 {
		t.Fatalf("%d row(s) were appended, want 1", len(rows))
	}
	if rows[0]["_ingestion_row_id"] == "" {
		t.Error("_ingestion_row_id is empty in the row written, want the copy to have been filled")
	}
}

// TestSinkRejectsATypedNilRow checks that a nil pointer is refused rather than
// reaching FillRow on a nil receiver.
func TestSinkRejectsATypedNilRow(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "user_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
	})
	writer := newFakeWriter(t)
	s := newTestSinker[*metadataRow](t, fake, writer)

	n, err := s.Sink(t.Context(), []*metadataRow{nil})
	if err == nil {
		t.Fatal("Sink() error = nil, want a nil row to be refused")
	}
	if n != 0 {
		t.Errorf("Sink() n = %d, want 0", n)
	}
	if rows := appendedRows(writer); len(rows) != 0 {
		t.Errorf("%d row(s) were appended, want 0", len(rows))
	}
}

// TestRetryKeepsTheSameRowID checks the contract that makes _ingestion_row_id
// usable for deduplication: FillRow runs once per row, before the conversion and
// before any retry, so a row resent after a transient failure carries the same id.
func TestRetryKeepsTheSameRowID(t *testing.T) {
	t.Parallel()

	loader := &fakeLoader{errs: []error{unavailableErr(), nil}}
	w, err := (&LoadJobs{RetryPolicy: fastRetryPolicy}).NewWriter(testClient(t), testRelation())
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	w.loader = loader
	fake := migratedTableFor(bigquery.Schema{
		{Name: "user_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
	})
	s := newTestSinker[metadataRow](t, fake, w)

	if _, err := s.Sink(t.Context(), metadataRow{UserID: "u1"}); err != nil {
		t.Fatalf("Sink() error = %v, want the retry to get through", err)
	}
	calls := loader.snapshot()
	if len(calls) != 2 {
		t.Fatalf("load was called %d times, want 2 (one failure then a success)", len(calls))
	}
	if calls[0].rows != calls[1].rows {
		t.Errorf("the retried load carried different rows, so a retry would look like another row:\nfirst:  %q\nsecond: %q",
			calls[0].rows, calls[1].rows)
	}
	if !strings.Contains(calls[1].rows, "_ingestion_row_id") {
		t.Errorf("the rows loaded carry no _ingestion_row_id: %q", calls[1].rows)
	}
}

// TestUnpromotedFillRowIsRejectedForAPointerRow covers the same silent miss as
// TestUnpromotedFillRowIsRejected for a row declared as a pointer. A type built at
// run time promotes nothing to its pointer either, so FillRow would never run.
func TestUnpromotedFillRowIsRejectedForAPointerRow(t *testing.T) {
	t.Parallel()

	rt := reflect.StructOf([]reflect.StructField{
		{Name: "IngestionMetadata", Type: reflect.TypeOf(IngestionMetadata{}), Anonymous: true},
		{Name: "Name", Type: reflect.TypeOf(""), Tag: `bqsink:"name"`},
	})
	_, err := NewSinker(newFakeWriter(t), DeclarationForType(reflect.PointerTo(rt)))
	if err == nil {
		t.Fatal("NewSinker() error = nil, want a row whose FillRow is not promoted to be refused")
	}
	if !strings.Contains(err.Error(), "does not promote its FillRow") {
		t.Errorf("NewSinker() error = %v, want it to say why FillRow would never run", err)
	}
}

// TestPointerRowThatFillsItselfIsAccepted guards the check above from turning down
// an ordinary pointer row, whose embedded FillRow is promoted as usual.
func TestPointerRowThatFillsItselfIsAccepted(t *testing.T) {
	t.Parallel()

	if _, err := NewSinker(newFakeWriter(t), DeclarationOf[*metadataRow]()); err != nil {
		t.Fatalf("NewSinker() error = %v, want a pointer row with a promoted FillRow to be accepted", err)
	}
}
