package bqsink

import (
	"context"
	"errors"
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
	writer := &fakeRowWriter{}
	s := newTestSinker[metadataRow](t, fake, WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

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

	s, err := New[metadataRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	id, err := uuid.Parse(s.sinkerID)
	if err != nil {
		t.Fatalf("the Sinker's id %q is not a UUID: %v", s.sinkerID, err)
	}
	if got := id.Version(); got != 7 {
		t.Errorf("the Sinker's id is version %d, want 7 so that ids sort by time", got)
	}

	row, err := s.prepare(t.Context(), metadataRow{})
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

	s, err := New[metadataRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var previous string
	for i := range 50 {
		row, err := s.prepare(t.Context(), metadataRow{})
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
	writer := &fakeRowWriter{}
	s := newTestSinker[metadataRow](t, fake, WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

	_, err := s.Sink(t.Context(),
		metadataRow{UserID: "u1"},
		metadataRow{UserID: "u2"},
		metadataRow{UserID: "u3"},
	)
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
	writer := &fakeRowWriter{}
	s := newTestSinker[customFillRow](t, fake, WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

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

	_, err := New[valueReceiverRow](testClient(t), testRelation())
	if err == nil {
		t.Fatal("New() error = nil, want a rejection of the value receiver")
	}
	if !strings.Contains(err.Error(), "pointer receiver") {
		t.Errorf("New() error = %v, want it to say a pointer receiver is needed", err)
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
	writer := &fakeRowWriter{}
	s := newTestSinker[*customFillRow](t, fake, WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

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
	writer := &fakeRowWriter{}
	s := newTestSinker[failingFillRow](t, fake, WithWriteStrategy(&fakeWriteStrategy{writer: writer}))

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

func TestRowWithoutAFillerIsUntouched(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.filler != nil {
		t.Error("filler is set for a type that does not implement RowFiller")
	}
}

// TestPrepareAlwaysGeneratesARowID checks that Row.ID is set even for a type
// that does not implement RowFiller, since it is what a transport names the row
// by and what _ingestion_row_id gets when the row type does fill it in.
func TestPrepareAlwaysGeneratesARowID(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.prepare(t.Context(), simpleRow{Name: "a"})
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

	s, err := New[metadataRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.prepare(t.Context(), metadataRow{UserID: "u1"})
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
