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

	schema, err := InferSchema[metadataRow]()
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
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
	if err := s.Sink(t.Context(), metadataRow{UserID: "u1"}); err != nil {
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

	info, err := s.appendInfo()
	if err != nil {
		t.Fatalf("appendInfo() error = %v", err)
	}
	rowID, err := uuid.Parse(info.RowID)
	if err != nil {
		t.Fatalf("the row id %q is not a UUID: %v", info.RowID, err)
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
		info, err := s.appendInfo()
		if err != nil {
			t.Fatalf("appendInfo() error = %v", err)
		}
		if previous != "" && info.RowID <= previous {
			t.Fatalf("row id %d (%q) does not sort after %q", i, info.RowID, previous)
		}
		previous = info.RowID
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

	err := s.SinkAll(t.Context(),
		metadataRow{UserID: "u1"},
		metadataRow{UserID: "u2"},
		metadataRow{UserID: "u3"},
	)
	if err != nil {
		t.Fatalf("SinkAll() error = %v", err)
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

// TestRetryKeepsTheSameRowID is what makes _ingestion_row_id usable for
// deduplication: at-least-once delivery must write the same id twice, not two.
func TestRetryKeepsTheSameRowID(t *testing.T) {
	t.Parallel()

	fake := migratedTableFor(bigquery.Schema{
		{Name: "_ingestion_at", Type: bigquery.TimestampFieldType},
		{Name: "_ingestion_id", Type: bigquery.StringFieldType},
		{Name: "_ingestion_row_id", Type: bigquery.StringFieldType},
		{Name: "user_id", Type: bigquery.StringFieldType},
	})
	writer := &recordingFlakyWriter{failAppends: 2, appendErr: unavailableErr()}
	s := newTestSinker[metadataRow](t, fake, WithWriteStrategy(&recordingFlakyStrategy{writer: writer}))

	if err := s.Sink(t.Context(), metadataRow{UserID: "u1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	seen := writer.seenRows()
	if len(seen) != 3 {
		t.Fatalf("Append was called %d times, want 3 (two failures then a success)", len(seen))
	}
	first, ok := seen[0]["_ingestion_row_id"].(string)
	if !ok {
		t.Fatalf("the first attempt has no _ingestion_row_id")
	}
	for i, row := range seen[1:] {
		if got := row["_ingestion_row_id"]; got != first {
			t.Errorf("attempt %d used the id %#v, want the same %q so duplicates can be spotted", i+2, got, first)
		}
	}
	if seen[0]["_ingestion_at"] != seen[2]["_ingestion_at"] {
		t.Error("_ingestion_at changed between attempts, want it fixed by the first")
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

	if err := s.Sink(t.Context(), customFillRow{UserID: "u1"}); err != nil {
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
	if err := s.Sink(t.Context(), row); err != nil {
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

	if err := s.Sink(t.Context(), failingFillRow{}); !errors.Is(err, errFillFailed) {
		t.Errorf("Sink() error = %v, want the fill failure", err)
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
