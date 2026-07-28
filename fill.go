package bqsink

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// AppendInfo carries what a row cannot work out for itself when it fills its own
// columns in.
//
// Values a row can produce on its own are deliberately absent. FillRow runs once
// per row, before the conversion and before any retry, so a row that wants a
// timestamp or an identifier of its own design can simply make one there and it
// will not drift across retries.
type AppendInfo struct {
	// Relation names the destination table.
	Relation Relation

	// SinkerID identifies the Sinker. It is decided in New, so it spans the
	// Sinker's whole life: a batch that builds one Sinker per run gets an id per
	// run, while a long lived process that keeps one around gets a single id until
	// it is replaced.
	//
	// It is a version 7 UUID, so it sorts in the order the Sinkers were built.
	SinkerID string

	// SinkerCreatedAt is when New was called.
	SinkerCreatedAt time.Time

	// RowID identifies this row. It is a version 7 UUID, so it sorts in the order
	// the rows were handed over, which suits a clustering key.
	//
	// A retry of the same row keeps the same value, so it can deduplicate what
	// at-least-once delivery may write twice.
	RowID string

	// Time is when Sink was called for this row. A retry keeps the same value.
	Time time.Time
}

// RowFiller lets a row type fill in values just before it is written, which is how
// columns such as a write timestamp or a row id get theirs.
//
// FillRow is called once per row, on a copy, before the conversion and before any
// retry. Two things follow from that. The value the caller passed to Sink is left
// untouched, unless T is itself a pointer type, in which case filling reaches the
// caller's own value. And a retried row carries the values it was first given, so
// RowID can be used to deduplicate.
//
// It needs a pointer receiver: with a value receiver it would write into a copy
// that is then discarded, and New rejects that rather than letting the columns stay
// empty.
//
// Embedding is the point, since a promoted method makes the outer row satisfy the
// interface. IngestionMetadata is a ready-made set of columns; a type of your own works
// the same way when the column names or the values need to differ.
//
//	type AccessLog struct {
//		bqsink.IngestionMetadata
//		UserID string `bqsink:"user_id"`
//	}
type RowFiller interface {
	FillRow(ctx context.Context, info AppendInfo) error
}

// IngestionMetadata is an embeddable set of columns describing how a row was written,
// which bqsink fills in.
//
// The columns are named with a leading underscore so that they sort ahead of the
// business columns and read as belonging to the pipeline rather than the data.
// BigQuery allows a column name to start with an underscore; its own pseudo columns
// such as _PARTITIONTIME are upper case, so these do not collide.
//
// Embed it to get the three columns below; write a type of your own implementing
// RowFiller when the names or the values need to differ.
//
//	type AccessLog struct {
//		bqsink.IngestionMetadata
//		UserID string `bqsink:"user_id"`
//	}
type IngestionMetadata struct {
	// IngestionAt is when Sink was called for the row, which a batching transport
	// defers from when the row reaches BigQuery.
	IngestionAt time.Time `bqsink:"_ingestion_at"`

	// IngestionID identifies the ingestion the row belongs to, which is one
	// Sinker's lifetime. It comes from AppendInfo.SinkerID and is decided in New,
	// so building a Sinker per batch gives an id per batch, while a long lived
	// process holding one Sinker writes every row under the same id.
	//
	// It does not change from one Sink to the next; _ingestion_at and
	// _ingestion_row_id do.
	IngestionID string `bqsink:"_ingestion_id"`

	// IngestionRowID identifies the row and stays the same across a retry, so it
	// can deduplicate rows written more than once.
	IngestionRowID string `bqsink:"_ingestion_row_id"`
}

// FillRow implements RowFiller.
func (m *IngestionMetadata) FillRow(_ context.Context, info AppendInfo) error {
	m.IngestionAt = info.Time
	m.IngestionID = info.SinkerID
	m.IngestionRowID = info.RowID
	return nil
}

// newID returns a version 7 UUID, whose value orders by the time it was made.
func newID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate an identifier: %w", err)
	}
	return id.String(), nil
}

// fillerFunc reaches the RowFiller of a value of type T.
type fillerFunc[T any] func(*T) RowFiller

// rowFillerOf works out how a value of T reaches its RowFiller, returning nil when
// T does not implement it.
//
// It fails when T implements the interface with a value receiver, since filling a
// copy that is then discarded leaves the columns empty with nothing to show why.
func rowFillerOf[T any]() (fillerFunc[T], error) {
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Pointer {
		if _, ok := reflect.New(t.Elem()).Interface().(RowFiller); !ok {
			return nil, nil
		}
		// T is already a pointer, so the value the caller passed is the filler.
		return func(v *T) RowFiller {
			filler, _ := any(*v).(RowFiller)
			return filler
		}, nil
	}
	if _, ok := reflect.New(t).Interface().(RowFiller); !ok {
		return nil, nil
	}
	var zero T
	if _, ok := any(zero).(RowFiller); ok {
		return nil, fmt.Errorf(
			"bqsink: %s implements RowFiller with a value receiver, which would fill a copy that is then discarded; give FillRow a pointer receiver",
			t)
	}
	return func(v *T) RowFiller {
		filler, _ := any(v).(RowFiller)
		return filler
	}, nil
}
