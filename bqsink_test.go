package bqsink

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

func testClient(t *testing.T) *bigquery.Client {
	t.Helper()
	client, err := bigquery.NewClient(context.Background(), "test-project", option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("bigquery.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client.Close() error = %v", err)
		}
	})
	return client
}

func testRelation() Relation {
	return Relation{DatasetID: "test_dataset", TableID: "test_table"}
}

type definedRow struct {
	Name string
}

func (definedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Labels: map[string]string{"receiver": "value"}}
}

type ptrDefinedRow struct {
	Name string
}

func (*ptrDefinedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Labels: map[string]string{"receiver": "pointer"}}
}

type schemaDefinedRow struct {
	Amount string `bqsink:"explicit"`
}

func (schemaDefinedRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "explicit", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
		},
	}
}

// testSinker builds a Sinker over a fake writer, for a test that only looks at
// what the declaration settled.
func testSinker[T any](t *testing.T, opts ...Option) (*Sinker, error) {
	t.Helper()
	return NewSinker(newFakeWriter(t), DeclarationOf[T](), opts...)
}

// metadataOf builds a Sinker for T and returns what its declaration settled on
// BigQueryTableMetadata.
func metadataOf[T any](t *testing.T) (*bigquery.TableMetadata, error) {
	t.Helper()
	s, err := testSinker[T](t)
	if err != nil {
		return nil, err
	}
	return s.metadata, nil
}

func TestNewSinkerRejectsNilWriter(t *testing.T) {
	t.Parallel()

	if _, err := NewSinker(nil, DeclarationOf[simpleRow]()); err == nil {
		t.Fatal("NewSinker() with a nil writer should fail")
	}
}

func TestNewSinkerRejectsEmptyDeclaration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		decl Declaration
	}{
		{name: "a zero value Declaration", decl: Declaration{}},
		{name: "DeclarationForType(nil)", decl: DeclarationForType(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewSinker(newFakeWriter(t), tt.decl); err == nil {
				t.Fatalf("NewSinker() with %s should fail", tt.name)
			}
		})
	}
}

// TestNewSinkerRejectsAWriterWithAnInvalidRelation checks the relation
// validation New used to run on its own relation argument: the relation now
// comes from the writer, so NewSinker validates what it reports instead.
func TestNewSinkerRejectsAWriterWithAnInvalidRelation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		relation Relation
	}{
		{name: "no dataset", relation: Relation{TableID: "t"}},
		{name: "no table", relation: Relation{DatasetID: "d"}},
		{name: "neither", relation: Relation{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newFakeWriter(t)
			w.relation = tt.relation
			if _, err := NewSinker(w, DeclarationOf[simpleRow]()); err == nil {
				t.Fatalf("NewSinker() with relation %+v should fail", tt.relation)
			}
		})
	}
}

// TestNewSinkerRequiresMigrationNoneWithoutAClient checks the safeguard around
// a writer that reports no BigQuery client: the table can never be read
// through it, so every strategy but MigrationNone is refused rather than
// silently skipping reconciliation.
func TestNewSinkerRequiresMigrationNoneWithoutAClient(t *testing.T) {
	t.Parallel()

	t.Run("the default strategy fails without a client", func(t *testing.T) {
		t.Parallel()
		w := newFakeWriter(t)
		w.client = nil
		if _, err := NewSinker(w, DeclarationOf[simpleRow]()); err == nil {
			t.Fatal("NewSinker() with a client-less writer and the default strategy should fail")
		}
	})

	t.Run("MigrationNone allows a client-less writer", func(t *testing.T) {
		t.Parallel()
		w := newFakeWriter(t)
		w.client = nil
		s, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(MigrationNone{}, nil))
		if err != nil {
			t.Fatalf("NewSinker() error = %v, want MigrationNone to be allowed without a client", err)
		}
		if s.api != nil {
			t.Errorf("api = %v, want nil since the writer has no client to reconcile through", s.api)
		}
	})
}

// TestDeclarationForTypeDerivesSchemaFromARuntimeType checks that
// DeclarationForType works from a reflect.Type settled only at run time,
// rather than one only DeclarationOf's type parameter can name.
func TestDeclarationForTypeDerivesSchemaFromARuntimeType(t *testing.T) {
	t.Parallel()

	rt := reflect.StructOf([]reflect.StructField{
		{Name: "Name", Type: reflect.TypeFor[string](), Tag: `bqsink:"name"`},
		{Name: "Count", Type: reflect.TypeFor[int64](), Tag: `bqsink:"count"`},
	})

	s, err := NewSinker(newFakeWriter(t), DeclarationForType(rt))
	if err != nil {
		t.Fatalf("NewSinker() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "count", Type: bigquery.IntegerFieldType},
	}
	if !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want %s", formatSchema(s.schema), formatSchema(want))
	}
}

// TestSinkRejectsATypeThatDoesNotMatchTheDeclaration checks that the type the
// declaration named at NewSinker time holds for every Sink call: the
// declaration is settled at construction now, so there is no first Sink to
// settle on a type instead.
func TestSinkRejectsATypeThatDoesNotMatchTheDeclaration(t *testing.T) {
	t.Parallel()

	s, err := testSinker[nestedRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), simpleRow{}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection: this Sinker's declaration is nestedRow")
	}
}

func TestNewSinkerInfersSchemaFromTags(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "Name", Type: bigquery.StringFieldType},
		{Name: "Count", Type: bigquery.IntegerFieldType},
	}
	if !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want %s", formatSchema(s.schema), formatSchema(want))
	}
	if s.metadata != nil {
		t.Errorf("metadata = %+v, want nil for a type that does not implement TableDefiner", s.metadata)
	}
}

func TestNewSinkerReadsTableDefiner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*testing.T) (*bigquery.TableMetadata, error)
		want string
	}{
		{name: "a value receiver implementation", fn: metadataOf[definedRow], want: "value"},
		{name: "a pointer receiver implementation", fn: metadataOf[ptrDefinedRow], want: "pointer"},
		{name: "a pointer type parameter", fn: metadataOf[*ptrDefinedRow], want: "pointer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			md, err := tt.fn(t)
			if err != nil {
				t.Fatalf("testSinker() error = %v", err)
			}
			if md == nil {
				t.Fatal("metadata = nil, want the value from BigQueryTableMetadata")
			}
			if got := md.Labels["receiver"]; got != tt.want {
				t.Errorf("Labels[receiver] = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewSinkerPrefersSchemaFromTableDefiner(t *testing.T) {
	t.Parallel()

	s, err := testSinker[schemaDefinedRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "explicit", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
	}
	if !reflect.DeepEqual(s.schema, want) {
		t.Errorf("schema = %s, want %s", formatSchema(s.schema), formatSchema(want))
	}
}

func TestNewSinkerUsesDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if s.migrationRetry == nil {
		t.Fatal("migrationRetry = nil, want DefaultRetryPolicy")
	}
	if s.migrationRetry() == nil {
		t.Error("migrationRetry() = nil, want a gax.Retryer")
	}
}

func TestWithMigrationStrategyReplacesTheDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	built := 0
	replacement := func() gax.Retryer {
		built++
		return gax.OnErrorFunc(gax.Backoff{}, func(error) bool { return false })
	}
	s, err := testSinker[simpleRow](t,
		WithMigrationStrategy(AppendNewColumns{CreateIfMissing: true}, replacement))
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if built != 0 {
		t.Errorf("the policy was built %d times during NewSinker, want 0", built)
	}
	if _, ok := s.migrationRetry().Retry(errors.New("boom")); ok {
		t.Error("Retry() = true, want the replacement policy's false")
	}
	if built != 1 {
		t.Errorf("the policy was built %d times, want 1", built)
	}
}

func TestNewSinkerRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{name: "a nil migration strategy", opt: WithMigrationStrategy(nil, nil)},
		{
			name: "a SyncAllColumns ignoring an empty name",
			opt:  WithMigrationStrategy(SyncAllColumns{IgnoreColumns: []string{""}}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testSinker[simpleRow](t, tt.opt); err == nil {
				t.Fatal("NewSinker() should reject the option")
			}
		})
	}
}

func TestNewSinkerFailsOnUninferableType(t *testing.T) {
	t.Parallel()

	if _, err := testSinker[unsupportedRow](t); err == nil {
		t.Fatal("NewSinker() should fail when the schema cannot be inferred")
	}
}

func TestNewSinkerUsesDefaultStrategies(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	strategy, ok := s.strategy.(AppendNewColumns)
	if !ok {
		t.Fatalf("strategy = %T, want AppendNewColumns", s.strategy)
	}
	if !strategy.CreateIfMissing {
		t.Error("the default strategy does not create a missing table, want it to")
	}
	if s.migrationRetry == nil {
		t.Error("migrationRetry = nil, want DefaultRetryPolicy")
	}
	if s.api == nil {
		t.Error("api = nil, want the destination table")
	}
}

func TestSinkWithNoRowsDoesNothing(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), []simpleRow{}); err != nil {
		t.Errorf("Sink() with no rows error = %v, want nil without contacting BigQuery", err)
	}
}

func TestSinkRejectsNilRows(t *testing.T) {
	t.Parallel()

	t.Run("nil rows", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.Sink(context.Background(), nil)
		if err == nil || !strings.Contains(err.Error(), "rows is nil") {
			t.Errorf("Sink() error = %v, want it to say rows is nil", err)
		}
	})

	t.Run("a nil element inside a []any batch", func(t *testing.T) {
		t.Parallel()
		s, err := testSinker[simpleRow](t)
		if err != nil {
			t.Fatalf("testSinker() error = %v", err)
		}
		_, err = s.Sink(context.Background(), []any{nil})
		if err == nil || !strings.Contains(err.Error(), "row 0 is nil") {
			t.Errorf("Sink() error = %v, want it to say row 0 is nil", err)
		}
	})
}

func TestSinkRejectsMixedTypesInABatch(t *testing.T) {
	t.Parallel()

	s, err := testSinker[simpleRow](t)
	if err != nil {
		t.Fatalf("testSinker() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), []any{simpleRow{}, nestedRow{}}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection of the mixed types")
	}
}

// TestSinkRejectsALaterBatchOfAnotherType checks that the type the
// declaration named holds for the rest of the Sinker's life, even once a
// batch of the right type has already been written.
func TestSinkRejectsALaterBatchOfAnotherType(t *testing.T) {
	t.Parallel()

	s := newTestSinker[nestedRow](t, migratedTable(), nil)

	if _, err := s.Sink(context.Background(), nestedRow{A: "a", B: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if _, err := s.Sink(context.Background(), simpleRow{}); err == nil {
		t.Fatal("Sink() error = nil, want a rejection: this Sinker declares nestedRow")
	}
}

func TestSinkWithAnEmptyOrNilSliceDoesNotMigrateOrWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows any
	}{
		{name: "an empty slice", rows: []simpleRow{}},
		{name: "a nil slice", rows: []simpleRow(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeTable{metadataErr: notFoundErr()}
			writer := newFakeWriter(t)
			s := newTestSinker[simpleRow](t, fake, writer)

			n, err := s.Sink(context.Background(), tt.rows)
			if err != nil {
				t.Errorf("Sink() error = %v, want nil", err)
			}
			if n != 0 {
				t.Errorf("Sink() n = %d, want 0", n)
			}
			if _, _, metadataCalls := fake.snapshot(); metadataCalls != 0 {
				t.Errorf("Metadata was called %d times, want 0; an empty batch must not migrate", metadataCalls)
			}
			writer.mu.Lock()
			calls := writer.calls
			writer.mu.Unlock()
			if calls != 0 {
				t.Errorf("WriteRows was called %d times, want 0", calls)
			}
		})
	}
}

// TestNewSinkerAcceptsAPointerMigrationNoneWithoutAClient checks that the strategy
// is recognised by what it is rather than by how it was written, since Plan has a
// value receiver and *MigrationNone satisfies MigrationStrategy just as well.
func TestNewSinkerAcceptsAPointerMigrationNoneWithoutAClient(t *testing.T) {
	t.Parallel()

	w := &fakeWriter{relation: testRelation()}
	if _, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(&MigrationNone{}, nil)); err != nil {
		t.Errorf("NewSinker() error = %v, want *MigrationNone to count as MigrationNone", err)
	}
}

// TestNewSinkerRejectsCreateIfMissingWithoutAClient checks that a table which
// cannot be read is not quietly left uncreated: without a client there is nothing
// to create it with, so asking for it is refused rather than ignored.
func TestNewSinkerRejectsCreateIfMissingWithoutAClient(t *testing.T) {
	t.Parallel()

	w := &fakeWriter{relation: testRelation()}
	_, err := NewSinker(w, DeclarationOf[simpleRow](), WithMigrationStrategy(MigrationNone{CreateIfMissing: true}, nil))
	if err == nil {
		t.Fatal("NewSinker() error = nil, want CreateIfMissing without a client to be refused")
	}
	if !strings.Contains(err.Error(), "CreateIfMissing") {
		t.Errorf("NewSinker() error = %v, want it to name the setting it cannot honour", err)
	}
}
