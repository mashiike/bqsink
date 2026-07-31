package bqsink

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
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

func TestNewRejectsNilClient(t *testing.T) {
	t.Parallel()

	if _, err := New[simpleRow](nil, testRelation()); err == nil {
		t.Fatal("New() with a nil client should fail")
	}
}

func TestNewRejectsIncompleteRelation(t *testing.T) {
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
			if _, err := New[simpleRow](testClient(t), tt.relation); err == nil {
				t.Fatalf("New() with %+v should fail", tt.relation)
			}
		})
	}
}

func TestNewFillsProjectFromClient(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), Relation{DatasetID: "ds", TableID: "tbl"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := s.Relation().ProjectID; got != "test-project" {
		t.Errorf("Relation().ProjectID = %q, want %q", got, "test-project")
	}
	if got, want := s.Relation().String(), "test-project.ds.tbl"; got != want {
		t.Errorf("Relation().String() = %q, want %q", got, want)
	}
	if got, want := s.Table().FullyQualifiedName(), "test-project:ds.tbl"; got != want {
		t.Errorf("Table().FullyQualifiedName() = %q, want %q", got, want)
	}
}

func TestNewKeepsExplicitProject(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), Relation{ProjectID: "other", DatasetID: "ds", TableID: "tbl"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := s.Relation().ProjectID; got != "other" {
		t.Errorf("Relation().ProjectID = %q, want %q", got, "other")
	}
	if got, want := s.Table().FullyQualifiedName(), "other:ds.tbl"; got != want {
		t.Errorf("Table().FullyQualifiedName() = %q, want %q", got, want)
	}
}

func TestNewInfersSchemaFromTags(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "Name", Type: bigquery.StringFieldType},
		{Name: "Count", Type: bigquery.IntegerFieldType},
	}
	if !reflect.DeepEqual(s.Schema(), want) {
		t.Errorf("Schema() = %s, want %s", formatSchema(s.Schema()), formatSchema(want))
	}
	if s.metadata != nil {
		t.Errorf("metadata = %+v, want nil for a type that does not implement TableDefiner", s.metadata)
	}
}

func TestNewReadsTableDefiner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*bigquery.Client) (*bigquery.TableMetadata, error)
		want string
	}{
		{
			name: "a value receiver implementation",
			fn: func(c *bigquery.Client) (*bigquery.TableMetadata, error) {
				return metadataOf(New[definedRow](c, testRelation()))
			},
			want: "value",
		},
		{
			name: "a pointer receiver implementation",
			fn: func(c *bigquery.Client) (*bigquery.TableMetadata, error) {
				return metadataOf(New[ptrDefinedRow](c, testRelation()))
			},
			want: "pointer",
		},
		{
			name: "a pointer type parameter",
			fn: func(c *bigquery.Client) (*bigquery.TableMetadata, error) {
				return metadataOf(New[*ptrDefinedRow](c, testRelation()))
			},
			want: "pointer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			md, err := tt.fn(testClient(t))
			if err != nil {
				t.Fatalf("New() error = %v", err)
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

func metadataOf[T any](s *Sinker[T], err error) (*bigquery.TableMetadata, error) {
	if err != nil {
		return nil, err
	}
	return s.metadata, nil
}

func TestNewPrefersSchemaFromTableDefiner(t *testing.T) {
	t.Parallel()

	s, err := New[schemaDefinedRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	want := bigquery.Schema{
		{Name: "explicit", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9},
	}
	if !reflect.DeepEqual(s.Schema(), want) {
		t.Errorf("Schema() = %s, want %s", formatSchema(s.Schema()), formatSchema(want))
	}
}

func TestNewUsesDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
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
	s, err := New[simpleRow](testClient(t), testRelation(),
		WithMigrationStrategy(AppendNewColumns{CreateIfMissing: true}, replacement))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if built != 0 {
		t.Errorf("the policy was built %d times during New, want 0", built)
	}
	if _, ok := s.migrationRetry().Retry(errors.New("boom")); ok {
		t.Error("Retry() = true, want the replacement policy's false")
	}
	if built != 1 {
		t.Errorf("the policy was built %d times, want 1", built)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{name: "a nil migration strategy", opt: WithMigrationStrategy(nil, nil)},
		{name: "a nil write strategy", opt: WithWriteStrategy(nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := New[simpleRow](testClient(t), testRelation(), tt.opt); err == nil {
				t.Fatal("New() should reject the option")
			}
		})
	}
}

func TestNewFailsOnUninferableType(t *testing.T) {
	t.Parallel()

	if _, err := New[unsupportedRow](testClient(t), testRelation()); err == nil {
		t.Fatal("New() should fail when the schema cannot be inferred")
	}
}

func TestNewUsesDefaultStrategies(t *testing.T) {
	t.Parallel()

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	strategy, ok := s.strategy.(AppendNewColumns)
	if !ok {
		t.Fatalf("strategy = %T, want AppendNewColumns", s.strategy)
	}
	if !strategy.CreateIfMissing {
		t.Error("the default strategy does not create a missing table, want it to")
	}
	if _, ok := s.writeStrategy.(*StorageWrite); !ok {
		t.Errorf("writeStrategy = %T, want *StorageWrite", s.writeStrategy)
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

	s, err := New[simpleRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := s.Sink(context.Background()); err != nil {
		t.Errorf("Sink() with no rows error = %v, want nil without contacting BigQuery", err)
	}
}

func TestNewRejectsInvalidStrategies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opt  Option
	}{
		{
			name: "a StorageWrite naming both a stream and a type",
			opt: WithWriteStrategy(&StorageWrite{
				StreamName: "projects/p/datasets/d/tables/t/streams/s",
				StreamType: managedwriter.CommittedStream,
			}),
		},
		{
			name: "a SyncAllColumns ignoring an empty name",
			opt:  WithMigrationStrategy(SyncAllColumns{IgnoreColumns: []string{""}}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := New[simpleRow](testClient(t), testRelation(), tt.opt); err == nil {
				t.Fatal("New() should reject the strategy its Validate rejects")
			}
		})
	}
}
