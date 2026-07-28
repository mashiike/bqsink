package bqsink

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"github.com/mashiike/bqsink/bqgcs"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Integration tests write to a real BigQuery project, so they only run when the
// environment names one. Nothing here carries a project id, and an unset variable
// skips rather than fails.
const (
	testProjectEnv     = "BQSINK_TEST_PROJECT"
	testDatasetEnv     = "BQSINK_TEST_DATASET"
	defaultTestDataset = "bqsink_integration_test"
)

// integrationTarget names where an integration test writes, skipping the test when
// no project is configured.
func integrationTarget(t *testing.T) (projectID, datasetID string) {
	t.Helper()
	projectID = os.Getenv(testProjectEnv)
	if projectID == "" {
		t.Skipf("%s is unset; skipping the tests that write to BigQuery", testProjectEnv)
	}
	datasetID = os.Getenv(testDatasetEnv)
	if datasetID == "" {
		datasetID = defaultTestDataset
	}
	return projectID, datasetID
}

func integrationClient(t *testing.T, projectID string) *bigquery.Client {
	t.Helper()
	client, err := bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		t.Fatalf("bigquery.NewClient(%s) error = %v", projectID, err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client.Close() error = %v", err)
		}
	})
	return client
}

func ensureDataset(t *testing.T, client *bigquery.Client, datasetID string) {
	t.Helper()
	ds := client.Dataset(datasetID)
	if _, err := ds.Metadata(context.Background()); err == nil {
		return
	}
	err := ds.Create(context.Background(), &bigquery.DatasetMetadata{
		Name:        datasetID,
		Description: "scratch dataset for bqsink integration tests",
	})
	if err != nil && !isAlreadyExists(err) {
		t.Fatalf("create dataset %s: %v", datasetID, err)
	}
}

func isAlreadyExists(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusConflict
}

// integrationRelation returns a relation naming a table unique to this test run,
// and arranges for the table to be removed afterwards.
func integrationRelation(t *testing.T, client *bigquery.Client, projectID, datasetID string) Relation {
	t.Helper()
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		}
		return '_'
	}, t.Name())
	relation := Relation{
		ProjectID: projectID,
		DatasetID: datasetID,
		TableID:   fmt.Sprintf("%s_%d", safe, time.Now().UnixNano()),
	}
	t.Cleanup(func() {
		// A test that never creates the table is normal, so only a real failure to
		// remove one is worth reporting.
		if err := relation.table(client).Delete(context.Background()); err != nil && !isNotFound(err) {
			t.Logf("could not delete %s: %v", relation, err)
		}
	})
	return relation
}

// readRows reads the table back, retrying while it reports no rows, since a write
// is not always visible the instant it is acknowledged.
func readRows(t *testing.T, client *bigquery.Client, relation Relation, want int) []map[string]bigquery.Value {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		rows := readRowsOnce(t, client, relation)
		if len(rows) >= want || time.Now().After(deadline) {
			return rows
		}
		time.Sleep(3 * time.Second)
	}
}

func readRowsOnce(t *testing.T, client *bigquery.Client, relation Relation) []map[string]bigquery.Value {
	t.Helper()
	it := relation.table(client).Read(context.Background())
	var rows []map[string]bigquery.Value
	for {
		row := map[string]bigquery.Value{}
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			return rows
		}
		if err != nil {
			t.Fatalf("read %s: %v", relation, err)
		}
		rows = append(rows, row)
	}
}

type integrationRow struct {
	ID     string            `bqsink:"id,required"`
	Count  int64             `bqsink:"count"`
	Rate   float64           `bqsink:"rate"`
	Flag   bool              `bqsink:"flag"`
	Blob   []byte            `bqsink:"blob"`
	At     time.Time         `bqsink:"at"`
	Day    civil.Date        `bqsink:"day"`
	Clock  civil.Time        `bqsink:"clock"`
	Moment civil.DateTime    `bqsink:"moment"`
	Money  *big.Rat          `bqsink:"money"`
	Tags   []string          `bqsink:"tags"`
	Doc    map[string]string `bqsink:"doc"`
	Inner  nestedRow         `bqsink:"inner,record"`
	Absent *string           `bqsink:"absent"`
}

func sampleIntegrationRow(id string) integrationRow {
	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	return integrationRow{
		ID:     id,
		Count:  42,
		Rate:   1.5,
		Flag:   true,
		Blob:   []byte("bytes"),
		At:     at,
		Day:    civil.DateOf(at),
		Clock:  civil.TimeOf(at),
		Moment: civil.DateTimeOf(at),
		Money:  big.NewRat(25, 2),
		Tags:   []string{"a", "b"},
		Doc:    map[string]string{"url": "https://x/?a=1&b=2"},
		Inner:  nestedRow{A: "inner", B: 7},
	}
}

// checkIntegrationRow verifies the values BigQuery returns for a row written by
// sampleIntegrationRow. It is where the representations chosen for each type are
// finally confirmed against the server.
func checkIntegrationRow(t *testing.T, row map[string]bigquery.Value, id string) {
	t.Helper()
	if got := row["id"]; got != id {
		t.Errorf("id = %#v, want %q", got, id)
	}
	if got := row["count"]; got != int64(42) {
		t.Errorf("count = %#v, want 42", got)
	}
	if got := row["rate"]; got != 1.5 {
		t.Errorf("rate = %#v, want 1.5", got)
	}
	if got := row["flag"]; got != true {
		t.Errorf("flag = %#v, want true", got)
	}
	if got, ok := row["blob"].([]byte); !ok || string(got) != "bytes" {
		t.Errorf("blob = %#v, want []byte(\"bytes\")", row["blob"])
	}
	at, ok := row["at"].(time.Time)
	if !ok || !at.Equal(time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)) {
		t.Errorf("at = %#v, want 2026-07-28T12:30:00Z", row["at"])
	}
	if got, ok := row["day"].(civil.Date); !ok || got.String() != "2026-07-28" {
		t.Errorf("day = %#v, want 2026-07-28", row["day"])
	}
	if got, ok := row["clock"].(civil.Time); !ok || got.String() != "12:30:00" {
		t.Errorf("clock = %#v, want 12:30:00", row["clock"])
	}
	if got, ok := row["moment"].(civil.DateTime); !ok || got.String() != "2026-07-28T12:30:00" {
		t.Errorf("moment = %#v, want 2026-07-28T12:30:00", row["moment"])
	}
	money, ok := row["money"].(*big.Rat)
	if !ok {
		t.Errorf("money = %#v, want a *big.Rat", row["money"])
	} else if money.Cmp(big.NewRat(25, 2)) != 0 {
		t.Errorf("money = %s, want 12.5; the decimal rendering did not survive", money.FloatString(2))
	}
	tags, ok := row["tags"].([]bigquery.Value)
	if !ok || len(tags) != 2 || tags[0] != "a" || tags[1] != "b" {
		t.Errorf("tags = %#v, want [a b]", row["tags"])
	}
	doc, ok := row["doc"].(string)
	switch {
	case !ok:
		t.Errorf("doc = %#v, want the JSON text", row["doc"])
	case !strings.Contains(doc, `"url"`):
		t.Errorf("doc = %s, want the url key", doc)
	case strings.Contains(doc, "\\u0026"):
		t.Errorf("doc = %s, want the ampersand unescaped", doc)
	case !strings.Contains(doc, "&"):
		t.Errorf("doc = %s, want the ampersand to survive", doc)
	case strings.HasPrefix(doc, `"`):
		t.Errorf("doc = %s, want the JSON value rather than a quoted string", doc)
	}
	inner, ok := row["inner"].(map[string]bigquery.Value)
	if !ok {
		t.Errorf("inner = %#v, want a nested map", row["inner"])
	} else {
		if inner["A"] != "inner" {
			t.Errorf("inner.A = %#v, want inner", inner["A"])
		}
		if inner["B"] != int64(7) {
			t.Errorf("inner.B = %#v, want 7", inner["B"])
		}
	}
	if got := row["absent"]; got != nil {
		t.Errorf("absent = %#v, want nil", got)
	}
}

func TestIntegrationLoadJobs(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	s, err := New[integrationRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	if err := s.Sink(ctx, sampleIntegrationRow("load")); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	checkIntegrationRow(t, rows[0], "load")
}

func TestIntegrationStorageWrite(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	s, err := New[integrationRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&StorageWrite{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	if err := s.Sink(ctx, sampleIntegrationRow("storage")); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	checkIntegrationRow(t, rows[0], "storage")
}

// narrowRow declares fewer columns than integrationRow, to check that a load job
// carrying a schema narrower than the table is accepted.
type narrowRow struct {
	ID    string `bqsink:"id,required"`
	Count int64  `bqsink:"count"`
}

func TestIntegrationLoadJobWithANarrowerSchema(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	wide, err := New[integrationRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := wide.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	narrow, err := New[narrowRow](client, relation, WithWriteStrategy(&LoadJobs{}))
	if err != nil {
		t.Fatalf("New() for the narrow row error = %v", err)
	}
	if err := narrow.Sink(ctx, narrowRow{ID: "narrow", Count: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := narrow.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v; a load job whose schema is narrower than the table was rejected", err)
	}

	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	if got := rows[0]["id"]; got != "narrow" {
		t.Errorf("id = %#v, want narrow", got)
	}
	if got := rows[0]["rate"]; got != nil {
		t.Errorf("rate = %#v, want nil for a column the write did not carry", got)
	}
}

// grownRow adds a column to narrowRow, so that AppendNewColumns has something to
// patch onto an existing table.
type grownRow struct {
	ID    string `bqsink:"id,required"`
	Count int64  `bqsink:"count"`
	Extra string `bqsink:"extra"`
}

func TestIntegrationAppendNewColumns(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	before, err := New[narrowRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := before.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	after, err := New[grownRow](client, relation,
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() for the grown row error = %v", err)
	}
	if err := after.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v; the column was not added", err)
	}

	md, err := relation.table(client).Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	found := false
	for _, f := range md.Schema {
		if f.Name == "extra" {
			found = true
			if f.Required {
				t.Error("extra was added as REQUIRED, which BigQuery should not allow")
			}
		}
	}
	if !found {
		t.Errorf("the table has no extra column: %s", formatSchema(md.Schema))
	}

	if err := after.Sink(ctx, grownRow{ID: "grown", Count: 2, Extra: "e"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := after.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	if got := rows[0]["extra"]; got != "e" {
		t.Errorf("extra = %#v, want e", got)
	}
}

func TestIntegrationSyncAllColumnsDropsAColumn(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	wide, err := New[grownRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := wide.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	narrow, err := New[narrowRow](client, relation,
		WithMigration(SyncAllColumns{}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() for the narrow row error = %v", err)
	}
	if err := narrow.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v; the DDL did not run", err)
	}

	md, err := relation.table(client).Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	for _, f := range md.Schema {
		if f.Name == "extra" {
			t.Errorf("extra is still present: %s", formatSchema(md.Schema))
		}
	}
}

func TestIntegrationIgnoreColumnsKeepsAColumn(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	wide, err := New[grownRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := wide.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	narrow, err := New[narrowRow](client, relation,
		WithMigration(SyncAllColumns{IgnoreColumns: []string{"extra"}}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := narrow.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	md, err := relation.table(client).Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	found := false
	for _, f := range md.Schema {
		if f.Name == "extra" {
			found = true
		}
	}
	if !found {
		t.Errorf("extra was dropped despite IgnoreColumns: %s", formatSchema(md.Schema))
	}
}

func TestIntegrationMissingTableIsReported(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	// The table is never created, but a cleanup guards against a change in the
	// default strategy quietly leaving one behind.
	relation := integrationRelation(t, client, projectID, datasetID)

	// The default strategy creates a missing table, so this needs MigrationNone to
	// reach ErrTableMissing at all.
	s, err := New[narrowRow](client, relation,
		WithMigration(MigrationNone{}),
		WithWriteStrategy(&LoadJobs{}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Migrate(context.Background()); !errors.Is(err, ErrTableMissing) {
		t.Errorf("Migrate() error = %v, want one wrapping ErrTableMissing", err)
	}
}

// testBucketEnv names the Cloud Storage bucket the staged load job writes to.
// Without it the staging test skips, since staging cannot be faked against the
// real service.
const testBucketEnv = "BQSINK_TEST_BUCKET"

func integrationBucket(t *testing.T) string {
	t.Helper()
	bucket := os.Getenv(testBucketEnv)
	if bucket == "" {
		t.Skipf("%s is unset; skipping the test that stages rows in Cloud Storage", testBucketEnv)
	}
	return bucket
}

func TestIntegrationLoadJobsThroughCloudStorage(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	bucket := integrationBucket(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := gcs.Close(); err != nil {
			t.Errorf("storage client Close() error = %v", err)
		}
	})

	staging := &bqgcs.Staging{
		Client: gcs,
		Bucket: bucket,
		Prefix: "bqsink-integration",
	}
	s, err := New[integrationRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{Staging: staging}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Sink(ctx, sampleIntegrationRow("staged")); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	checkIntegrationRow(t, rows[0], "staged")
}

func TestIntegrationStagedObjectIsRemoved(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	bucket := integrationBucket(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := gcs.Close(); err != nil {
			t.Errorf("storage client Close() error = %v", err)
		}
	})

	prefix := fmt.Sprintf("bqsink-cleanup-%d", time.Now().UnixNano())
	staging := &bqgcs.Staging{Client: gcs, Bucket: bucket, Prefix: prefix}
	s, err := New[narrowRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{Staging: staging}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Sink(ctx, narrowRow{ID: "staged", Count: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	it := gcs.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	var left []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatalf("list gs://%s/%s: %v", bucket, prefix, err)
		}
		left = append(left, attrs.Name)
	}
	if len(left) != 0 {
		t.Errorf("%d staged object(s) were left behind: %v", len(left), left)
		for _, name := range left {
			if err := gcs.Bucket(bucket).Object(name).Delete(ctx); err != nil {
				t.Logf("could not delete %s: %v", name, err)
			}
		}
	}
}

func TestIntegrationStagedObjectIsKept(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	bucket := integrationBucket(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := gcs.Close(); err != nil {
			t.Errorf("storage client Close() error = %v", err)
		}
	})

	prefix := fmt.Sprintf("bqsink-keep-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		it := gcs.Bucket(bucket).Objects(context.Background(), &storage.Query{Prefix: prefix})
		for {
			attrs, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return
			}
			if err != nil {
				t.Logf("list gs://%s/%s: %v", bucket, prefix, err)
				return
			}
			if err := gcs.Bucket(bucket).Object(attrs.Name).Delete(context.Background()); err != nil {
				t.Logf("could not delete %s: %v", attrs.Name, err)
			}
		}
	})

	staging := &bqgcs.Staging{Client: gcs, Bucket: bucket, Prefix: prefix, Keep: true}
	s, err := New[narrowRow](client, relation,
		WithMigration(AppendNewColumns{CreateIfMissing: true}),
		WithWriteStrategy(&LoadJobs{Staging: staging}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Sink(ctx, narrowRow{ID: "kept", Count: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	it := gcs.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	found := 0
	for {
		if _, err := it.Next(); errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			t.Fatalf("list gs://%s/%s: %v", bucket, prefix, err)
		}
		found++
	}
	if found != 1 {
		t.Errorf("%d staged object(s) remain, want 1 since Keep is set", found)
	}
}

// metadataIntegrationRow checks that BigQuery accepts the underscore-prefixed
// columns IngestionMetadata declares, and that the values arrive.
type metadataIntegrationRow struct {
	IngestionMetadata
	ID string `bqsink:"id,required"`
}

func TestIntegrationIngestionMetadata(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	s, err := New[metadataIntegrationRow](client, relation, WithWriteStrategy(&LoadJobs{}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Sink(ctx, metadataIntegrationRow{ID: "with-metadata"}); err != nil {
		t.Fatalf("Sink() error = %v; BigQuery may have rejected the underscore-prefixed columns", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	md, err := relation.table(client).Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	byName := map[string]*bigquery.FieldSchema{}
	for _, f := range md.Schema {
		byName[f.Name] = f
	}
	for _, name := range []string{"_ingestion_at", "_ingestion_id", "_ingestion_row_id"} {
		if byName[name] == nil {
			t.Errorf("the table has no %s column: %s", name, formatSchema(md.Schema))
		}
	}

	rows := readRows(t, client, relation, 1)
	if len(rows) != 1 {
		t.Fatalf("read %d row(s), want 1", len(rows))
	}
	row := rows[0]
	if got := row["id"]; got != "with-metadata" {
		t.Errorf("id = %#v, want with-metadata", got)
	}
	if _, ok := row["_ingestion_at"].(time.Time); !ok {
		t.Errorf("_ingestion_at = %#v, want a time.Time", row["_ingestion_at"])
	}
	jobID, ok := row["_ingestion_id"].(string)
	if !ok || jobID != s.sinkerID {
		t.Errorf("_ingestion_id = %#v, want the Sinker's id %q", row["_ingestion_id"], s.sinkerID)
	}
	rowID, ok := row["_ingestion_row_id"].(string)
	if !ok || rowID == "" {
		t.Errorf("_ingestion_row_id = %#v, want a UUID", row["_ingestion_row_id"])
	}
}

// layoutIntegrationRow describes the table's physical layout entirely in tags.
type layoutIntegrationRow struct {
	At     time.Time `bqsink:"at,required" partition:"day"`
	UserID string    `bqsink:"user_id" cluster:"1"`
	Region string    `bqsink:"region" cluster:"2"`
	Amount *big.Rat  `bqsink:"amount" description:"billed amount, including tax"`
}

// timeColumnIntegrationRow writes one instant into each column the time options
// name. The two transports represent these types differently on the wire, so the
// only place the conversions can be confirmed is a real table.
type timeColumnIntegrationRow struct {
	ID      string    `bqsink:"id,required"`
	At      time.Time `bqsink:"at"`
	OnDay   time.Time `bqsink:"on_day,date" partition:"month"`
	LocalAt time.Time `bqsink:"local_at,datetime"`
	OpenAt  time.Time `bqsink:"open_at,time"`
}

func TestIntegrationTimeColumnOptions(t *testing.T) {
	t.Parallel()

	// A Tokyo morning whose UTC date is the day before, so a conversion reading the
	// wrong location shows up as the wrong day rather than as the same answer twice.
	jst := time.FixedZone("JST", 9*60*60)
	at := time.Date(2026, 7, 29, 7, 30, 15, 0, jst)

	day := civil.Date{Year: 2026, Month: time.July, Day: 29}
	clock := civil.Time{Hour: 7, Minute: 30, Second: 15}
	want := map[string]bigquery.Value{
		"on_day":   day,
		"local_at": civil.DateTime{Date: day, Time: clock},
		"open_at":  clock,
	}

	tests := []struct {
		name     string
		strategy WriteStrategy
	}{
		{name: "LoadJobs", strategy: &LoadJobs{}},
		{name: "StorageWrite", strategy: &StorageWrite{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectID, datasetID := integrationTarget(t)
			client := integrationClient(t, projectID)
			ensureDataset(t, client, datasetID)
			relation := integrationRelation(t, client, projectID, datasetID)

			s, err := New[timeColumnIntegrationRow](client, relation, WithWriteStrategy(tt.strategy))
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			ctx := context.Background()
			row := timeColumnIntegrationRow{ID: tt.name, At: at, OnDay: at, LocalAt: at, OpenAt: at}
			if err := s.Sink(ctx, row); err != nil {
				t.Fatalf("Sink() error = %v", err)
			}
			if err := s.Close(ctx); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			md, err := relation.table(client).Metadata(ctx)
			if err != nil {
				t.Fatalf("Metadata() error = %v", err)
			}
			if md.TimePartitioning == nil || md.TimePartitioning.Field != "on_day" {
				t.Errorf("TimePartitioning = %#v, want the tagged DATE column", md.TimePartitioning)
			}

			rows := readRows(t, client, relation, 1)
			if len(rows) != 1 {
				t.Fatalf("read %d row(s), want 1", len(rows))
			}
			for column, w := range want {
				if got := rows[0][column]; got != w {
					t.Errorf("%s = %v (%T), want %v", column, got, got, w)
				}
			}
			// The TIMESTAMP column keeps the instant itself, which in UTC falls on
			// the previous day. That is the component the other three drop.
			gotAt, ok := rows[0]["at"].(time.Time)
			if !ok {
				t.Fatalf("at = %v (%T), want a time.Time", rows[0]["at"], rows[0]["at"])
			}
			if !gotAt.Equal(at) {
				t.Errorf("at = %s, want %s", gotAt, at)
			}
		})
	}
}

func TestIntegrationTaggedLayout(t *testing.T) {
	t.Parallel()

	projectID, datasetID := integrationTarget(t)
	client := integrationClient(t, projectID)
	ensureDataset(t, client, datasetID)
	relation := integrationRelation(t, client, projectID, datasetID)

	ctx := context.Background()
	s, err := New[layoutIntegrationRow](client, relation, WithWriteStrategy(&LoadJobs{}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v; BigQuery may have rejected the tagged layout", err)
	}

	md, err := relation.table(client).Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if md.TimePartitioning == nil {
		t.Fatal("the table is not partitioned")
	}
	if got, want := md.TimePartitioning.Field, "at"; got != want {
		t.Errorf("partitioning field = %q, want %q", got, want)
	}
	if got, want := md.TimePartitioning.Type, bigquery.DayPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
	if md.Clustering == nil {
		t.Fatal("the table is not clustered")
	}
	want := []string{"user_id", "region"}
	if !reflect.DeepEqual(md.Clustering.Fields, want) {
		t.Errorf("clustering fields = %v, want %v", md.Clustering.Fields, want)
	}
	for _, f := range md.Schema {
		if f.Name != "amount" {
			continue
		}
		if got, want := f.Description, "billed amount, including tax"; got != want {
			t.Errorf("amount description = %q, want %q", got, want)
		}
	}
}
