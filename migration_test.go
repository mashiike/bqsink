package bqsink

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

func formatSchema(s bigquery.Schema) string {
	if s == nil {
		return "<nil>"
	}
	parts := make([]string, len(s))
	for i, f := range s {
		mode := "NULLABLE"
		switch {
		case f.Repeated:
			mode = "REPEATED"
		case f.Required:
			mode = "REQUIRED"
		}
		parts[i] = fmt.Sprintf("%s %s %s", f.Name, f.Type, mode)
		if len(f.Schema) > 0 {
			parts[i] += "<" + formatSchema(f.Schema) + ">"
		}
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

type diffSummary struct {
	added     []string
	removed   []string
	relaxed   []string
	conflicts []string
}

func summarize(d SchemaDiff) diffSummary {
	s := diffSummary{removed: d.Removed, relaxed: d.Relaxed}
	for _, f := range d.Added {
		s.added = append(s.added, f.Name)
	}
	for _, c := range d.Conflicts {
		s.conflicts = append(s.conflicts, c.String())
	}
	return s
}

type changeSummary struct {
	create bool
	add    []string
	relax  []string
	drop   []string
}

func summarizeChange(c SchemaChange) changeSummary {
	s := changeSummary{create: c.CreateTable, relax: c.RelaxColumns, drop: c.DropColumns}
	for _, f := range c.AddColumns {
		s.add = append(s.add, f.Name)
	}
	return s
}

func TestDiffSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bigquery.Schema
		got  bigquery.Schema
		sum  diffSummary
	}{
		{
			name: "identical schemas produce no diff",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
				{Name: "b", Type: bigquery.IntegerFieldType, Required: true},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
				{Name: "b", Type: bigquery.IntegerFieldType, Required: true},
			},
			sum: diffSummary{},
		},
		{
			name: "a declared column the table lacks is added",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
				{Name: "b", Type: bigquery.IntegerFieldType},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{added: []string{"b"}},
		},
		{
			name: "a table column the declaration lacks is removed",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{removed: []string{"legacy"}},
		},
		{
			name: "REQUIRED to NULLABLE is a relaxation",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType, Required: true},
			},
			sum: diffSummary{relaxed: []string{"a"}},
		},
		{
			name: "NULLABLE to REQUIRED conflicts",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType, Required: true},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{conflicts: []string{"a: declared REQUIRED, table has NULLABLE"}},
		},
		{
			name: "a differing type conflicts",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.IntegerFieldType},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{conflicts: []string{"a: declared INTEGER, table has STRING"}},
		},
		{
			name: "a differing repeated mode conflicts",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType, Repeated: true},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{conflicts: []string{"a: declared repeated=true, table has repeated=false"}},
		},
		{
			name: "additions, removals and relaxations appear together",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
				{Name: "added", Type: bigquery.IntegerFieldType},
			},
			got: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType, Required: true},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
			sum: diffSummary{added: []string{"added"}, removed: []string{"legacy"}, relaxed: []string{"a"}},
		},
		{
			name: "every column is added when the table has none",
			want: bigquery.Schema{
				{Name: "a", Type: bigquery.StringFieldType},
			},
			got: nil,
			sum: diffSummary{added: []string{"a"}},
		},
		{
			name: "matching RECORD fields produce no diff",
			want: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
					{Name: "b", Type: bigquery.IntegerFieldType},
				}},
			},
			got: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
					{Name: "b", Type: bigquery.IntegerFieldType},
				}},
			},
			sum: diffSummary{},
		},
		{
			name: "a field added inside a RECORD conflicts",
			want: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
					{Name: "b", Type: bigquery.IntegerFieldType},
				}},
			},
			got: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
				}},
			},
			sum: diffSummary{conflicts: []string{"inner: RECORD fields differ, declared [a b], table has [a]"}},
		},
		{
			name: "a changed type inside a RECORD conflicts",
			want: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.IntegerFieldType},
				}},
			},
			got: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
				}},
			},
			sum: diffSummary{conflicts: []string{"inner: RECORD fields differ, declared [a], table has [a]"}},
		},
		{
			name: "a difference two levels deep is detected",
			want: bigquery.Schema{
				{Name: "outer", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
						{Name: "deep", Type: bigquery.StringFieldType},
					}},
				}},
			},
			got: bigquery.Schema{
				{Name: "outer", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
						{Name: "deep", Type: bigquery.IntegerFieldType},
					}},
				}},
			},
			sum: diffSummary{conflicts: []string{"outer: RECORD fields differ, declared [inner], table has [inner]"}},
		},
		{
			name: "RECORD fields are compared regardless of order",
			want: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.StringFieldType},
					{Name: "b", Type: bigquery.IntegerFieldType},
				}},
			},
			got: bigquery.Schema{
				{Name: "inner", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
					{Name: "b", Type: bigquery.IntegerFieldType},
					{Name: "a", Type: bigquery.StringFieldType},
				}},
			},
			sum: diffSummary{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := summarize(DiffSchema(tt.want, tt.got))
			if !reflect.DeepEqual(got, tt.sum) {
				t.Errorf("DiffSchema() mismatch\n got: %+v\nwant: %+v", got, tt.sum)
			}
		})
	}
}

func TestSchemaDiffEmpty(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}}
	if !DiffSchema(schema, schema).Empty() {
		t.Error("DiffSchema() of an identical schema should be empty")
	}
	if DiffSchema(schema, nil).Empty() {
		t.Error("DiffSchema() against an empty table should not be empty")
	}
}

func TestSchemaDiffWithout(t *testing.T) {
	t.Parallel()

	declared := bigquery.Schema{
		{Name: "keep", Type: bigquery.StringFieldType},
		{Name: "new", Type: bigquery.StringFieldType},
		{Name: "typed", Type: bigquery.IntegerFieldType},
	}
	table := bigquery.Schema{
		{Name: "keep", Type: bigquery.StringFieldType, Required: true},
		{Name: "typed", Type: bigquery.StringFieldType},
		{Name: "legacy", Type: bigquery.StringFieldType},
	}
	diff := DiffSchema(declared, table)

	t.Run("an empty list changes nothing", func(t *testing.T) {
		t.Parallel()
		if !reflect.DeepEqual(summarize(diff.Without(nil)), summarize(diff)) {
			t.Error("Without(nil) should return the diff unchanged")
		}
	})

	t.Run("named columns disappear from every list", func(t *testing.T) {
		t.Parallel()
		got := summarize(diff.Without([]string{"new", "typed", "legacy", "keep"}))
		if !reflect.DeepEqual(got, diffSummary{}) {
			t.Errorf("Without() left %+v, want an empty diff", got)
		}
	})

	t.Run("unnamed columns stay", func(t *testing.T) {
		t.Parallel()
		got := summarize(diff.Without([]string{"legacy"}))
		want := diffSummary{
			added:     []string{"new"},
			relaxed:   []string{"keep"},
			conflicts: []string{"typed: declared INTEGER, table has STRING"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Without() = %+v, want %+v", got, want)
		}
	})
}

func TestMigrationStrategyPlan(t *testing.T) {
	t.Parallel()

	missing := TableState{}
	drifted := TableState{
		Exists: true,
		Diff: DiffSchema(
			bigquery.Schema{
				{Name: "keep", Type: bigquery.StringFieldType},
				{Name: "new", Type: bigquery.StringFieldType},
			},
			bigquery.Schema{
				{Name: "keep", Type: bigquery.StringFieldType, Required: true},
				{Name: "legacy", Type: bigquery.StringFieldType},
			},
		),
	}
	conflicted := TableState{
		Exists: true,
		Diff: DiffSchema(
			bigquery.Schema{{Name: "a", Type: bigquery.IntegerFieldType}},
			bigquery.Schema{{Name: "a", Type: bigquery.StringFieldType}},
		),
	}

	tests := []struct {
		name     string
		strategy MigrationStrategy
		state    TableState
		want     changeSummary
		wantErr  error
	}{
		{
			name:     "MigrationNone leaves a drifted table alone",
			strategy: MigrationNone{},
			state:    drifted,
			want:     changeSummary{},
		},
		{
			name:     "MigrationNone does not create a missing table by default",
			strategy: MigrationNone{},
			state:    missing,
			want:     changeSummary{},
		},
		{
			name:     "MigrationNone creates a missing table when asked",
			strategy: MigrationNone{CreateIfMissing: true},
			state:    missing,
			want:     changeSummary{create: true},
		},
		{
			name:     "MigrationNone still reports a conflict",
			strategy: MigrationNone{},
			state:    conflicted,
			wantErr:  ErrSchemaConflict,
		},
		{
			name:     "AppendNewColumns adds and relaxes but does not drop",
			strategy: AppendNewColumns{},
			state:    drifted,
			want:     changeSummary{add: []string{"new"}, relax: []string{"keep"}},
		},
		{
			name:     "AppendNewColumns does not create a missing table by default",
			strategy: AppendNewColumns{},
			state:    missing,
			want:     changeSummary{},
		},
		{
			name:     "AppendNewColumns creates a missing table when asked",
			strategy: AppendNewColumns{CreateIfMissing: true},
			state:    missing,
			want:     changeSummary{create: true},
		},
		{
			name:     "AppendNewColumns reports a conflict",
			strategy: AppendNewColumns{},
			state:    conflicted,
			wantErr:  ErrSchemaConflict,
		},
		{
			name:     "SyncAllColumns also drops",
			strategy: SyncAllColumns{},
			state:    drifted,
			want:     changeSummary{add: []string{"new"}, relax: []string{"keep"}, drop: []string{"legacy"}},
		},
		{
			name:     "SyncAllColumns keeps an ignored column",
			strategy: SyncAllColumns{IgnoreColumns: []string{"legacy"}},
			state:    drifted,
			want:     changeSummary{add: []string{"new"}, relax: []string{"keep"}},
		},
		{
			name:     "SyncAllColumns ignores a conflict on an ignored column",
			strategy: SyncAllColumns{IgnoreColumns: []string{"a"}},
			state:    conflicted,
			want:     changeSummary{},
		},
		{
			name:     "SyncAllColumns reports a conflict on a managed column",
			strategy: SyncAllColumns{IgnoreColumns: []string{"other"}},
			state:    conflicted,
			wantErr:  ErrSchemaConflict,
		},
		{
			name:     "SyncAllColumns creates a missing table when asked",
			strategy: SyncAllColumns{CreateIfMissing: true},
			state:    missing,
			want:     changeSummary{create: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			change, err := tt.strategy.Plan(tt.state, discardLogger())
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Plan() error = %v, want one wrapping %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Plan() error = %v", err)
			}
			if got := summarizeChange(change); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Plan() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestSchemaChangeEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change SchemaChange
		want   bool
	}{
		{name: "the zero value is empty", change: SchemaChange{}, want: true},
		{name: "creating a table is not empty", change: SchemaChange{CreateTable: true}},
		{
			name:   "adding a column is not empty",
			change: SchemaChange{AddColumns: bigquery.Schema{{Name: "a"}}},
		},
		{name: "relaxing a column is not empty", change: SchemaChange{RelaxColumns: []string{"a"}}},
		{name: "dropping a column is not empty", change: SchemaChange{DropColumns: []string{"a"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.change.Empty(); got != tt.want {
				t.Errorf("Empty() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestMergeSchema(t *testing.T) {
	t.Parallel()

	declared := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType},
		{Name: "b", Type: bigquery.IntegerFieldType},
	}
	table := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType, Required: true},
		{Name: "legacy", Type: bigquery.StringFieldType},
	}
	change, err := AppendNewColumns{}.Plan(TableState{Exists: true, Diff: DiffSchema(declared, table)}, discardLogger())
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	merged := mergeSchema(table, change)

	expect := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType},
		{Name: "legacy", Type: bigquery.StringFieldType},
		{Name: "b", Type: bigquery.IntegerFieldType},
	}
	if !reflect.DeepEqual(merged, expect) {
		t.Errorf("mergeSchema() mismatch\n got: %s\nwant: %s", formatSchema(merged), formatSchema(expect))
	}
	if !table[0].Required {
		t.Error("mergeSchema() must not modify the schema it was given")
	}
	if merged[2] == declared[1] {
		t.Error("mergeSchema() must copy the added columns instead of sharing them")
	}
}

func TestRequiredNames(t *testing.T) {
	t.Parallel()

	schema := bigquery.Schema{
		{Name: "a", Type: bigquery.StringFieldType},
		{Name: "b", Type: bigquery.StringFieldType, Required: true},
		{Name: "c", Type: bigquery.StringFieldType, Required: true},
	}
	got := requiredNames(schema)
	want := []string{"b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("requiredNames() = %v, want %v", got, want)
	}
}

func TestClassifyAPIError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		err              error
		notFound         bool
		concurrentChange bool
	}{
		{name: "nil", err: nil},
		{name: "a plain error", err: errors.New("boom")},
		{name: "not found", err: &googleapi.Error{Code: http.StatusNotFound}, notFound: true},
		{name: "precondition failed", err: &googleapi.Error{Code: http.StatusPreconditionFailed}, concurrentChange: true},
		{name: "conflict", err: &googleapi.Error{Code: http.StatusConflict}, concurrentChange: true},
		{name: "forbidden", err: &googleapi.Error{Code: http.StatusForbidden}},
		{
			name:     "a wrapped not found",
			err:      fmt.Errorf("read metadata: %w", &googleapi.Error{Code: http.StatusNotFound}),
			notFound: true,
		},
		{
			name:             "a wrapped precondition failure",
			err:              fmt.Errorf("update schema: %w", &googleapi.Error{Code: http.StatusPreconditionFailed}),
			concurrentChange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isNotFound(tt.err); got != tt.notFound {
				t.Errorf("isNotFound() = %t, want %t", got, tt.notFound)
			}
			if got := isConcurrentChange(tt.err); got != tt.concurrentChange {
				t.Errorf("isConcurrentChange() = %t, want %t", got, tt.concurrentChange)
			}
		})
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	t.Run("retries a concurrent change up to the limit", func(t *testing.T) {
		t.Parallel()
		policy := DefaultRetryPolicy()
		err := &googleapi.Error{Code: http.StatusPreconditionFailed}
		for i := range migrateMaxRetries {
			pause, ok := policy.Retry(err)
			if !ok {
				t.Fatalf("retry %d: Retry() = false, want true", i+1)
			}
			if pause <= 0 {
				t.Errorf("retry %d: pause = %v, want a positive duration", i+1, pause)
			}
		}
		if _, ok := policy.Retry(err); ok {
			t.Errorf("Retry() = true after %d retries, want false", migrateMaxRetries)
		}
	})

	t.Run("does not retry other errors", func(t *testing.T) {
		t.Parallel()
		policy := DefaultRetryPolicy()
		if _, ok := policy.Retry(&googleapi.Error{Code: http.StatusForbidden}); ok {
			t.Error("Retry() = true for a permission error, want false")
		}
		if _, ok := policy.Retry(errors.New("boom")); ok {
			t.Error("Retry() = true for a plain error, want false")
		}
	})

	t.Run("each call returns a fresh policy", func(t *testing.T) {
		t.Parallel()
		err := &googleapi.Error{Code: http.StatusPreconditionFailed}
		exhausted := DefaultRetryPolicy()
		for range migrateMaxRetries {
			exhausted.Retry(err)
		}
		if _, ok := DefaultRetryPolicy().Retry(err); !ok {
			t.Error("a newly built policy should still allow a retry")
		}
	})
}

func TestConflictReasonString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason ConflictReason
		want   string
	}{
		{reason: ConflictType, want: "type"},
		{reason: ConflictRepeated, want: "repeated"},
		{reason: ConflictRequired, want: "required"},
		{reason: ConflictNested, want: "nested"},
		{reason: ConflictReason(99), want: "ConflictReason(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			if got := tt.reason.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
