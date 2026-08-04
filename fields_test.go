package bqsink

import (
	"reflect"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
)

type embeddedBase struct {
	ID   string `bqsink:"id"`
	Kind string `bqsink:"kind"`
}

// promotingRow embeds a struct whose columns are promoted into it.
type promotingRow struct {
	embeddedBase
	Name string `bqsink:"name"`
}

// lockedRow embeds a sync.Mutex, which contributes no columns because it has no
// exported fields.
type lockedRow struct {
	sync.Mutex
	Name string `bqsink:"name"`
}

// namedEmbedRow gives its embedded struct a column name, so it becomes one JSON
// column instead of being promoted.
type namedEmbedRow struct {
	embeddedBase `bqsink:"base"`
	Name         string `bqsink:"name"`
}

// skippedEmbedRow drops the embedded struct entirely.
type skippedEmbedRow struct {
	embeddedBase `bqsink:"-"`
	Name         string `bqsink:"name"`
}

// shadowingRow declares a column the embedded struct also declares; the shallower
// one wins.
type shadowingRow struct {
	embeddedBase
	ID   string `bqsink:"id"`
	Name string `bqsink:"name"`
}

type otherBase struct {
	ID string `bqsink:"id"`
}

// ambiguousRow embeds two structs that both declare "id" at the same depth, so
// neither is used.
type ambiguousRow struct {
	embeddedBase
	otherBase
	Name string `bqsink:"name"`
}

// taggedTieRow has the same collision, but only one side names the column in a
// tag, which settles it.
type untaggedBase struct {
	Value string
}

type taggedBase struct {
	Other string `bqsink:"Value"`
}

type taggedTieRow struct {
	untaggedBase
	taggedBase
	Name string `bqsink:"name"`
}

// deepRow promotes through two levels of embedding.
type middleLevel struct {
	embeddedBase
	Middle string `bqsink:"middle"`
}

type deepRow struct {
	middleLevel
	Name string `bqsink:"name"`
}

// ptrEmbedRow embeds by pointer, which may be nil at write time.
type ptrEmbedRow struct {
	*embeddedBase
	Name string `bqsink:"name"`
}

func columnNames(schema bigquery.Schema) []string {
	names := make([]string, len(schema))
	for i, f := range schema {
		names[i] = f.Name
	}
	return names
}

func TestEmbeddedFieldsArePromoted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(...*Marshalers) (bigquery.Schema, error)
		want []string
	}{
		{
			name: "an embedded struct's columns are promoted",
			fn:   inferSchema[promotingRow],
			want: []string{"id", "kind", "name"},
		},
		{
			name: "an embedded type with no exported fields contributes nothing",
			fn:   inferSchema[lockedRow],
			want: []string{"name"},
		},
		{
			name: "a named embedded struct becomes one column",
			fn:   inferSchema[namedEmbedRow],
			want: []string{"base", "name"},
		},
		{
			name: "an embedded struct can be dropped",
			fn:   inferSchema[skippedEmbedRow],
			want: []string{"name"},
		},
		{
			name: "the shallower field hides the promoted one of the same name",
			fn:   inferSchema[shadowingRow],
			want: []string{"kind", "id", "name"},
		},
		{
			name: "a tie at the same depth removes that column only",
			fn:   inferSchema[ambiguousRow],
			want: []string{"kind", "name"},
		},
		{
			name: "an explicit tag settles a tie at the same depth",
			fn:   inferSchema[taggedTieRow],
			want: []string{"Value", "name"},
		},
		{
			name: "promotion works through two levels",
			fn:   inferSchema[deepRow],
			want: []string{"id", "kind", "middle", "name"},
		},
		{
			name: "an embedded pointer is promoted too",
			fn:   inferSchema[ptrEmbedRow],
			want: []string{"id", "kind", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			schema, err := tt.fn()
			if err != nil {
				t.Fatalf("inferSchema() error = %v", err)
			}
			if got := columnNames(schema); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("columns = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNamedEmbeddedStructIsAJSONColumn(t *testing.T) {
	t.Parallel()

	schema, err := inferSchema[namedEmbedRow]()
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	if schema[0].Type != bigquery.JSONFieldType {
		t.Errorf("base type = %s, want JSON", schema[0].Type)
	}
}

func TestToRowPromotesEmbeddedValues(t *testing.T) {
	t.Parallel()

	s, err := New[deepRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(deepRow{
		middleLevel: middleLevel{
			embeddedBase: embeddedBase{ID: "abc", Kind: "k"},
			Middle:       "m",
		},
		Name: "n",
	})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	want := map[string]bigquery.Value{
		"id":     "abc",
		"kind":   "k",
		"middle": "m",
		"name":   "n",
	}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v", row, want)
	}
}

func TestToRowShadowedFieldUsesTheOuterValue(t *testing.T) {
	t.Parallel()

	s, err := New[shadowingRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(shadowingRow{
		embeddedBase: embeddedBase{ID: "inner", Kind: "hidden"},
		ID:           "outer",
		Name:         "n",
	})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	if got := row["id"]; got != "outer" {
		t.Errorf("row[id] = %#v, want the outer field's value", got)
	}
	// Only the colliding name is hidden; the rest of the embedded struct is still
	// promoted, as encoding/json does.
	if got := row["kind"]; got != "hidden" {
		t.Errorf("row[kind] = %#v, want the promoted value from the embedded struct", got)
	}
}

func TestToRowWithANilEmbeddedPointer(t *testing.T) {
	t.Parallel()

	s, err := New[ptrEmbedRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("a nil embedded pointer leaves its columns NULL", func(t *testing.T) {
		t.Parallel()
		row, err := s.toRow(ptrEmbedRow{Name: "n"})
		if err != nil {
			t.Fatalf("toRow() error = %v", err)
		}
		for _, column := range []string{"id", "kind"} {
			got, ok := row[column]
			if !ok {
				t.Fatalf("row has no %q column", column)
			}
			if got != nil {
				t.Errorf("row[%q] = %#v, want nil", column, got)
			}
		}
		if got := row["name"]; got != "n" {
			t.Errorf("row[name] = %#v, want n", got)
		}
	})

	t.Run("a set embedded pointer contributes its values", func(t *testing.T) {
		t.Parallel()
		row, err := s.toRow(ptrEmbedRow{embeddedBase: &embeddedBase{ID: "i", Kind: "k"}, Name: "n"})
		if err != nil {
			t.Fatalf("toRow() error = %v", err)
		}
		want := map[string]bigquery.Value{"id": "i", "kind": "k", "name": "n"}
		if !reflect.DeepEqual(row, want) {
			t.Errorf("toRow() = %#v, want %#v", row, want)
		}
	})
}

func TestLockedRowIgnoresTheMutex(t *testing.T) {
	t.Parallel()

	s, err := New[lockedRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(lockedRow{Name: "n"})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	want := map[string]bigquery.Value{"name": "n"}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v; an embedded sync.Mutex must not become a column", row, want)
	}
}
