package bqsink

import (
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// everythingRow exercises every shape the plan has to handle at once, so that one
// assertion can check the schema and the row agree.
type everythingRow struct {
	Str      string    `bqsink:"str"`
	Req      string    `bqsink:"req,required"`
	Nullable *string   `bqsink:"nullable"`
	NullZero string    `bqsink:"null_zero,nullifzero"`
	Num      int32     `bqsink:"num"`
	Rate     float64   `bqsink:"rate"`
	Flag     bool      `bqsink:"flag"`
	Blob     []byte    `bqsink:"blob"`
	At       time.Time `bqsink:"at"`
	Day      civil.Date
	OnDay    time.Time         `bqsink:"on_day,date"`
	OnDayPtr *time.Time        `bqsink:"on_day_ptr,date"`
	OnDays   []time.Time       `bqsink:"on_days,date"`
	LocalAt  time.Time         `bqsink:"local_at,datetime"`
	OpenAt   time.Time         `bqsink:"open_at,time"`
	Money    *big.Rat          `bqsink:"money"`
	Tags     []string          `bqsink:"tags"`
	Inner    nestedRow         `bqsink:"inner,record"`
	InnerPtr *nestedRow        `bqsink:"inner_ptr,record"`
	Records  []nestedRow       `bqsink:"records,record"`
	AsJSON   nestedRow         `bqsink:"as_json"`
	Attrs    map[string]string `bqsink:"attrs"`
	Raw      json.RawMessage   `bqsink:"raw"`
	Declared payload           `bqsink:"declared"`
	External external          `bqsink:"external"`
	Skipped  string            `bqsink:"-"`
	hidden   string
}

func everythingSinker(t *testing.T) *Sinker[everythingRow] {
	t.Helper()
	s, err := New[everythingRow](testClient(t), testRelation(), WithMarshalers(jsonMarshalers()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return s
}

// TestRowKeysMatchTheSchema is the assertion that guards against the schema and
// the row walk drifting apart: every column the schema declares is written, and
// nothing else is.
func TestRowKeysMatchTheSchema(t *testing.T) {
	t.Parallel()

	s := everythingSinker(t)
	row, err := s.toRow(everythingRow{})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}

	var want []string
	for _, f := range s.Schema() {
		want = append(want, f.Name)
	}
	var got []string
	for name := range row {
		got = append(got, name)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("row keys mismatch\n got: %v\nwant: %v", got, want)
	}
}

// timeColumnRow writes one instant into each of the columns a time.Time can take.
type timeColumnRow struct {
	At      time.Time `bqsink:"at"`
	OnDay   time.Time `bqsink:"on_day,date"`
	LocalAt time.Time `bqsink:"local_at,datetime"`
	OpenAt  time.Time `bqsink:"open_at,time"`
}

func TestTimeColumnOptionsDropAComponent(t *testing.T) {
	t.Parallel()

	s, err := New[timeColumnRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	at := time.Date(2026, 7, 28, 22, 30, 15, 500, time.UTC)
	row, err := s.toRow(timeColumnRow{At: at, OnDay: at, LocalAt: at, OpenAt: at})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	day := civil.Date{Year: 2026, Month: time.July, Day: 28}
	clock := civil.Time{Hour: 22, Minute: 30, Second: 15, Nanosecond: 500}
	want := map[string]bigquery.Value{
		"at":       at,
		"on_day":   day,
		"local_at": civil.DateTime{Date: day, Time: clock},
		"open_at":  clock,
	}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v", row, want)
	}
}

func TestTimeColumnsReadTheValuesLocation(t *testing.T) {
	t.Parallel()

	s, err := New[timeColumnRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// 22:30 UTC on the 28th is 07:30 the next morning in Tokyo, so the calendar a
	// column records follows the location the caller hands over. That is why there
	// is no timezone option.
	at := time.Date(2026, 7, 28, 22, 30, 0, 0, time.UTC)
	jst := time.FixedZone("JST", 9*60*60)

	tests := []struct {
		name string
		at   time.Time
		want civil.Date
	}{
		{name: "UTC", at: at, want: civil.Date{Year: 2026, Month: time.July, Day: 28}},
		{name: "the same instant in Tokyo", at: at.In(jst), want: civil.Date{Year: 2026, Month: time.July, Day: 29}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			row, err := s.toRow(timeColumnRow{OnDay: tt.at})
			if err != nil {
				t.Fatalf("toRow() error = %v", err)
			}
			if got := row["on_day"]; got != bigquery.Value(tt.want) {
				t.Errorf("on_day = %v, want %v", got, tt.want)
			}
		})
	}
}

// repeatedTimeColumnRow shows the option reaching through a slice and a pointer.
type repeatedTimeColumnRow struct {
	Days   []time.Time `bqsink:"days,date"`
	DayPtr *time.Time  `bqsink:"day_ptr,date"`
}

func TestTimeColumnOptionsReachThroughSlicesAndPointers(t *testing.T) {
	t.Parallel()

	s, err := New[repeatedTimeColumnRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	second := time.Date(2026, 7, 29, 23, 0, 0, 0, time.UTC)
	row, err := s.toRow(repeatedTimeColumnRow{Days: []time.Time{first, second}, DayPtr: &first})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	want := map[string]bigquery.Value{
		"days": []bigquery.Value{
			civil.Date{Year: 2026, Month: time.July, Day: 28},
			civil.Date{Year: 2026, Month: time.July, Day: 29},
		},
		"day_ptr": civil.Date{Year: 2026, Month: time.July, Day: 28},
	}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v", row, want)
	}
}

// zeroTimeColumnRow pairs a DATE column with and without "nullifzero", since the
// zero time.Time is a date BigQuery accepts rather than something that has to be
// treated as absent.
type zeroTimeColumnRow struct {
	Kept    time.Time  `bqsink:"kept,date"`
	Dropped time.Time  `bqsink:"dropped,date,nullifzero"`
	Missing *time.Time `bqsink:"missing,date"`
}

func TestZeroTimeInADateColumn(t *testing.T) {
	t.Parallel()

	s, err := New[zeroTimeColumnRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(zeroTimeColumnRow{})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	want := map[string]bigquery.Value{
		"kept":    civil.Date{Year: 1, Month: time.January, Day: 1},
		"dropped": nil,
		"missing": nil,
	}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v", row, want)
	}
}

type dateOnStringRow struct {
	A string `bqsink:"a,date"`
}

// namedTime is not time.Time as far as the plan is concerned: the lookup is by
// exact type, so this is a JSON column and the option cannot describe it.
type namedTime time.Time

type dateOnNamedTimeRow struct {
	A namedTime `bqsink:"a,date"`
}

type twoTimeTypesRow struct {
	A time.Time `bqsink:"a,date,datetime"`
}

type dateAndRecordRow struct {
	A time.Time `bqsink:"a,date,record"`
}

func TestTimeColumnOptionsAreValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(...*Marshalers) (bigquery.Schema, error)
		want string
	}{
		{
			name: `"date" on a string`,
			fn:   InferSchema[dateOnStringRow],
			want: "needs a time.Time",
		},
		{
			name: `"date" on a named type whose underlying type is time.Time`,
			fn:   InferSchema[dateOnNamedTimeRow],
			want: "needs a time.Time",
		},
		{
			name: "two options naming the type",
			fn:   InferSchema[twoTimeTypesRow],
			want: "a column has one type",
		},
		{
			name: `"date" together with "record"`,
			fn:   InferSchema[dateAndRecordRow],
			want: "a column has one type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.fn()
			if err == nil {
				t.Fatalf("error = nil, want one mentioning %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestTimeColumnOptionConflictingWithAMarshalerIsRejected(t *testing.T) {
	t.Parallel()

	// A marshaler registered for time.Time claims every time.Time column, so the
	// option and the registration disagree about this one. Reporting that beats
	// letting either win silently.
	m := MarshalFunc(bigquery.StringFieldType, func(v time.Time) (bigquery.Value, error) {
		return v.Format(time.RFC3339), nil
	})
	_, err := InferSchema[timeColumnRow](m)
	if err == nil {
		t.Fatal("InferSchema() error = nil, want the contradiction to be rejected")
	}
	if !strings.Contains(err.Error(), "drop one of them") {
		t.Errorf("InferSchema() error = %v, want it to say the two disagree", err)
	}
}

func TestSchemaOfEverythingRow(t *testing.T) {
	t.Parallel()

	s := everythingSinker(t)
	nested := bigquery.Schema{
		{Name: "A", Type: bigquery.StringFieldType},
		{Name: "B", Type: bigquery.IntegerFieldType},
	}
	want := bigquery.Schema{
		{Name: "str", Type: bigquery.StringFieldType},
		{Name: "req", Type: bigquery.StringFieldType, Required: true},
		{Name: "nullable", Type: bigquery.StringFieldType},
		{Name: "null_zero", Type: bigquery.StringFieldType},
		{Name: "num", Type: bigquery.IntegerFieldType},
		{Name: "rate", Type: bigquery.FloatFieldType},
		{Name: "flag", Type: bigquery.BooleanFieldType},
		{Name: "blob", Type: bigquery.BytesFieldType},
		{Name: "at", Type: bigquery.TimestampFieldType},
		{Name: "Day", Type: bigquery.DateFieldType},
		{Name: "on_day", Type: bigquery.DateFieldType},
		{Name: "on_day_ptr", Type: bigquery.DateFieldType},
		{Name: "on_days", Type: bigquery.DateFieldType, Repeated: true},
		{Name: "local_at", Type: bigquery.DateTimeFieldType},
		{Name: "open_at", Type: bigquery.TimeFieldType},
		{Name: "money", Type: bigquery.NumericFieldType},
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "inner", Type: bigquery.RecordFieldType, Schema: nested},
		{Name: "inner_ptr", Type: bigquery.RecordFieldType, Schema: nested},
		{Name: "records", Type: bigquery.RecordFieldType, Repeated: true, Schema: nested},
		{Name: "as_json", Type: bigquery.JSONFieldType},
		{Name: "attrs", Type: bigquery.JSONFieldType},
		{Name: "raw", Type: bigquery.JSONFieldType},
		{Name: "declared", Type: bigquery.JSONFieldType},
		{Name: "external", Type: bigquery.JSONFieldType},
	}
	if !reflect.DeepEqual(s.Schema(), want) {
		t.Errorf("Schema() mismatch\n got: %s\nwant: %s", formatSchema(s.Schema()), formatSchema(want))
	}
}

func TestToRowValues(t *testing.T) {
	t.Parallel()

	s := everythingSinker(t)
	str := "pointed at"
	at := time.Date(2026, 7, 28, 12, 30, 0, 0, time.UTC)
	money := big.NewRat(25, 2)
	row, err := s.toRow(everythingRow{
		Str:      "plain",
		Req:      "needed",
		Nullable: &str,
		NullZero: "not zero",
		Num:      42,
		Rate:     1.5,
		Flag:     true,
		Blob:     []byte("bytes"),
		At:       at,
		Day:      civil.Date{Year: 2026, Month: time.July, Day: 28},
		Money:    money,
		Tags:     []string{"a", "b"},
		Inner:    nestedRow{A: "inner a", B: 1},
		InnerPtr: &nestedRow{A: "ptr a", B: 2},
		Records:  []nestedRow{{A: "r1", B: 3}, {A: "r2", B: 4}},
		AsJSON:   nestedRow{A: "json a", B: 5},
		Attrs:    map[string]string{"url": "https://x/?a=1&b=2"},
		Raw:      json.RawMessage(`{"already":"json"}`),
		Declared: payload{Data: map[string]string{"k": "v"}},
		External: external{Amount: "12.5"},
		Skipped:  "dropped",
	})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}

	tests := []struct {
		column string
		want   bigquery.Value
	}{
		{column: "str", want: "plain"},
		{column: "req", want: "needed"},
		{column: "nullable", want: "pointed at"},
		{column: "null_zero", want: "not zero"},
		{column: "num", want: int64(42)},
		{column: "rate", want: 1.5},
		{column: "flag", want: true},
		{column: "at", want: at},
		{column: "Day", want: civil.Date{Year: 2026, Month: time.July, Day: 28}},
		{column: "declared", want: `{"k":"v"}`},
		{column: "external", want: `{"Amount":"12.5"}`},
		{column: "as_json", want: `{"A":"json a","B":5}`},
		{column: "raw", want: `{"already":"json"}`},
	}
	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			t.Parallel()
			if got := row[tt.column]; got != tt.want {
				t.Errorf("row[%q] = %#v, want %#v", tt.column, got, tt.want)
			}
		})
	}

	t.Run("bytes keep their contents", func(t *testing.T) {
		t.Parallel()
		if got, ok := row["blob"].([]byte); !ok || string(got) != "bytes" {
			t.Errorf("row[blob] = %#v, want []byte(\"bytes\")", row["blob"])
		}
	})

	t.Run("NUMERIC stays a *big.Rat", func(t *testing.T) {
		t.Parallel()
		got, ok := row["money"].(*big.Rat)
		if !ok {
			t.Fatalf("row[money] = %#v, want a *big.Rat", row["money"])
		}
		if got.Cmp(money) != 0 {
			t.Errorf("row[money] = %s, want %s", got, money)
		}
	})

	t.Run("a repeated column becomes a slice of values", func(t *testing.T) {
		t.Parallel()
		want := []bigquery.Value{"a", "b"}
		if !reflect.DeepEqual(row["tags"], want) {
			t.Errorf("row[tags] = %#v, want %#v", row["tags"], want)
		}
	})

	t.Run("a RECORD becomes a nested map", func(t *testing.T) {
		t.Parallel()
		want := map[string]bigquery.Value{"A": "inner a", "B": int64(1)}
		if !reflect.DeepEqual(row["inner"], want) {
			t.Errorf("row[inner] = %#v, want %#v", row["inner"], want)
		}
	})

	t.Run("a repeated RECORD becomes a slice of maps", func(t *testing.T) {
		t.Parallel()
		want := []bigquery.Value{
			map[string]bigquery.Value{"A": "r1", "B": int64(3)},
			map[string]bigquery.Value{"A": "r2", "B": int64(4)},
		}
		if !reflect.DeepEqual(row["records"], want) {
			t.Errorf("row[records] = %#v, want %#v", row["records"], want)
		}
	})

	t.Run("a skipped field is absent", func(t *testing.T) {
		t.Parallel()
		if _, ok := row["Skipped"]; ok {
			t.Error(`row has a "Skipped" column, want the field dropped by the "-" tag`)
		}
	})

	t.Run("a JSON column is not HTML escaped", func(t *testing.T) {
		t.Parallel()
		want := `{"url":"https://x/?a=1&b=2"}`
		if got := row["attrs"]; got != want {
			t.Errorf("row[attrs] = %#v, want %s; & must not become \\u0026", got, want)
		}
	})
}

func TestToRowNullHandling(t *testing.T) {
	t.Parallel()

	s := everythingSinker(t)
	row, err := s.toRow(everythingRow{})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}

	tests := []struct {
		name   string
		column string
		want   bigquery.Value
	}{
		{name: "a nil pointer becomes NULL", column: "nullable", want: nil},
		{name: "a nil *big.Rat becomes NULL", column: "money", want: nil},
		{name: "a nil pointer to struct becomes NULL", column: "inner_ptr", want: nil},
		{name: "nullifzero turns the zero string into NULL", column: "null_zero", want: nil},
		{name: "a plain zero string stays an empty string", column: "str", want: ""},
		{name: "a zero number stays zero", column: "num", want: int64(0)},
		{name: "a zero bool stays false", column: "flag", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := row[tt.column]
			if !ok {
				t.Fatalf("row has no %q column", tt.column)
			}
			if got != tt.want {
				t.Errorf("row[%q] = %#v, want %#v", tt.column, got, tt.want)
			}
		})
	}

	t.Run("a nil slice becomes an empty array, not NULL", func(t *testing.T) {
		t.Parallel()
		got, ok := row["tags"].([]bigquery.Value)
		if !ok {
			t.Fatalf("row[tags] = %#v, want a []bigquery.Value", row["tags"])
		}
		if len(got) != 0 {
			t.Errorf("row[tags] = %#v, want an empty array; BigQuery has no NULL for a REPEATED column", got)
		}
	})

	t.Run("a zero time stays a zero time", func(t *testing.T) {
		t.Parallel()
		got, ok := row["at"].(time.Time)
		if !ok {
			t.Fatalf("row[at] = %#v, want a time.Time", row["at"])
		}
		if !got.IsZero() {
			t.Errorf("row[at] = %v, want the zero time", got)
		}
	})
}

// zeroTimeRow checks that nullifzero uses IsZero, which is what makes a zero
// time.Time recognisable.
type zeroTimeRow struct {
	At    time.Time  `bqsink:"at,nullifzero"`
	AtPtr *time.Time `bqsink:"at_ptr,nullifzero"`
	Num   int64      `bqsink:"num,nullifzero"`
}

// repeatedNullIfZeroRow uses nullifzero on a repeated column, where it means
// "no elements".
type repeatedNullIfZeroRow struct {
	Tags []string `bqsink:"tags,nullifzero"`
}

func TestNullIfZeroUsesIsZero(t *testing.T) {
	t.Parallel()

	s, err := New[zeroTimeRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("a zero time and a zero number become NULL", func(t *testing.T) {
		t.Parallel()
		row, err := s.toRow(zeroTimeRow{})
		if err != nil {
			t.Fatalf("toRow() error = %v", err)
		}
		for _, column := range []string{"at", "at_ptr", "num"} {
			if got := row[column]; got != nil {
				t.Errorf("row[%q] = %#v, want nil", column, got)
			}
		}
	})

	t.Run("set values survive", func(t *testing.T) {
		t.Parallel()
		at := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
		row, err := s.toRow(zeroTimeRow{At: at, AtPtr: &at, Num: 5})
		if err != nil {
			t.Fatalf("toRow() error = %v", err)
		}
		if got := row["at"]; got != at {
			t.Errorf("row[at] = %#v, want %v", got, at)
		}
		if got := row["at_ptr"]; got != at {
			t.Errorf("row[at_ptr] = %#v, want %v", got, at)
		}
		if got := row["num"]; got != int64(5) {
			t.Errorf("row[num] = %#v, want 5", got)
		}
	})
}

func TestNullIfZeroOnARepeatedColumn(t *testing.T) {
	t.Parallel()

	s, err := New[repeatedNullIfZeroRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		name string
		tags []string
		want bigquery.Value
	}{
		{name: "a nil slice becomes NULL", tags: nil, want: nil},
		{name: "an empty slice becomes NULL too", tags: []string{}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			row, err := s.toRow(repeatedNullIfZeroRow{Tags: tt.tags})
			if err != nil {
				t.Fatalf("toRow() error = %v", err)
			}
			if got := row["tags"]; got != tt.want {
				t.Errorf("row[tags] = %#v, want %#v", got, tt.want)
			}
		})
	}

	t.Run("a non-empty slice survives", func(t *testing.T) {
		t.Parallel()
		row, err := s.toRow(repeatedNullIfZeroRow{Tags: []string{"x"}})
		if err != nil {
			t.Fatalf("toRow() error = %v", err)
		}
		if got := row["tags"]; !reflect.DeepEqual(got, []bigquery.Value{"x"}) {
			t.Errorf("row[tags] = %#v, want [x]", got)
		}
	})
}

func TestToRowFollowsPointerTypeParameter(t *testing.T) {
	t.Parallel()

	s, err := New[*nestedRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(&nestedRow{A: "a", B: 7})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	want := map[string]bigquery.Value{"A": "a", "B": int64(7)}
	if !reflect.DeepEqual(row, want) {
		t.Errorf("toRow() = %#v, want %#v", row, want)
	}
}

func TestToRowRejectsANilRow(t *testing.T) {
	t.Parallel()

	s, err := New[*nestedRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := s.toRow(nil); err == nil {
		t.Fatal("toRow(nil) error = nil, want a rejection")
	}
}

// failingMarshalRow carries a FieldMarshaler that always fails.
type failingMarshalRow struct {
	Bad failingValue
}

type failingValue struct{}

var errMarshalFailed = errors.New("marshal failed")

func (failingValue) BigQueryFieldType() bigquery.FieldType { return bigquery.StringFieldType }

func (failingValue) MarshalBigQueryValue() (bigquery.Value, error) { return nil, errMarshalFailed }

func TestToRowPropagatesMarshalErrors(t *testing.T) {
	t.Parallel()

	s, err := New[failingMarshalRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := s.toRow(failingMarshalRow{}); !errors.Is(err, errMarshalFailed) {
		t.Errorf("toRow() error = %v, want one wrapping the marshaler's failure", err)
	}
}

func TestSinkWritesThroughTheWriteStrategy(t *testing.T) {
	t.Parallel()

	fake := &fakeTable{metadata: &bigquery.TableMetadata{
		ETag: "etag-1",
		Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.StringFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
		},
	}}
	writer := &fakeRowWriter{}
	s := newTestSinker[nestedRow](t, fake,
		WithMigration(AppendNewColumns{}),
		WithWriteStrategy(&fakeWriteStrategy{writer: writer}),
	)

	ctx := t.Context()
	if err := s.Sink(ctx, nestedRow{A: "one", B: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if err := s.SinkAll(ctx, nestedRow{A: "two", B: 2}, nestedRow{A: "three", B: 3}); err != nil {
		t.Fatalf("SinkAll() error = %v", err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	want := []map[string]bigquery.Value{
		{"A": "one", "B": int64(1)},
		{"A": "two", "B": int64(2)},
		{"A": "three", "B": int64(3)},
	}
	if !reflect.DeepEqual(writer.rows, want) {
		t.Errorf("appended rows = %#v, want %#v", writer.rows, want)
	}
	if writer.flushes != 1 {
		t.Errorf("Flush was called %d times, want 1", writer.flushes)
	}
	if writer.closes != 1 {
		t.Errorf("Close was called %d times, want 1", writer.closes)
	}
	if writer.opens != 1 {
		t.Errorf("Open was called %d times, want 1; the writer must be reused", writer.opens)
	}
	if !reflect.DeepEqual(writer.openedSchema, s.Schema()) {
		t.Errorf("Open received %s, want the declared %s", formatSchema(writer.openedSchema), formatSchema(s.Schema()))
	}
}

// unsignedValueRow checks that a uint64 too large for INT64 survives as NUMERIC.
type unsignedValueRow struct {
	Small uint32 `bqsink:"small"`
	Large uint64 `bqsink:"large"`
}

func TestToRowUnsignedValues(t *testing.T) {
	t.Parallel()

	s, err := New[unsignedValueRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	row, err := s.toRow(unsignedValueRow{Small: 4294967295, Large: math.MaxUint64})
	if err != nil {
		t.Fatalf("toRow() error = %v", err)
	}
	if got := row["small"]; got != int64(4294967295) {
		t.Errorf("row[small] = %#v, want an int64", got)
	}
	rat, ok := row["large"].(*big.Rat)
	if !ok {
		t.Fatalf("row[large] = %#v, want a *big.Rat so that NUMERIC can hold it", row["large"])
	}
	if want := "18446744073709551615"; rat.RatString() != want {
		t.Errorf("row[large] = %s, want %s", rat.RatString(), want)
	}
}
