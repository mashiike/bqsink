package bqsink

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// layoutRow carries the physical layout in tag keys of its own.
type layoutRow struct {
	Timestamp time.Time `bqsink:"timestamp,required" partition:"day"`
	UserID    string    `bqsink:"user_id" cluster:"1"`
	Region    string    `bqsink:"region" cluster:"2"`
	Amount    *big.Rat  `bqsink:"amount" description:"billed amount, including tax"`
}

func planOf[T any](t *testing.T) *rowPlan {
	t.Helper()
	plan, err := buildRowPlan(reflect.TypeFor[T](), nil)
	if err != nil {
		t.Fatalf("buildRowPlan() error = %v", err)
	}
	return plan
}

func TestLayoutFromTags(t *testing.T) {
	t.Parallel()

	plan := planOf[layoutRow](t)

	if plan.partitioning == nil {
		t.Fatal("partitioning = nil, want the tagged column")
	}
	if got, want := plan.partitioning.Field, "timestamp"; got != want {
		t.Errorf("partitioning field = %q, want %q", got, want)
	}
	if got, want := plan.partitioning.Type, bigquery.DayPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
	if plan.requireFilter {
		t.Error("requireFilter is set without the option asking for it")
	}
	if plan.clustering == nil {
		t.Fatal("clustering = nil, want the tagged columns")
	}
	want := []string{"user_id", "region"}
	if !reflect.DeepEqual(plan.clustering.Fields, want) {
		t.Errorf("clustering fields = %v, want %v in tag position order", plan.clustering.Fields, want)
	}
}

func TestDescriptionReachesTheSchema(t *testing.T) {
	t.Parallel()

	schema, err := inferSchema[layoutRow]()
	if err != nil {
		t.Fatalf("inferSchema() error = %v", err)
	}
	var amount *bigquery.FieldSchema
	for _, f := range schema {
		if f.Name == "amount" {
			amount = f
		}
	}
	if amount == nil {
		t.Fatal("the schema has no amount column")
	}
	// A comma in the text is why description is a tag key of its own.
	if want := "billed amount, including tax"; amount.Description != want {
		t.Errorf("description = %q, want %q", amount.Description, want)
	}
}

// clusterOrderRow declares its clustering columns out of order, to show that the
// position comes from the tag rather than the declaration.
type clusterOrderRow struct {
	Second string `bqsink:"second" cluster:"2"`
	First  string `bqsink:"first" cluster:"1"`
	Third  string `bqsink:"third" cluster:"3"`
}

func TestClusterPositionsComeFromTheTag(t *testing.T) {
	t.Parallel()

	plan := planOf[clusterOrderRow](t)
	want := []string{"first", "second", "third"}
	if !reflect.DeepEqual(plan.clustering.Fields, want) {
		t.Errorf("clustering fields = %v, want %v", plan.clustering.Fields, want)
	}
}

type partitionGranularityRow struct {
	At time.Time `bqsink:"at" partition:"hour,require"`
}

func TestPartitionGranularityAndRequire(t *testing.T) {
	t.Parallel()

	plan := planOf[partitionGranularityRow](t)
	if got, want := plan.partitioning.Type, bigquery.HourPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
	if !plan.requireFilter {
		t.Error("requireFilter is not set, want the require option to set it")
	}
}

type defaultGranularityRow struct {
	At time.Time `bqsink:"at" partition:""`
}

func TestEmptyPartitionTagMeansDay(t *testing.T) {
	t.Parallel()

	plan := planOf[defaultGranularityRow](t)
	if got, want := plan.partitioning.Type, bigquery.DayPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
}

// taggedTimeColumnLayoutRow partitions and clusters on columns whose type comes
// from a tag rather than from the Go type.
type taggedTimeColumnLayoutRow struct {
	Day     time.Time `bqsink:"day,date" partition:"month"`
	LocalAt time.Time `bqsink:"local_at,datetime" cluster:"1"`
}

func TestTaggedTimeColumnsCanCarryTheLayout(t *testing.T) {
	t.Parallel()

	plan := planOf[taggedTimeColumnLayoutRow](t)
	if plan.partitioning == nil {
		t.Fatal("partitioning = nil, want the DATE column the tag asked for")
	}
	if got, want := plan.partitioning.Field, "day"; got != want {
		t.Errorf("partitioning field = %q, want %q", got, want)
	}
	if got, want := plan.partitioning.Type, bigquery.MonthPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
	if plan.clustering == nil || !reflect.DeepEqual(plan.clustering.Fields, []string{"local_at"}) {
		t.Errorf("clustering = %#v, want the DATETIME column", plan.clustering)
	}
}

// hourlyTaggedDateTimeRow is the granularity a DATE column cannot carry on a
// column the tag made DATETIME, which BigQuery does allow. Asserting the positive
// keeps the check from over-firing on the whole option.
type hourlyTaggedDateTimeRow struct {
	A time.Time `bqsink:"a,datetime" partition:"hour"`
}

func TestHourlyPartitioningOnATaggedDateTimeColumn(t *testing.T) {
	t.Parallel()

	plan := planOf[hourlyTaggedDateTimeRow](t)
	if got, want := plan.partitioning.Type, bigquery.HourPartitioningType; got != want {
		t.Errorf("partitioning type = %s, want %s", got, want)
	}
	if got, want := plan.fields[0].fieldType, bigquery.DateTimeFieldType; got != want {
		t.Errorf("column type = %s, want %s", got, want)
	}
}

// Each of these asks for something BigQuery rejects, which was confirmed by asking
// it to create such a table.
type twoPartitionsRow struct {
	A time.Time `bqsink:"a" partition:"day"`
	B time.Time `bqsink:"b" partition:"day"`
}

type stringPartitionRow struct {
	A string `bqsink:"a" partition:"day"`
}

type integerPartitionRow struct {
	A int64 `bqsink:"a" partition:"day"`
}

type hourlyDateRow struct {
	A civil.Date `bqsink:"a" partition:"hour"`
}

// The column type the "date" and "time" options ask for is what the partitioning
// checks read, so tagging a time.Time is constrained exactly as the civil types are.
type hourlyTaggedDateRow struct {
	A time.Time `bqsink:"a,date" partition:"hour"`
}

type dailyTaggedTimeRow struct {
	A time.Time `bqsink:"a,time" partition:"day"`
}

type repeatedPartitionRow struct {
	A []time.Time `bqsink:"a" partition:"day"`
}

type fiveClusterRow struct {
	A string `bqsink:"a" cluster:"1"`
	B string `bqsink:"b" cluster:"2"`
	C string `bqsink:"c" cluster:"3"`
	D string `bqsink:"d" cluster:"4"`
	E string `bqsink:"e" cluster:"5"`
}

type duplicateClusterRow struct {
	A string `bqsink:"a" cluster:"1"`
	B string `bqsink:"b" cluster:"1"`
}

type gappedClusterRow struct {
	A string `bqsink:"a" cluster:"1"`
	B string `bqsink:"b" cluster:"3"`
}

type floatClusterRow struct {
	A float64 `bqsink:"a" cluster:"1"`
}

type jsonClusterRow struct {
	A map[string]string `bqsink:"a" cluster:"1"`
}

type bytesClusterRow struct {
	A []byte `bqsink:"a" cluster:"1"`
}

type repeatedClusterRow struct {
	A []string `bqsink:"a" cluster:"1"`
}

type badGranularityRow struct {
	A time.Time `bqsink:"a" partition:"fortnight"`
}

type badClusterPositionRow struct {
	A string `bqsink:"a" cluster:"zero"`
}

type unknownPartitionOptionRow struct {
	A time.Time `bqsink:"a" partition:"day,nope"`
}

func TestLayoutIsValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(...*Marshalers) (bigquery.Schema, error)
		want string
	}{
		{
			name: "two partitioning columns",
			fn:   inferSchema[twoPartitionsRow],
			want: "one partitioning column",
		},
		{
			name: "a STRING partitioning column",
			fn:   inferSchema[stringPartitionRow],
			want: "TIMESTAMP, DATE or DATETIME",
		},
		{
			name: "an INTEGER partitioning column",
			fn:   inferSchema[integerPartitionRow],
			want: "TIMESTAMP, DATE or DATETIME",
		},
		{
			name: "hourly partitioning on a DATE column",
			fn:   inferSchema[hourlyDateRow],
			want: "cannot be partitioned by hour",
		},
		{
			name: `hourly partitioning on a time.Time tagged "date"`,
			fn:   inferSchema[hourlyTaggedDateRow],
			want: "cannot be partitioned by hour",
		},
		{
			name: `partitioning on a time.Time tagged "time"`,
			fn:   inferSchema[dailyTaggedTimeRow],
			want: "TIMESTAMP, DATE or DATETIME",
		},
		{
			name: "a repeated partitioning column",
			fn:   inferSchema[repeatedPartitionRow],
			want: "repeated",
		},
		{
			name: "five clustering columns",
			fn:   inferSchema[fiveClusterRow],
			want: "position from 1 to 4",
		},
		{
			name: "two columns at the same position",
			fn:   inferSchema[duplicateClusterRow],
			want: "both claim",
		},
		{
			name: "a gap in the positions",
			fn:   inferSchema[gappedClusterRow],
			want: "without a gap",
		},
		{
			name: "clustering on FLOAT",
			fn:   inferSchema[floatClusterRow],
			want: "cannot cluster on",
		},
		{
			name: "clustering on JSON",
			fn:   inferSchema[jsonClusterRow],
			want: "cannot cluster on",
		},
		{
			name: "clustering on BYTES",
			fn:   inferSchema[bytesClusterRow],
			want: "cannot cluster on",
		},
		{
			name: "clustering on a repeated column",
			fn:   inferSchema[repeatedClusterRow],
			want: "repeated",
		},
		{
			name: "an unknown granularity",
			fn:   inferSchema[badGranularityRow],
			want: "want day, hour, month or year",
		},
		{
			name: "a cluster position that is not a number",
			fn:   inferSchema[badClusterPositionRow],
			want: "want a position",
		},
		{
			name: "an unknown partition option",
			fn:   inferSchema[unknownPartitionOptionRow],
			want: "unknown option",
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

func TestNewAppliesTheTaggedLayout(t *testing.T) {
	t.Parallel()

	s, err := New[layoutRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.metadata == nil {
		t.Fatal("metadata = nil, want the layout from the tags")
	}
	if got := s.metadata.TimePartitioning; got == nil || got.Field != "timestamp" {
		t.Errorf("TimePartitioning = %#v, want the timestamp column", got)
	}
	want := []string{"user_id", "region"}
	if got := s.metadata.Clustering; got == nil || !reflect.DeepEqual(got.Fields, want) {
		t.Errorf("Clustering = %#v, want %v", got, want)
	}
}

// metadataConflictRow tags a layout that its own BigQueryTableMetadata also sets.
type metadataConflictRow struct {
	At time.Time `bqsink:"at" partition:"day"`
}

func (metadataConflictRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		TimePartitioning: &bigquery.TimePartitioning{Field: "at", Type: bigquery.DayPartitioningType},
	}
}

// TestTaggedLayoutConflictingWithMetadataIsRejected drives resolveTableMetadata
// rather than New, since the metadata now comes from a method on the row type and
// one type cannot stand for several contradictions at once. The case a row type
// really can reach is covered end to end below.
func TestTaggedLayoutConflictingWithMetadataIsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		md   *bigquery.TableMetadata
	}{
		{
			name: "TimePartitioning set both ways",
			md: &bigquery.TableMetadata{
				TimePartitioning: &bigquery.TimePartitioning{Field: "at", Type: bigquery.DayPartitioningType},
			},
		},
		{
			name: "RangePartitioning against a tagged partition",
			md: &bigquery.TableMetadata{
				RangePartitioning: &bigquery.RangePartitioning{Field: "at"},
			},
		},
		{
			name: "Clustering set both ways",
			md: &bigquery.TableMetadata{
				Clustering: &bigquery.Clustering{Fields: []string{"second"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan := planOf[clusteredConflictRow](t)
			_, err := resolveTableMetadata(tt.md, plan)
			if err == nil {
				t.Fatal("resolveTableMetadata() error = nil, want the contradiction to be rejected")
			}
			if !strings.Contains(err.Error(), "drop one of them") {
				t.Errorf("resolveTableMetadata() error = %v, want it to say the two disagree", err)
			}
		})
	}
}

// clusteredConflictRow tags both a partition and a clustering key, so that a
// single plan contradicts whichever of the two a metadata sets.
type clusteredConflictRow struct {
	At     time.Time `bqsink:"at" partition:"day"`
	Second string    `bqsink:"second" cluster:"1"`
}

func TestNewRejectsATypeContradictingItself(t *testing.T) {
	t.Parallel()

	_, err := New[metadataConflictRow](testClient(t), testRelation())
	if err == nil {
		t.Fatal("New() error = nil, want the contradiction between the tag and the method to be rejected")
	}
	if !strings.Contains(err.Error(), "drop one of them") {
		t.Errorf("New() error = %v, want it to say the two disagree", err)
	}
}

// tableMetaRow says what the table is without a BigQueryTableMetadata method.
type tableMetaRow struct {
	TableMeta `description:"one row per request, including tax" labels:"team=data,env=prod,owner="`

	UserID string `bqsink:"user_id"`
}

func TestTableMetaDescribesTheTable(t *testing.T) {
	t.Parallel()

	s, err := New[tableMetaRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.metadata == nil {
		t.Fatal("metadata = nil, want the one the embedded TableMeta describes")
	}
	if got, want := s.metadata.Description, "one row per request, including tax"; got != want {
		t.Errorf("Description = %q, want %q", got, want)
	}
	want := map[string]string{"team": "data", "env": "prod", "owner": ""}
	if got := s.metadata.Labels; !reflect.DeepEqual(got, want) {
		t.Errorf("Labels = %v, want %v", got, want)
	}
}

func TestTableMetaContributesNoColumn(t *testing.T) {
	t.Parallel()

	s, err := New[tableMetaRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	want := bigquery.Schema{{Name: "user_id", Type: bigquery.StringFieldType}}
	if got := s.Schema(); !reflect.DeepEqual(got, want) {
		t.Errorf("Schema() = %s, want %s", formatSchema(got), formatSchema(want))
	}
}

// tableMetaConflictRow says the same thing twice, once in a tag and once in the
// method.
type tableMetaConflictRow struct {
	TableMeta `description:"tagged"`

	UserID string `bqsink:"user_id"`
}

func (tableMetaConflictRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Description: "from the method"}
}

// labelConflictRow tags labels the method also sets.
type labelConflictRow struct {
	TableMeta `labels:"team=data"`

	UserID string `bqsink:"user_id"`
}

func (labelConflictRow) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{Labels: map[string]string{"env": "prod"}}
}

func TestTableMetaContradictingTheMethodIsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*bigquery.Client) error
	}{
		{
			name: "a description set both ways",
			fn: func(c *bigquery.Client) error {
				_, err := New[tableMetaConflictRow](c, testRelation())
				return err
			},
		},
		{
			name: "labels set both ways",
			fn: func(c *bigquery.Client) error {
				_, err := New[labelConflictRow](c, testRelation())
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.fn(testClient(t))
			if err == nil {
				t.Fatal("New() error = nil, want the contradiction to be rejected")
			}
			if !strings.Contains(err.Error(), "drop one of them") {
				t.Errorf("New() error = %v, want it to say the two disagree", err)
			}
		})
	}
}

func TestTableMetaRejectsColumnTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tag  reflect.StructTag
	}{
		{name: "the column tag", tag: `bqsink:"meta"`},
		{name: "a partition tag", tag: `partition:"day"`},
		{name: "a cluster tag", tag: `cluster:"1"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// The type is built here rather than declared, so that one case does not
			// need a named type of its own.
			rowType := reflect.StructOf([]reflect.StructField{
				{Name: "TableMeta", Type: reflect.TypeFor[TableMeta](), Tag: tt.tag, Anonymous: true},
				{Name: "UserID", Type: reflect.TypeFor[string](), Tag: `bqsink:"user_id"`},
			})
			_, err := buildRowPlan(rowType, nil)
			if err == nil {
				t.Fatal("buildRowPlan() error = nil, want the column tag on TableMeta to be rejected")
			}
			if !strings.Contains(err.Error(), "TableMeta is not one") {
				t.Errorf("buildRowPlan() error = %v, want it to say TableMeta is no column", err)
			}
		})
	}
}

func TestLabelsTagIsCheckedForShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "a pair with no equals sign", value: "team", want: "want key=value"},
		{name: "an empty key", value: "=data", want: "whose key is empty"},
		{name: "the same key twice", value: "team=data,team=other", want: "twice"},
		{name: "an empty tag", value: "", want: "want key=value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseLabels(tt.value)
			if err == nil {
				t.Fatalf("parseLabels(%q) error = nil, want a rejection", tt.value)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("parseLabels(%q) error = %v, want it to mention %q", tt.value, err, tt.want)
			}
		})
	}
}

// nestedTableMetaRow reaches a TableMeta through another embedded struct, which is
// deliberately not searched.
type nestedTableMetaRow struct {
	tableMetaCarrier

	UserID string `bqsink:"user_id"`
}

type tableMetaCarrier struct {
	TableMeta `description:"not read from here"`
}

func TestTableMetaIsOnlyReadFromADirectField(t *testing.T) {
	t.Parallel()

	s, err := New[nestedTableMetaRow](testClient(t), testRelation())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s.metadata != nil {
		t.Errorf("metadata = %+v, want nil for a TableMeta reached through an embedded struct", s.metadata)
	}
}

// A dropped field carries no column, so nothing else about it can apply.
type skippedLayoutRow struct {
	At   time.Time `bqsink:"-" partition:"day"`
	Name string    `bqsink:"name"`
}

func TestASkippedFieldContributesNoLayout(t *testing.T) {
	t.Parallel()

	plan := planOf[skippedLayoutRow](t)
	if plan.partitioning != nil {
		t.Errorf("partitioning = %#v, want nil for a dropped field", plan.partitioning)
	}
}
