# bqsink

Expressive BigQuery ingestion library for Go.

bqsink writes rows to BigQuery and keeps the destination table's schema in sync
with a schema declared in Go code.

**The declaration is the source of truth.** The schema comes from struct tags or an
explicit `bigquery.Schema`, and the real table follows it. bqsink never infers a
schema from the data being written, so a field added to a struct becomes a column
rather than a silent mismatch at write time.

```go
type AccessLog struct {
	Timestamp time.Time `bqsink:"timestamp,required"`
	UserID    string    `bqsink:"user_id"`
	Path      string    `bqsink:"path"`
}

client, err := bigquery.NewClient(ctx, "my-project")
if err != nil {
	return err
}

s, err := bqsink.New[AccessLog](client, bqsink.Relation{
	DatasetID: "logs",
	TableID:   "access",
})
if err != nil {
	return err
}
defer func() {
	if cerr := s.Close(ctx); cerr != nil && err == nil {
		err = cerr
	}
}()

if err := s.Sink(ctx, AccessLog{Timestamp: time.Now(), UserID: "u1", Path: "/"}); err != nil {
	return err
}
return s.Flush(ctx)
```

`Close` and `Flush` report rows that never reached BigQuery, so a bare
`defer s.Close(ctx)` loses them silently. Capture the error as above.

## Declaring the schema

Struct tags describe the columns. Columns are **NULLABLE by default**, unlike
`bigquery.InferSchema`.

```go
type Row struct {
	ID       string            `bqsink:"id,required"`     // REQUIRED
	Name     string            `bqsink:"name"`            // NULLABLE
	Seen     time.Time         `bqsink:"seen,nullifzero"` // zero value becomes NULL
	Tags     []string          `bqsink:"tags"`            // REPEATED STRING
	Attrs    map[string]string `bqsink:"attrs"`           // JSON
	Detail   Detail            `bqsink:"detail,record"`   // RECORD
	Internal string            `bqsink:"-"`               // dropped
}
```

| Option | Effect |
|---|---|
| `required` | the column is REQUIRED rather than NULLABLE |
| `nullifzero` | a zero value is written as NULL, using `IsZero()` where the type has one |
| `record` | a struct expands into a RECORD rather than becoming JSON |
| `date` | a `time.Time` becomes a DATE rather than a TIMESTAMP |
| `datetime` | a `time.Time` becomes a DATETIME |
| `time` | a `time.Time` becomes a TIME |
| `-` | the field appears in neither the schema nor the rows |

`required` and `nullifzero` cannot be combined. On a repeated column `nullifzero`
means "no elements", so a nil or empty slice becomes NULL.

### Writing a time.Time as a DATE, DATETIME or TIME

Carrying a calendar day around as a `time.Time` is ordinary Go, so the column it
becomes is a tag away:

```go
type Booking struct {
	CreatedAt time.Time `bqsink:"created_at"`                  // TIMESTAMP
	StayOn    time.Time `bqsink:"stay_on,date"`                // DATE
	CheckInAt time.Time `bqsink:"check_in_at,datetime"`        // DATETIME
	OpensAt   time.Time `bqsink:"opens_at,time"`               // TIME
}
```

Each option drops what the column does not record: the time of day, the UTC
offset, and the date. **The conversion reads the value's own location**, so the
calendar a column records is chosen by handing over a `time.Time` already in it:

```go
b.StayOn = t.In(jst) // records the Tokyo date, which past 15:00 UTC is tomorrow
```

That is why there is no separate timezone option. `t` and `t.In(jst)` are the same
instant, so a TIMESTAMP column is unaffected either way.

These are the only options that change a column's type away from the Go type's
own. Dropping a component is the whole conversion — there is no rounding to choose
and nothing that can fail. A conversion needing either, such as a `float64` written
to an INTEGER column, belongs in a `FieldMarshaler` or `MarshalFunc` where the
caller states the policy; declare the Go field as `int64` if that is what the
column is. Only `time.Time` takes these options, not a named type whose underlying
type is `time.Time`, and not alongside `record` or one another.

### Type mapping

```
STRING     string
BOOL       bool
INTEGER    int, int8, int16, int32, int64, uint8, uint16, uint32
FLOAT      float32, float64
BYTES      []byte
TIMESTAMP  time.Time
DATE       civil.Date
TIME       civil.Time
DATETIME   civil.DateTime
NUMERIC    big.Rat, uint, uint64
JSON       a struct, a map with string keys, json.RawMessage, or any
```

**A type carrying structure BigQuery has no column type for becomes JSON.** That
covers structs, maps with string keys, `json.RawMessage` and `any`. JSON leaves the
shape inside the column free, so adding a field to a nested struct needs no
migration; `record` opts back into a RECORD, which keeps the columnar layout that
lets BigQuery read one nested field without scanning the rest.

`uint` and `uint64` become NUMERIC rather than INTEGER, because BigQuery's INTEGER
is a signed INT64 and cannot hold the upper half of a `uint64`.

Embedded structs are promoted into the outer struct, following the rules of
`encoding/json`. An embedded type with no exported fields, such as a
`sync.Mutex`, contributes no columns.

### Columns tags cannot express

Pass a schema outright for BIGNUMERIC precision, column descriptions, policy tags
and the like:

```go
s, err := bqsink.New[Row](client, relation, bqsink.WithSchema(bigquery.Schema{
	{Name: "amount", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9,
		Description: "billed amount"},
}))
```

Every column the struct would write has to be present in the schema; `New` fails
otherwise rather than letting BigQuery reject the first write. A schema wider than
the struct is fine — the extra columns stay NULL.

### Partitioning, clustering and descriptions

The physical layout lives in tag keys of its own, because Go's convention for a
tag string is a concatenation of space-separated `key:"value"` pairs and a
description may contain the commas a comma-separated list cannot:

```go
type AccessLog struct {
	Timestamp time.Time `bqsink:"timestamp,required" partition:"day"`
	UserID    string    `bqsink:"user_id" cluster:"1"`
	Region    string    `bqsink:"region" cluster:"2"`
	Amount    *big.Rat  `bqsink:"amount" description:"billed amount, including tax"`
}
```

| Key | Value |
|---|---|
| `partition` | `day` (the default for an empty value), `hour`, `month` or `year`, optionally followed by `,require` to demand a partition filter on every query |
| `cluster` | the column's position, counting from 1; the order decides how well BigQuery can prune |
| `description` | documents the column |

`New` rejects a layout BigQuery would refuse, so a bad tag fails before the first
write rather than at `CREATE TABLE`:

- one partitioning column per table, not repeated, and TIMESTAMP, DATE or DATETIME
- `partition:"hour"` on a DATE column — the one granularity DATE cannot carry
- more than four clustering columns, a repeated position, or a gap in the positions
- clustering on FLOAT, JSON, BYTES or a repeated column
- `require` with nothing partitioned, which BigQuery accepts but cannot act on

### Table level settings

Settings with no column to hang off — labels, expiration, and anything the tags
above do not cover — come from a method:

```go
func (AccessLog) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Labels:         map[string]string{"env": "prod"},
		ExpirationTime: time.Now().Add(30 * 24 * time.Hour),
	}
}
```

Declaring the same thing both ways is an error rather than one silently winning:
`New` fails if the metadata sets `TimePartitioning`, `RangePartitioning` or
`Clustering` for a column that also carries a tag.

### Custom column types

A type can declare the column it becomes:

```go
func (Payload) BigQueryFieldType() bigquery.FieldType { return bigquery.JSONFieldType }

func (p Payload) MarshalBigQueryValue() (bigquery.Value, error) {
	b, err := json.Marshal(p)
	return string(b), err
}
```

For types you do not own, register the mapping instead:

```go
bqsink.WithMarshalers(
	bqsink.MarshalFunc(bigquery.StringFieldType, func(id ExternalID) (bigquery.Value, error) {
		return id.String(), nil
	}),
)
```

A registered mapping wins over the type's own `FieldMarshaler`.

### Columns bqsink fills in

Embed `IngestionMetadata` to get columns describing how the row was written:

```go
type AccessLog struct {
	bqsink.IngestionMetadata          // _ingestion_at, _ingestion_id, _ingestion_row_id
	UserID string `bqsink:"user_id"`
}
```

| Column | Value |
|---|---|
| `_ingestion_at` | when `Sink` was called for the row |
| `_ingestion_id` | identifies the `Sinker`, a version 7 UUID |
| `_ingestion_row_id` | identifies the row, a version 7 UUID |

**`_ingestion_row_id` is what makes at-least-once delivery workable.** A retried row keeps
the same id, so duplicates are identifiable downstream. Version 7 UUIDs sort by the
time they were made, which also suits a clustering key.

Write your own type when the names or the values need to differ:

```go
type Provenance struct {
	At    time.Time `bqsink:"ingested_at"`
	RunID string    `bqsink:"run_id"`
}

func (p *Provenance) FillRow(_ context.Context, info bqsink.AppendInfo) error {
	p.At = info.Time
	p.RunID = os.Getenv("WORKFLOW_RUN_ID")
	return nil
}

type AccessLog struct {
	Provenance
	UserID string `bqsink:"user_id"`
}
```

`FillRow` runs **once per row, on a copy, before the conversion and before any
retry**. So a value made inside it — `time.Now()`, a ULID, a trace id — does not
drift when the row is retried. `AppendInfo` carries only what the row cannot work
out for itself: the destination, the `Sinker`'s id and creation time, the row's id
and time.

It needs a **pointer receiver**; with a value receiver it would fill a copy that is
then discarded, and `New` rejects that.

## Migration

`Migrate` reads the table's state, asks the strategy what to change, and applies
the answer. `Sink` triggers it on the first call, so calling it directly is only
needed to apply schema changes ahead of time, such as during a deploy.

| Strategy | Behaviour |
|---|---|
| `AppendNewColumns{}` | adds missing columns, relaxes REQUIRED to NULLABLE |
| `SyncAllColumns{}` | the above, and drops columns the declaration no longer has |
| `MigrationNone{}` | leaves the schema alone |

**The default is `AppendNewColumns{CreateIfMissing: true}`**: following the
declaration is what bqsink is for, and neither change it makes destroys anything.
So the example above creates the table if it is absent and adds a column when the
struct gains a field.

All three take `CreateIfMissing`. Where it is off and the table does not exist,
`Migrate` returns an error wrapping `ErrTableMissing` rather than creating it.

```go
bqsink.WithMigration(bqsink.MigrationNone{})  // write to a table something else owns
```

```go
bqsink.WithMigration(bqsink.SyncAllColumns{
	IgnoreColumns:   []string{"managed_elsewhere"},
	CreateIfMissing: true,
})
```

`IgnoreColumns` names columns bqsink does not manage: they are neither dropped nor
reported as conflicts.

A difference BigQuery cannot reconcile — a changed type, NULLABLE turning
REQUIRED, or a change inside a RECORD — is reported as an error wrapping
`ErrSchemaConflict` rather than migrated.

`Migrate` runs once per `Sinker` and caches its outcome, success or failure alike.
Recovering from a failure means building a new `Sinker`.

## Transports

```go
bqsink.WithWriteStrategy(&bqsink.StorageWrite{})  // the default
bqsink.WithWriteStrategy(&bqsink.LoadJobs{})
```

**`StorageWrite`** uses the BigQuery Storage Write API. Rows land as they arrive.
`Append` hands the row over and keeps the pending result, so a rejected row
surfaces from `Flush` or `Close`. Only the default and committed stream types are
supported.

**`LoadJobs`** buffers rows as newline delimited JSON and submits load jobs. Batch
oriented: the `Append` that reaches the threshold blocks until BigQuery finishes
the job. Thresholds are `FlushRows` (10000) and `FlushBytes` (32 MiB).

Large batches can be staged in Cloud Storage instead of being uploaded with the
job:

```go
import "github.com/mashiike/bqsink/bqgcs"

bqsink.WithWriteStrategy(&bqsink.LoadJobs{
	Staging: &bqgcs.Staging{Client: gcsClient, Bucket: "staging-bucket", Prefix: "bqsink"},
})
```

`bqgcs` is a separate package so that using bqsink does not pull in
`cloud.google.com/go/storage`.

## Retries

A transient failure is retried under one policy, shared by `Migrate` and the write
path: a concurrent change to the table, a rate limit, or a server side failure,
over either HTTP or gRPC. Four retries with jittered backoff between 200ms and 5s.

```go
bqsink.WithRetryPolicy(func() gax.Retryer {
	return gax.OnErrorFunc(gax.Backoff{Initial: time.Second}, myPredicate)
})
```

**Writes are at-least-once.** A retry can deliver a row twice and neither transport
deduplicates.

## Logging

```go
bqsink.WithLogger(slog.Default())
```

Nothing is logged without it. The default discards every record rather than writing
to the embedding program's `slog.Default()` uninvited. Every record names the
relation.

| Level | What it reports |
|---|---|
| `Debug` | the difference `Migrate` found, the stream that was opened, the rows a flush carried |
| `Info` | a change made to the table, and a load job submitted and finished |
| `Warn` | something bqsink let pass |

**`Error` is unused.** A failure is returned, and logging it as well would report it
twice. What `Warn` is for is the opposite case, where nothing is returned and the
record is the only trace: a failure a retry got past, a schema difference the
strategy chose to leave alone, a column drop, or a failure that had to give way to
a worse one — such as a stream that would not close after rows had already been
lost.

## Testing

```
go test ./...
```

Unit tests reach no network. Integration tests write to a real project and skip
unless the environment names one:

```
BQSINK_TEST_PROJECT=my-project go test ./...
BQSINK_TEST_PROJECT=my-project BQSINK_TEST_BUCKET=my-bucket go test ./...
```

| Variable | Effect |
|---|---|
| `BQSINK_TEST_PROJECT` | required; without it every integration test skips |
| `BQSINK_TEST_DATASET` | dataset to use, created if absent (`bqsink_integration_test`) |
| `BQSINK_TEST_BUCKET` | required only for the Cloud Storage staging tests |

Each test uses a uniquely named table and removes it afterwards.

## Not supported

- **Migrating a change inside a RECORD.** Detected and reported, not applied. Only
  reachable through the `record` tag, since a struct is JSON by default
- **Pending and buffered streams.** They need their rows committed or their offset
  flushed, which bqsink does not do, so they are rejected rather than silently
  losing rows
- **Exactly-once delivery**

## License

MIT
