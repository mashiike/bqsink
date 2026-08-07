# bqsink

Expressive BigQuery ingestion library for Go.

bqsink writes rows to BigQuery and keeps the destination table's schema in sync
with a schema declared in Go code.

**The declaration is the source of truth, and it belongs to the row type.** The
schema comes from struct tags, or from the type's own `BigQueryTableMetadata` where
tags cannot say it, and the real table follows. No option describes the table:
bqsink never infers a schema from the data being written either, so a field added to
a struct becomes a column rather than a silent mismatch at write time.

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

w, err := (&bqsink.LoadJobs{}).NewWriter(client, bqsink.Relation{
	DatasetID: "logs",
	TableID:   "access",
})
if err != nil {
	return err
}
defer func() {
	if cerr := w.Close(ctx); cerr != nil && err == nil {
		err = cerr
	}
}()

s, err := bqsink.NewSinker(w, bqsink.DeclarationOf[AccessLog]())
if err != nil {
	return err
}

rows := []AccessLog{
	{Timestamp: time.Now(), UserID: "u1", Path: "/"},
	{Timestamp: time.Now(), UserID: "u2", Path: "/about"},
}
n, err := s.Sink(ctx, rows)
if err != nil {
	// rows[n:] never reached BigQuery. They are still yours to deal with.
	return err
}
return nil
```

Building takes two steps: a **writer** for the transport (`LoadJobs` here; see
[Transports](#transports)), and a **Sinker** for the declaration. Closing belongs to
the writer, since that is what holds a connection — a `Sinker` buffers nothing of
its own to flush and so has no `Close`.

`Sink` hands the rows it is given to the writer and waits for its `WriteResult`
to settle, so nothing is buffered inside the `Sinker` itself: **the rows it is
given are the batch handed to the writer.** What the writer then does with that
batch is its own business — `LoadJobs.FlushRows` can hold rows back across
calls, for instance (see [Flushing rows](#flushing-rows)). `Sink` returns how
many of them that settlement counts as done, and a non-nil error whenever that
is fewer than it was given: `rows[n:]` are exactly the ones that did not make
it, and nothing else records them.

What counts as done depends on the writer, not on `Sink`: delivery to BigQuery
for one that promises it, or only acceptance into a buffer of its own for one
that promises that instead, such as a `LoadJobsWriter` with `LoadJobs.FlushRows`
set. **A `Sink` call whose rows only reached that buffer can still return `(n,
nil)`** — the submission it goes on to trigger, or fails to, is reported later,
by `FlushRows` or `Close`, not by this call (see [Flushing rows](#flushing-rows)).
Give `Sink` a whole batch rather than one row at a time, especially with
`LoadJobs`, where every call is a load job unless `FlushRows` says otherwise.

A slice is a batch of its elements and anything else is a single row, so `Sink(ctx,
rows)` and `Sink(ctx, row)` are both ordinary calls. Every row in a batch has to be of
one type, which a `[]AccessLog` gives for free and a `[]any` can break.

**The row type is settled by `NewSinker`, not by the first `Sink`.** It reads the
`Declaration` it is given and keeps it for as long as the `Sinker` lives: a later
`Sink` handing over another type is an error, and a second type needs a `Sinker` of
its own. Everything the declaration decides — a struct that cannot be mapped to a
row, a column missing from a spelled out schema, a `FillRow` with a value receiver
— is therefore reported by `NewSinker`, which talks to nothing and reads nothing.
Nothing contacts BigQuery until the first `Sink`, which is what reconciles the real
table with the declaration and hands the writer the settled schema.

`DeclarationOf[T]()` is the ordinary way to build a `Declaration`, for a row type
known at compile time. Where the schema is only settled at run time,
`DeclarationFromMetadata(md *bigquery.TableMetadata, marshalers ...*Marshalers)`
reads it from data instead: the row type is fixed to `map[string]any`, since
there is no Go type to derive columns from, and `md` supplies the schema a
struct's tags would otherwise describe. `md` must not be nil and its `Schema`
must not be empty, since there would then be nothing to check a row's keys
against; either is reported here, the same as a struct tag `DeclarationOf`
could not parse.

A row built this way is a plain `map[string]any`: a key the schema does not
declare is an error, and a column the map omits becomes NULL, the same as a
struct field would. `RowFiller` has no way in — a map cannot implement it — so
a caller wanting a column like `_ingestion_at` fills it into the map directly,
before `Sink`.

## Options

`NewSinker` takes two, and **none of them describes the table.** What the table
looks like belongs to the row type, which reaches `NewSinker` as a `Declaration`;
these settle how bqsink behaves around that instead. How rows travel is settled
earlier still, on the writer's own constructor — see [Transports](#transports).

| Option | Default | Section |
|---|---|---|
| `WithMigrationStrategy` | `AppendNewColumns{CreateIfMissing: true}`, four retries | [Migration](#migration) |
| `WithLogger` | records are discarded | [Logging](#logging) |

Per-type marshaling overrides are not an Option either: `DeclarationOf` and
`DeclarationFromMetadata` both take `marshalers ...*Marshalers` themselves, so
that they settle how a value is written the same way they settle everything
else about the table — see [Custom column types](#custom-column-types).

A migration strategy is configured with a struct literal rather than an option of
its own, so that its settings never look like one:

```go
bqsink.WithMigrationStrategy(bqsink.SyncAllColumns{}, bqsink.DefaultRetryPolicy)
```

A writer's settings take the same shape — `LoadJobs` and `StorageWrite` are struct
literals too, configured where they are built rather than through an `Option`.

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

For BIGNUMERIC precision, policy tags and the like, spell the schema out on the row
type itself. **There is no option for declaring a schema:** what the table holds is
said in one place, next to the fields it describes.

```go
type Row struct {
	Amount *big.Rat `bqsink:"amount"`
}

func (Row) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "amount", Type: bigquery.BigNumericFieldType, Precision: 38, Scale: 9,
				Description: "billed amount"},
		},
	}
}
```

The same method carries partitioning, clustering, labels and expiration, so a type
that already has one simply adds `Schema` to it.

Every column the struct would write has to be present in the schema; `NewSinker`
fails otherwise rather than letting BigQuery reject the write. A schema wider than
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

A layout BigQuery would refuse is rejected before anything is written rather than at
`CREATE TABLE`:

- one partitioning column per table, not repeated, and TIMESTAMP, DATE or DATETIME
- `partition:"hour"` on a DATE column — the one granularity DATE cannot carry
- more than four clustering columns, a repeated position, or a gap in the positions
- clustering on FLOAT, JSON, BYTES or a repeated column

`require` needs no check of its own: it is written inside the `partition` tag, so
there is no way to ask for a partition filter without a partitioning column.

### Table level settings

A description and labels have no column to hang off, so they go on an embedded
`bqsink.TableMeta`, which contributes no column of its own:

```go
type AccessLog struct {
	bqsink.TableMeta `description:"one row per request" labels:"team=data,env=prod"`

	Timestamp time.Time `bqsink:"timestamp,required" partition:"day"`
	UserID    string    `bqsink:"user_id"`
}
```

| Key | Value |
|---|---|
| `description` | documents the table |
| `labels` | a `key=value,key=value` list; a value may be empty, a key may not |

Neither separator needs escaping, since BigQuery allows only lowercase letters,
digits, underscores and dashes in a label's key and value. Only these two keys are
read there — a `bqsink`, `partition` or `cluster` tag on `TableMeta` is rejected
rather than ignored, and a `TableMeta` reached through another embedded struct is
not searched for.

Anything else — expiration, and whatever the tags do not cover — comes from a
method:

```go
func (AccessLog) BigQueryTableMetadata() *bigquery.TableMetadata {
	return &bigquery.TableMetadata{
		ExpirationTime: time.Now().Add(30 * 24 * time.Hour),
	}
}
```

Declaring the same thing both ways is an error rather than one silently winning:
`NewSinker` fails if the metadata sets `Description`, `Labels`, `TimePartitioning`,
`RangePartitioning` or `Clustering` that a tag also settles.

### Custom column types

A type can declare the column it becomes:

```go
func (Payload) BigQueryFieldType() bigquery.FieldType { return bigquery.JSONFieldType }

func (p Payload) MarshalBigQueryValue() (bigquery.Value, error) {
	b, err := json.Marshal(p)
	return string(b), err
}
```

For types you do not own, register the mapping instead, passed to `DeclarationOf`
or `DeclarationFromMetadata` rather than to an Option:

```go
bqsink.DeclarationOf[Row](
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
then discarded, and `NewSinker` rejects that.

## Migration

The first `Sink` reads the table's state, asks the strategy what to change, and
applies the answer before writing anything. There is no separate method for it:
`NewSinker` already knows the declared schema from the `Declaration` it was given,
and the first batch is simply the earliest point at which reconciling it with
BigQuery becomes unavoidable — a `Sinker` with no rows to write has nothing to
reconcile.

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
`Sink` returns an error wrapping `ErrTableMissing` rather than creating it.

```go
bqsink.WithMigrationStrategy(bqsink.MigrationNone{})  // write to a table something else owns
```

```go
bqsink.WithMigrationStrategy(bqsink.SyncAllColumns{
	IgnoreColumns:   []string{"managed_elsewhere"},
	CreateIfMissing: true,
})
```

`IgnoreColumns` names columns bqsink does not manage: they are neither dropped nor
reported as conflicts.

A difference BigQuery cannot reconcile — a changed type, NULLABLE turning
REQUIRED, or a change inside a RECORD — is reported as an error wrapping
`ErrSchemaConflict` rather than migrated.

The migration runs once per `Sinker` and caches its outcome, success or failure alike.
Recovering from a failure means building a new `Sinker`.

## Transports

A writer is built directly from the transport's own settings, before there is a
`Sinker` to hand it to. **There is no default transport any more** — an unset
Option used to mean `&StorageWrite{}`; now the choice is which constructor you
call:

```go
w, err := (&bqsink.StorageWrite{}).NewWriter(client, relation)
w, err := (&bqsink.LoadJobs{}).NewWriter(client, relation)
```

Neither talks to BigQuery yet: `NewWriter` only builds the writer, and what
happens over the network happens on the first `Sink`, through the writer
`NewSinker` was given.

Every `WriteRows` returns a `WriteResult`, whose `Wait` reports what that call's
rows are settled as. That settlement is the writer's own promise, not the same
for every one: delivery to BigQuery for a writer such as `StorageWrite`, or only
acceptance into a buffer of its own for one such as a `LoadJobsWriter` with
`LoadJobs.FlushRows` set. Using `Sink` hides most of this: it calls `WriteRows`
and blocks on `Wait` before returning, so the `(n, err)` `Sink` gives back
already is that settlement — but what it settles is still whichever promise the
writer made, not necessarily delivery (see [Flushing rows](#flushing-rows)
below).

`SinkAsync` does the same batch reading, type checking, first-call
reconciliation, per-row `FillRow` and conversion that `Sink` does, but hands
back the `WriteResult` without calling `Wait` on it, leaving that to the
caller. It suits a caller keeping several batches in flight at once, or one
that wants to decide for itself when, or whether, to wait. `Sink`'s own guard
against a writer under-reporting — returning an error when `Wait` reports
fewer rows than were given — is not repeated here, since `SinkAsync` returns
before there is anything to compare `Wait`'s answer against; a caller wanting
the same guard compares the `n` its own call to `Wait` returns against how
many rows it gave `SinkAsync`.

**`StorageWrite`** uses the BigQuery Storage Write API. Each append is all or
nothing: none of the rows in a rejected request land. Only the default and
committed stream types are supported.

**`LoadJobs`** renders the rows as newline delimited JSON and submits a load job,
blocking until BigQuery finishes it, which takes seconds to minutes. A load job is
all or nothing too. Nothing is serialised on a lock, so concurrent calls submit
concurrent jobs — and a table's daily job quota is why rows belong in batches
rather than one call each; `FlushRows` gathers several calls into fewer jobs (see
[Flushing rows](#flushing-rows)).

Large batches can be staged in Cloud Storage instead of being uploaded with the
job:

```go
import "github.com/mashiike/bqsink/bqgcs"

w, err := (&bqsink.LoadJobs{
	Staging: &bqgcs.Staging{Client: gcsClient, Bucket: "staging-bucket", Prefix: "bqsink"},
}).NewWriter(client, relation)
```

`bqgcs` is a separate package so that using bqsink does not pull in
`cloud.google.com/go/storage`.

## Flushing rows

`LoadJobs.FlushRows` gathers rows across `WriteRows` calls and submits them as one
load job once that many are held, instead of a job per call:

```go
w, err := (&bqsink.LoadJobs{FlushRows: 10_000}).NewWriter(client, relation)
```

It is the threshold that submits a batch, not a `Wait` call: once a `WriteRows`
appends enough rows to reach `FlushRows`, that call submits the batch itself,
under `RetryPolicy`, and blocks until the job finishes before returning — the
way `bufio.Writer` flushes on a full buffer rather than on `Flush`. Every
earlier call that joined the same batch already returned, its rows merely
accepted rather than delivered, so **a single goroutine calling `Sink` on its
own, one batch after another, does end up with fewer jobs than calls once
enough rows have gone by.** Several goroutines calling `Sink` on the same
writer at once share a batch the same way and gain the same thing: whichever
of them fills it is the one that submits, and the rest share its job.

`FlushRows` earns its keep where rows are given to the writer directly through
`WriteRows` — a lower-level entry point than `Sink`, taking already-marshalled
`[]bqsink.Row`, and usable once some `Sinker` built on this writer has made its
first `Sink` call to bind the schema — gathering them without waiting on each
result right away:

```go
r1, err := w.WriteRows(ctx, rows1) // joins the open batch, no job yet
if err != nil {
	return err
}
r2, err := w.WriteRows(ctx, rows2) // joins the same batch
if err != nil {
	return err
}

// later, e.g. from a ticker, or once enough calls have gone by:
res, err := w.FlushRows(ctx)
if err != nil {
	return err
}
if _, err := res.Wait(ctx); err != nil { // the job's own outcome
	return err
}
if _, err := r1.Wait(ctx); err != nil { // already settled by FlushRows
	return err
}
if _, err := r2.Wait(ctx); err != nil {
	return err
}
```

Discarding the `WriteResult` `FlushRows` returns without a `Wait` leaves no
way to learn that job's own outcome, success or failure — `err` from
`FlushRows` itself only reports whether the flush was accepted, not how the
job it started turned out.

`Close` submits whatever `FlushRows` is still holding back, so rows gathered but
never flushed are not lost to a shutdown; it is also where a threshold
submission's failure gets reported if nothing has read it yet — that submission
never had a `WriteResult` of its own to carry the outcome, so `Close` is the
last chance.

**Not calling `Close`, or ignoring the error it returns, can lose rows that a
`Sink` or `WriteRows` call already reported as accepted.** A submission that
one of those calls triggered on its own, by filling the buffer to `FlushRows`,
is folded into the error `Close` returns if nothing else reported it first —
skip `Close` and that outcome reaches no one.

**A `FlushRows` call's own submission is different: its outcome is carried
solely by the `WriteResult` that call returned.** Discard that result without a
`Wait` and `Close` does not recover it either — the rows behind it are lost to
every caller, not just the one that called `FlushRows`.

## Retries

**Retrying belongs to whoever is holding the rows.** A writer still has them while
they are in flight, so it is the one that can try again; `Sinker` never retries a
write behind the writer's back.

```go
// Migration: the policy is the second argument, since a strategy is a pure
// decision and retrying is not part of it. nil means one attempt.
bqsink.WithMigrationStrategy(bqsink.SyncAllColumns{}, bqsink.DefaultRetryPolicy)

// LoadJobs retries a load job itself.
(&bqsink.LoadJobs{RetryPolicy: myPolicy}).NewWriter(client, relation)
```

`DefaultRetryPolicy` is four retries with jittered backoff between 200ms and 5s,
covering a concurrent change to the table, a rate limit, or a server side failure,
over either HTTP or gRPC. It is what `NewSinker` uses for migration when
`WithMigrationStrategy` is not given: two replicas deploying at once add the same
column at once, and BigQuery reports that as a failed precondition that only a retry
gets past.

`StorageWrite` has no policy to set: the client library's own automatic retries are
the ones that know how to re-enqueue an append on a reconnected stream, so bqsink
enables those and the number of attempts is theirs to decide. Set
`DisableWriteRetries` to have bqsink leave them alone.

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
| `Debug` | the difference the migration found, the stream that was opened, the rows a flush carried |
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
