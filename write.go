package bqsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

// Row is one row on its way to BigQuery.
type Row struct {
	// ID identifies the row. It is what a transport names the row by when it has
	// something to say about it, and it is the value the _ingestion_row_id column
	// gets when the row type fills that column in.
	//
	// It plays no part in reporting what could not be written: the count WriteRows
	// returns is what says that.
	ID string

	// Values are the columns to write.
	Values map[string]bigquery.Value
}

// RowWriter sends rows to BigQuery. Retrying a transient failure is its
// responsibility, since it is the one holding the rows while they are in flight.
//
// An implementation must be safe for concurrent use, because a Sinker is and
// hands its writer straight to the caller's goroutines.
type RowWriter interface {
	// WriteRows writes rows in order and returns how many of them it wrote.
	//
	// It must return a non-nil error when n < len(rows), so that rows[n:] are
	// exactly the rows that did not land. It must not retain rows after returning.
	WriteRows(ctx context.Context, rows []Row) (n int, err error)

	// Close releases what the writer holds.
	Close(ctx context.Context) error
}

// WriteStrategy decides how rows reach BigQuery.
type WriteStrategy interface {
	// Open returns a RowWriter for table. Migrate has already reconciled the
	// schema by this point, so an implementation may derive a descriptor from it
	// once and keep it for the writer's lifetime.
	//
	// logger is the one WithLogger settled on, already carrying the relation, and
	// it is never nil. A writer is expected to keep it and describe what it does
	// with it, since an error it returns is the caller's to report and a failure
	// it has to swallow would otherwise leave no trace.
	Open(ctx context.Context, table *bigquery.Table, schema bigquery.Schema, logger *slog.Logger) (RowWriter, error)
}

// StorageWrite writes rows through the BigQuery Storage Write API.
//
// Each WriteRows sends the rows as one append and waits for BigQuery to accept
// them, so throughput comes from how many rows a single Sink is given rather than
// from overlapping appends. An append is all or nothing: BigQuery appends none of
// the rows in a request it rejects.
//
// BigQuery caps how large an append request may be, and StorageWrite does not split
// one, so a batch past that cap is rejected rather than divided. Where batches are
// large enough for that to be a worry, LoadJobs is the transport for them.
//
// Retrying is left to the client library's own automatic retries, which suit
// at-least-once delivery. The number of attempts is therefore the library's to
// decide, so StorageWrite has no policy to set the way LoadJobs does; what it has
// is DisableWriteRetries, for turning them off.
type StorageWrite struct {
	// StreamType selects the kind of stream to create. The zero value means
	// managedwriter.DefaultStream, which appends immediately and at least once.
	//
	// Only DefaultStream and CommittedStream are supported. PendingStream needs
	// its rows committed in a batch and BufferedStream needs its offset flushed,
	// neither of which bqsink does, so rows written to them would never become
	// visible.
	StreamType managedwriter.StreamType

	// StreamName writes to a stream that already exists instead of creating one.
	// StreamType is ignored when it is set.
	StreamName string

	// DisableWriteRetries stops bqsink from turning the client library's automatic
	// write retries on. The zero value leaves them on, since a row that never lands
	// is what bqsink is for; turn them off where an append being sent twice matters
	// more than it arriving, which is the exactly-once pattern the client library
	// warns they complicate.
	//
	// It says what bqsink does rather than what the stream ends up doing: the client
	// library has no way to switch retries back off, so an EnableWriteRetries(true)
	// of your own in WriterOptions still stands.
	DisableWriteRetries bool

	// ClientOptions configure the managedwriter client.
	ClientOptions []option.ClientOption

	// WriterOptions configure the stream. bqsink applies these first and then sets
	// the destination table and the schema descriptor itself, so an option naming
	// either of those has no effect: the relation and the declared schema are the
	// source of truth.
	WriterOptions []managedwriter.WriterOption
}

// Validate implements Validator.
func (w *StorageWrite) Validate() error {
	if w.StreamName != "" && w.StreamType != "" {
		return errors.New("StorageWrite: StreamName and StreamType are mutually exclusive, since a stream that already exists has its type fixed")
	}
	switch w.StreamType {
	case "", managedwriter.DefaultStream, managedwriter.CommittedStream:
		return nil
	case managedwriter.PendingStream:
		return fmt.Errorf("StorageWrite: StreamType %s is not supported, because bqsink does not commit a pending stream and its rows would never become visible", w.StreamType)
	case managedwriter.BufferedStream:
		return fmt.Errorf("StorageWrite: StreamType %s is not supported, because bqsink does not advance a buffered stream's offset and its rows would never become visible", w.StreamType)
	}
	return fmt.Errorf("StorageWrite: unknown StreamType %q", w.StreamType)
}

// Stager puts the rows somewhere a load job can read them, instead of uploading
// them with the job itself.
//
// Staging through Cloud Storage suits large batches: the upload is a plain object
// write that can be retried on its own, and the load job then reads from a URI
// rather than carrying the data.
type Stager interface {
	// Stage writes rows and returns the URI a load job should read. The returned
	// cleanup, when not nil, removes what Stage created and is called once the
	// load job has finished, whether or not it succeeded.
	Stage(ctx context.Context, rows []byte) (uri string, cleanup func(context.Context) error, err error)
}

// LoadJobs writes rows by rendering them as newline delimited JSON and submitting
// a BigQuery load job.
//
// One WriteRows is one load job, so how many rows a job carries is decided by how
// many rows a single Sink is given. LoadJobs does not accumulate rows across calls:
// writing them one at a time submits a load job each time, which a table's daily
// job quota will not stand for. Give Sink a whole batch, or accumulate before it.
//
// A load job is submitted synchronously, so every WriteRows blocks until BigQuery
// finishes the job, which takes seconds to minutes. Nothing is serialised on a
// lock, so concurrent calls submit concurrent jobs.
//
// A load job is all or nothing: WriteRows returns len(rows) or, having retried
// under RetryPolicy, 0 and an error. Rows are never left half written.
//
// Rows are uploaded with the load job itself unless Staging is set. Set it to
// bqgcs.Staging to put them in Cloud Storage first, which suits large batches.
type LoadJobs struct {
	// Staging, when set, writes the rows through a Stager and has the load job
	// read them from there instead of carrying them itself.
	Staging Stager

	// RetryPolicy decides how a load job that failed in a way a later attempt
	// could get past is retried. The zero value means DefaultRetryPolicy; set it
	// to a policy of your own to change that, and note that returning a bare
	// gax.OnErrorFunc places no limit on the number of attempts.
	//
	// It is called once per WriteRows, because a gax.Retryer carries the state of
	// its backoff and cannot be reused.
	RetryPolicy func() gax.Retryer
}

func (w *LoadJobs) retryPolicy() func() gax.Retryer {
	if w.RetryPolicy == nil {
		return DefaultRetryPolicy
	}
	return w.RetryPolicy
}

// Validate implements Validator.
func (w *LoadJobs) Validate() error {
	if v, ok := w.Staging.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("LoadJobs: %w", err)
		}
	}
	return nil
}

// Open implements WriteStrategy. It contacts BigQuery only when a load job is
// submitted, so opening never fails.
func (w *LoadJobs) Open(_ context.Context, table *bigquery.Table, schema bigquery.Schema, logger *slog.Logger) (RowWriter, error) {
	var loader jobLoader = tableLoader{table: table, logger: logger}
	if w.Staging != nil {
		loader = stagedLoader{table: table, stager: w.Staging, logger: logger}
	}
	return &loadJobsWriter{
		loader:      loader,
		schema:      schema,
		retryPolicy: w.retryPolicy(),
		logger:      logger,
	}, nil
}

// jobLoader submits the rows and waits for BigQuery to finish. It exists so that
// the writer can be tested without BigQuery.
type jobLoader interface {
	load(ctx context.Context, rows []byte, schema bigquery.Schema) error
}

type tableLoader struct {
	table  *bigquery.Table
	logger *slog.Logger
}

// load submits a load job with an explicit schema and CreateNever, leaving the
// table's shape entirely to Migrate.
func (l tableLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema) error {
	source := bigquery.NewReaderSource(bytes.NewReader(rows))
	source.SourceFormat = bigquery.JSON
	source.Schema = schema
	return runLoader(ctx, l.table, source, l.logger)
}

// runLoader submits the load job and waits for it, with an explicit schema and
// CreateNever so that the table's shape is left entirely to Migrate.
func runLoader(ctx context.Context, table *bigquery.Table, source bigquery.LoadSource, logger *slog.Logger) error {
	loader := table.LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.CreateDisposition = bigquery.CreateNever
	started := time.Now()
	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("bqsink: submit load job for %s: %w", table.FullyQualifiedName(), err)
	}
	// The wait below takes seconds to minutes, so the job is worth reporting before
	// it rather than only once it is over.
	logger.InfoContext(ctx, "submitted a load job", slog.String("job", job.ID()))
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("bqsink: wait for load job %s: %w", job.ID(), err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("bqsink: load job %s failed: %w", job.ID(), err)
	}
	// Measured from before the submission, since what the caller waited for is the
	// whole thing and not the job's own runtime.
	logger.InfoContext(ctx, "the load job finished",
		slog.String("job", job.ID()),
		slog.Duration("elapsed", time.Since(started)))
	return nil
}

// stagedLoader hands the rows to a Stager and points the load job at the URI it
// returns.
type stagedLoader struct {
	table  *bigquery.Table
	stager Stager
	logger *slog.Logger
}

func (l stagedLoader) load(ctx context.Context, rows []byte, schema bigquery.Schema) error {
	uri, cleanup, err := l.stager.Stage(ctx, rows)
	if err != nil {
		return fmt.Errorf("bqsink: stage rows for %s: %w", l.table.FullyQualifiedName(), err)
	}
	l.logger.DebugContext(ctx, "staged the rows for a load job",
		slog.String("uri", uri),
		slog.Int("bytes", len(rows)))
	if cleanup != nil {
		defer func() {
			// The rows are already loaded or already reported as undelivered, so a
			// failure to tidy up is not worth overriding that with; it is only logged,
			// and what is left behind needs removing by hand.
			if cerr := cleanup(ctx); cerr != nil {
				l.logger.WarnContext(ctx, "could not remove the staged rows",
					slog.String("uri", uri),
					slog.Any("error", cerr))
			}
		}()
	}
	source := bigquery.NewGCSReference(uri)
	source.SourceFormat = bigquery.JSON
	source.Schema = schema
	return runLoader(ctx, l.table, source, l.logger)
}

// loadJobsWriter holds nothing that changes, so concurrent writes need no lock.
type loadJobsWriter struct {
	loader      jobLoader
	schema      bigquery.Schema
	retryPolicy func() gax.Retryer
	logger      *slog.Logger
}

// WriteRows implements RowWriter. It submits one load job for rows and returns
// len(rows) once BigQuery has finished it.
func (w *loadJobsWriter) WriteRows(ctx context.Context, rows []Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	var buf bytes.Buffer
	for i := range rows {
		line, err := encodeJSONRow(rows[i].Values, w.schema)
		if err != nil {
			return 0, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	w.logger.DebugContext(ctx, "loading rows",
		slog.Int("rows", len(rows)),
		slog.Int("bytes", buf.Len()))
	err := retrying(ctx, w.logger, "load", w.retryPolicy, func(ctx context.Context) error {
		return w.loader.load(ctx, buf.Bytes(), w.schema)
	})
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Close implements RowWriter. A load job holds nothing once it has finished, so
// there is nothing to release.
func (w *loadJobsWriter) Close(context.Context) error {
	return nil
}
