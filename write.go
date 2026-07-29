package bqsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
)

// RowWriter accepts rows and sends them to BigQuery.
//
// An implementation must be safe for concurrent use, because a Sinker is and
// hands its writer straight to the caller's goroutines.
type RowWriter interface {
	// Append hands a row over for writing. It may buffer rather than send.
	Append(ctx context.Context, row map[string]bigquery.Value) error

	// Flush sends whatever is buffered.
	Flush(ctx context.Context) error

	// Close flushes and then releases the writer's resources.
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
// Append hands the row to BigQuery and keeps the pending result rather than
// waiting for it, so a rejected row surfaces from Flush or Close rather than from
// the Append that sent it. Their errors therefore report rows that never landed
// and must not be discarded.
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

	// ClientOptions configure the managedwriter client.
	ClientOptions []option.ClientOption

	// WriterOptions configure the stream. bqsink applies these first and then
	// sets the destination table and the schema descriptor itself, so an option
	// naming either of those has no effect: the relation and the declared schema
	// are the source of truth.
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

// DefaultFlushRows is how many rows LoadJobs buffers before submitting a load
// job, used when FlushRows is not set.
//
// BigQuery caps how many load jobs a table accepts per day, so a small threshold
// exhausts that budget on a busy table. The exact cap is not verified here.
const DefaultFlushRows = 10000

// DefaultFlushBytes is how much buffered JSON makes LoadJobs submit a load job
// regardless of the row count, used when FlushBytes is not set.
//
// This is about the writer's own memory rather than a BigQuery limit: the client
// library uploads with a resumable request and splits it into chunks itself, so
// the request size is not what needs bounding. What needs bounding is the buffer,
// which holds every row until it is flushed.
const DefaultFlushBytes = 32 << 20

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

// LoadJobs writes rows by buffering them as newline delimited JSON and
// submitting BigQuery load jobs.
//
// A load job is submitted synchronously, so the Append that reaches FlushRows,
// and every Flush and Close, blocks until BigQuery finishes the job. That takes
// seconds to minutes, and it happens while the writer's lock is held, so
// concurrent calls to Sink wait for it. This is a batch transport; use
// StorageWrite where rows should land as they arrive.
//
// A transient failure is retried by the Sinker under its retry policy. Rows still
// buffered when a load job finally fails are dropped, so that a table which keeps
// rejecting them cannot make the buffer grow without bound; the error says how
// many were lost. Only Flush and Close report whether the rows they were holding
// reached BigQuery, so their errors matter.
//
// Rows are uploaded with the load job itself unless Staging is set. Set it to
// bqgcs.Staging to put them in Cloud Storage first, which suits large batches.
type LoadJobs struct {
	// FlushRows submits a load job once this many rows are buffered. The zero
	// value means DefaultFlushRows.
	FlushRows int

	// FlushBytes submits a load job once the buffered JSON reaches this size,
	// whatever the row count. The zero value means DefaultFlushBytes. A single row
	// larger than this is still written, in a batch of its own.
	FlushBytes int

	// Staging, when set, writes the rows through a Stager and has the load job
	// read them from there instead of carrying them itself.
	Staging Stager
}

func (w *LoadJobs) flushRows() int {
	if w.FlushRows <= 0 {
		return DefaultFlushRows
	}
	return w.FlushRows
}

func (w *LoadJobs) flushBytes() int {
	if w.FlushBytes <= 0 {
		return DefaultFlushBytes
	}
	return w.FlushBytes
}

// Validate implements Validator.
func (w *LoadJobs) Validate() error {
	if w.FlushRows < 0 {
		return fmt.Errorf("LoadJobs: FlushRows is %d, want zero for the default or a positive count", w.FlushRows)
	}
	if w.FlushBytes < 0 {
		return fmt.Errorf("LoadJobs: FlushBytes is %d, want zero for the default or a positive size", w.FlushBytes)
	}
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
		loader:     loader,
		schema:     schema,
		flushRows:  w.flushRows(),
		flushBytes: w.flushBytes(),
		logger:     logger,
	}, nil
}

// jobLoader submits the buffered rows and waits for BigQuery to finish. It exists
// so that the buffering can be tested without BigQuery.
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
	// The wait below takes seconds to minutes and holds the writer's lock, so the
	// job is worth reporting before it rather than only once it is over.
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
			// The rows are already loaded or already reported as lost, so a failure
			// to tidy up is not worth overriding that with; it is only logged, and
			// what is left behind needs removing by hand.
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

type loadJobsWriter struct {
	loader     jobLoader
	schema     bigquery.Schema
	flushRows  int
	flushBytes int
	logger     *slog.Logger

	mu   sync.Mutex
	buf  bytes.Buffer
	rows int
}

// Append implements RowWriter. It buffers the row, submitting a load job once
// FlushRows rows are held.
func (w *loadJobsWriter) Append(ctx context.Context, row map[string]bigquery.Value) error {
	line, err := encodeJSONRow(row, w.schema)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(line)
	w.buf.WriteByte('\n')
	w.rows++
	if w.rows < w.flushRows && w.buf.Len() < w.flushBytes {
		return nil
	}
	return w.flushLocked(ctx)
}

// Flush implements RowWriter.
func (w *loadJobsWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

// Close implements RowWriter. It flushes, so its error reports rows that never
// reached BigQuery and must not be discarded.
func (w *loadJobsWriter) Close(ctx context.Context) error {
	return w.Flush(ctx)
}

// flushLocked submits the buffered rows and clears the buffer either way, so that
// a failing table cannot make it grow without bound. The rows are lost, which the
// returned error reports.
func (w *loadJobsWriter) flushLocked(ctx context.Context) error {
	if w.rows == 0 {
		return nil
	}
	rows := bytes.Clone(w.buf.Bytes())
	count := w.rows
	w.buf.Reset()
	w.rows = 0
	w.logger.DebugContext(ctx, "flushing the buffered rows",
		slog.Int("rows", count),
		slog.Int("bytes", len(rows)))
	if err := w.loader.load(ctx, rows, w.schema); err != nil {
		return fmt.Errorf("%w: %d buffered row(s) were dropped", err, count)
	}
	return nil
}
