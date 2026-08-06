package bqsink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// textualTypes are the BigQuery types bqsink sends to the Storage Write API as
// proto strings rather than the packed form adapt maps them to by default.
//
// The default mapping wants NUMERIC and BIGNUMERIC as bytes holding a BigDecimal,
// and DATETIME and TIME as int64 in an encoding the client library does not
// expose. BigQuery accepts a string for all of them, which is the same text a
// load job reads, so one row rendering serves both transports.
//
// TIMESTAMP is absent on purpose: BigQuery does not accept a string there, so it
// stays int64 microseconds since the epoch.
var textualTypes = []storagepb.TableFieldSchema_Type{
	storagepb.TableFieldSchema_NUMERIC,
	storagepb.TableFieldSchema_BIGNUMERIC,
	storagepb.TableFieldSchema_DATETIME,
	storagepb.TableFieldSchema_DATE,
	storagepb.TableFieldSchema_TIME,
}

// rowDescriptor builds the proto message a row is marshalled into.
func rowDescriptor(schema bigquery.Schema) (protoreflect.MessageDescriptor, error) {
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("bqsink: convert the schema for the Storage Write API: %w", err)
	}
	opts := make([]adapt.ProtoConversionOption, 0, len(textualTypes))
	for _, t := range textualTypes {
		opts = append(opts, adapt.WithProtoMapping(adapt.ProtoMapping{
			FieldType: t,
			Type:      descriptorpb.FieldDescriptorProto_TYPE_STRING,
		}))
	}
	desc, err := adapt.StorageSchemaToProtoDescriptorWithOptions(storageSchema, "Row", opts...)
	if err != nil {
		return nil, fmt.Errorf("bqsink: derive a proto descriptor from the schema: %w", err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("bqsink: the schema produced a %T, want a message descriptor", desc)
	}
	return md, nil
}

// StorageWrite writes rows through the BigQuery Storage Write API. It is the
// settings a StorageWriter is made from.
//
// WriteRows sends the rows as one append and hands back a WriteResult without
// waiting for BigQuery to accept them; that wait happens in the WriteResult's
// Wait. An append is all or nothing: BigQuery appends none of the rows in a
// request it rejects.
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

// NewWriter returns a writer that appends rows to the table relation names.
//
// It does not contact BigQuery: opening the managedwriter client and the stream
// happens in BindSchema, once the migration has settled the schema. An empty
// ProjectID on relation is filled in from the client.
//
// When StreamName is set, the stream already belongs to a table, so NewWriter
// checks relation against it rather than letting relation be silently ignored:
// it fails if the stream's table and relation name different tables.
func (w *StorageWrite) NewWriter(client *bigquery.Client, relation Relation) (*StorageWriter, error) {
	if client == nil {
		return nil, errors.New("bqsink: StorageWrite.NewWriter: client is nil")
	}
	if err := relation.validate(); err != nil {
		return nil, err
	}
	if relation.ProjectID == "" {
		relation.ProjectID = client.Project()
	}
	if err := w.Validate(); err != nil {
		return nil, fmt.Errorf("bqsink: %w", err)
	}
	if w.StreamName != "" {
		streamRelation, err := parseTableParent(managedwriter.TableParentFromStreamName(w.StreamName))
		if err != nil {
			return nil, fmt.Errorf("bqsink: StorageWrite.NewWriter: StreamName %q: %w", w.StreamName, err)
		}
		if streamRelation != relation {
			return nil, fmt.Errorf("bqsink: StorageWrite.NewWriter: StreamName %q belongs to %s, not %s",
				w.StreamName, streamRelation, relation)
		}
	}
	return &StorageWriter{
		relation:            relation,
		client:              client,
		streamName:          w.StreamName,
		streamType:          w.StreamType,
		disableWriteRetries: w.DisableWriteRetries,
		clientOptions:       w.ClientOptions,
		writerOpts:          w.WriterOptions,
		logger:              slog.New(slog.DiscardHandler),
	}, nil
}

// parseTableParent parses the "projects/P/datasets/D/tables/T" prefix
// managedwriter.TableParentFromStreamName returns, so that the relation a
// stream already belongs to can be compared against the one NewWriter was given.
func parseTableParent(parent string) (Relation, error) {
	parts := strings.Split(parent, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "datasets" || parts[4] != "tables" {
		return Relation{}, fmt.Errorf("%q is not a projects/{project}/datasets/{dataset}/tables/{table} table parent", parent)
	}
	return Relation{ProjectID: parts[1], DatasetID: parts[3], TableID: parts[5]}, nil
}

// StorageWriter writes rows to one BigQuery table through the Storage Write
// API. It implements RowsWriter.
//
// BindSchema opens the managedwriter client and stream; nothing is sent before
// that. WriteRows sends an append and returns without waiting for BigQuery to
// accept it, so a WriteResult nobody calls Wait on before Close never reports
// whether its rows landed: StorageWriter buffers no rows of its own to report
// them on Close's behalf the way LoadJobsWriter does.
type StorageWriter struct {
	relation Relation
	client   *bigquery.Client

	streamName          string
	streamType          managedwriter.StreamType
	disableWriteRetries bool
	clientOptions       []option.ClientOption
	writerOpts          []managedwriter.WriterOption

	mu         sync.Mutex
	logger     *slog.Logger
	schema     bigquery.Schema
	descriptor protoreflect.MessageDescriptor
	mwClient   *managedwriter.Client
	stream     *managedwriter.ManagedStream
	closed     bool
}

// Relation implements RowsWriter.
func (w *StorageWriter) Relation() Relation { return w.relation }

// Client implements RowsWriter.
func (w *StorageWriter) Client() *bigquery.Client { return w.client }

// BindLogger implements LoggerBindable.
func (w *StorageWriter) BindLogger(logger *slog.Logger) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logger = logger
}

// BindSchema implements RowsWriter.
//
// It derives the proto descriptor from the schema once, which is possible because
// the migration has already settled it, and opens a managedwriter client and stream.
func (w *StorageWriter) BindSchema(ctx context.Context, schema bigquery.Schema) error {
	if len(schema) == 0 {
		return errors.New("bqsink: StorageWriter: the schema has no columns")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stream != nil {
		return errors.New("bqsink: StorageWriter: a schema is already bound")
	}
	descriptor, err := rowDescriptor(schema)
	if err != nil {
		return err
	}
	normalized, err := adapt.NormalizeDescriptor(descriptor)
	if err != nil {
		return fmt.Errorf("bqsink: normalize the proto descriptor: %w", err)
	}
	client, err := managedwriter.NewClient(ctx, w.relation.ProjectID, w.clientOptions...)
	if err != nil {
		return fmt.Errorf("bqsink: open a Storage Write API client for %s: %w", w.relation.ProjectID, err)
	}
	stream, err := client.NewManagedStream(ctx, w.writerOptions(normalized)...)
	if err != nil {
		if cerr := client.Close(); cerr != nil {
			return fmt.Errorf("bqsink: open a write stream for %s: %w (closing the client also failed: %v)",
				w.relation, err, cerr)
		}
		return fmt.Errorf("bqsink: open a write stream for %s: %w", w.relation, err)
	}
	// The stream name says which kind of stream this is, so its type needs no
	// attribute of its own; a stream opened by name has a type bqsink cannot tell.
	w.logger.DebugContext(ctx, "opened a write stream", slog.String("stream", stream.StreamName()))
	w.schema = schema
	w.descriptor = descriptor
	w.mwClient = client
	w.stream = stream
	return nil
}

// writerOptions puts the caller's options first, then the destination and the
// schema descriptor, so that the relation and the declared schema win over
// anything naming them.
func (w *StorageWriter) writerOptions(descriptor *descriptorpb.DescriptorProto) []managedwriter.WriterOption {
	opts := make([]managedwriter.WriterOption, 0, len(w.writerOpts)+4)
	opts = append(opts, w.writerOpts...)
	if w.streamName != "" {
		opts = append(opts, managedwriter.WithStreamName(w.streamName))
	} else {
		streamType := w.streamType
		if streamType == "" {
			streamType = managedwriter.DefaultStream
		}
		opts = append(opts,
			managedwriter.WithType(streamType),
			managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(
				w.relation.ProjectID, w.relation.DatasetID, w.relation.TableID)))
	}
	opts = append(opts, managedwriter.WithSchemaDescriptor(descriptor))
	if w.disableWriteRetries {
		return opts
	}
	// Retrying is the writer's responsibility, and the client library's own retries
	// are the ones that know how to re-enqueue an append on a reconnected stream.
	// They suit at-least-once delivery, which is what bqsink offers.
	return append(opts, managedwriter.EnableWriteRetries(true))
}

// WriteRows implements RowsWriter. It marshals the rows and sends them as one
// append, returning a WriteResult that reports whether BigQuery accepted them
// once Wait is called.
//
// An append is all or nothing, so the WriteResult's Wait reports len(rows) or
// 0: BigQuery appends none of the rows in a request it rejects.
func (w *StorageWriter) WriteRows(ctx context.Context, rows []Row) (WriteResult, error) {
	if len(rows) == 0 {
		return ResolvedResult(0, nil), nil
	}
	w.mu.Lock()
	closed := w.closed
	stream := w.stream
	logger := w.logger
	schema := w.schema
	descriptor := w.descriptor
	w.mu.Unlock()
	if closed {
		return nil, errors.New("bqsink: StorageWriter: the writer is closed")
	}
	if stream == nil {
		return nil, errors.New("bqsink: StorageWriter: no schema is bound yet")
	}
	data := make([][]byte, len(rows))
	for i := range rows {
		marshalled, err := marshalStorageWriteRow(rows[i].Values, schema, descriptor)
		if err != nil {
			return nil, err
		}
		data[i] = marshalled
	}
	result, err := stream.AppendRows(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("bqsink: append %d row(s) to %s: %w", len(rows), stream.StreamName(), err)
	}
	return &storageResult{result: result, rows: rows, streamName: stream.StreamName(), logger: logger}, nil
}

// marshalStorageWriteRow is handed the schema and the descriptor rather than
// reading them off the writer, since they are settled under its lock.
func marshalStorageWriteRow(row map[string]bigquery.Value, schema bigquery.Schema, descriptor protoreflect.MessageDescriptor) ([]byte, error) {
	text, err := encodeStorageWriteRow(row, schema)
	if err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(descriptor)
	if err := protojson.Unmarshal(text, message); err != nil {
		return nil, fmt.Errorf("bqsink: fit the row to the table's proto descriptor: %w", err)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("bqsink: marshal the row: %w", err)
	}
	return data, nil
}

// describeRowErrors names the rows BigQuery found malformed, which it reports by
// their index in the request. It returns the empty string when there are none, so
// that a request-level failure is not dressed up as a row-level one.
func describeRowErrors(response *storagepb.AppendRowsResponse, rows []Row) string {
	rowErrors := response.GetRowErrors()
	if len(rowErrors) == 0 {
		return ""
	}
	described := make([]string, 0, len(rowErrors))
	for _, rowErr := range rowErrors {
		index := rowErr.GetIndex()
		id := "unknown row"
		if index >= 0 && index < int64(len(rows)) {
			id = rows[index].ID
		}
		described = append(described, fmt.Sprintf("%s: %s: %s", id, rowErr.GetCode(), rowErr.GetMessage()))
	}
	return fmt.Sprintf(" (malformed rows: %s)", strings.Join(described, "; "))
}

// storageResult is the WriteResult WriteRows returns. It keeps the rows so that
// Wait can name the ones BigQuery rejects.
type storageResult struct {
	result     *managedwriter.AppendResult
	rows       []Row
	streamName string
	logger     *slog.Logger
}

// Wait implements WriteResult.
func (r *storageResult) Wait(ctx context.Context) (int, error) {
	// The full response is what carries the per-row detail; GetResult would leave
	// only the request-level error to report.
	response, err := r.result.FullResponse(ctx)
	if err != nil {
		return 0, fmt.Errorf("bqsink: %d row(s) were rejected by %s: %w%s",
			len(r.rows), r.streamName, err, describeRowErrors(response, r.rows))
	}
	r.logger.DebugContext(ctx, "appended rows", slog.Int("rows", len(r.rows)))
	return len(r.rows), nil
}

// Close implements RowsWriter. It closes both the stream and the client, so that
// neither leaks its gRPC connection.
//
// Only the first failure is returned, so the other is logged: nothing else would
// say that a connection did not shut down cleanly. Close does not report the
// outcome of a WriteResult nobody waited on, unlike LoadJobsWriter's: StorageWriter
// holds no rows of its own to report them with, so that outcome is simply lost.
func (w *StorageWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	stream := w.stream
	client := w.mwClient
	logger := w.logger
	w.mu.Unlock()
	if stream == nil {
		return nil
	}
	var err error
	// managedwriter reports a cleanly closed stream as io.EOF: "For normal
	// operation, mark the stream error as io.EOF."
	if serr := stream.Close(); serr != nil && !errors.Is(serr, io.EOF) {
		err = fmt.Errorf("bqsink: close the write stream: %w", serr)
	}
	if cerr := client.Close(); cerr != nil {
		if err == nil {
			err = fmt.Errorf("bqsink: close the Storage Write API client: %w", cerr)
		} else {
			logger.WarnContext(ctx, "could not close the Storage Write API client", slog.Any("error", cerr))
		}
	}
	return err
}
