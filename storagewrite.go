package bqsink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
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

// Open implements WriteStrategy.
//
// It derives the proto descriptor from the schema once, which is possible because
// Migrate has already settled it, and opens a managedwriter client and stream.
func (w *StorageWrite) Open(ctx context.Context, table *bigquery.Table, schema bigquery.Schema) (RowWriter, error) {
	descriptor, err := rowDescriptor(schema)
	if err != nil {
		return nil, err
	}
	normalized, err := adapt.NormalizeDescriptor(descriptor)
	if err != nil {
		return nil, fmt.Errorf("bqsink: normalize the proto descriptor: %w", err)
	}
	client, err := managedwriter.NewClient(ctx, table.ProjectID, w.ClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("bqsink: open a Storage Write API client for %s: %w", table.ProjectID, err)
	}
	stream, err := client.NewManagedStream(ctx, w.writerOptions(table, normalized)...)
	if err != nil {
		if cerr := client.Close(); cerr != nil {
			return nil, fmt.Errorf("bqsink: open a write stream for %s: %w (closing the client also failed: %v)",
				table.FullyQualifiedName(), err, cerr)
		}
		return nil, fmt.Errorf("bqsink: open a write stream for %s: %w", table.FullyQualifiedName(), err)
	}
	return &storageWriteWriter{
		client:     client,
		stream:     stream,
		schema:     schema,
		descriptor: descriptor,
	}, nil
}

// writerOptions puts the caller's options first, then the destination and the
// schema descriptor, so that the relation and the declared schema win over
// anything naming them.
func (w *StorageWrite) writerOptions(table *bigquery.Table, descriptor *descriptorpb.DescriptorProto) []managedwriter.WriterOption {
	opts := make([]managedwriter.WriterOption, 0, len(w.WriterOptions)+3)
	opts = append(opts, w.WriterOptions...)
	if w.StreamName != "" {
		opts = append(opts, managedwriter.WithStreamName(w.StreamName))
	} else {
		streamType := w.StreamType
		if streamType == "" {
			streamType = managedwriter.DefaultStream
		}
		opts = append(opts,
			managedwriter.WithType(streamType),
			managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(
				table.ProjectID, table.DatasetID, table.TableID)),
		)
	}
	return append(opts, managedwriter.WithSchemaDescriptor(descriptor))
}

type storageWriteWriter struct {
	client     *managedwriter.Client
	stream     *managedwriter.ManagedStream
	schema     bigquery.Schema
	descriptor protoreflect.MessageDescriptor

	mu      sync.Mutex
	pending []*managedwriter.AppendResult
}

// Append implements RowWriter. It sends the row and keeps the result to be checked
// by Flush, so that appends are not serialised on a round trip each.
func (w *storageWriteWriter) Append(ctx context.Context, row map[string]bigquery.Value) error {
	data, err := w.marshalRow(row)
	if err != nil {
		return err
	}
	result, err := w.stream.AppendRows(ctx, [][]byte{data})
	if err != nil {
		return fmt.Errorf("bqsink: append a row to %s: %w", w.stream.StreamName(), err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = append(w.pending, result)
	return nil
}

func (w *storageWriteWriter) marshalRow(row map[string]bigquery.Value) ([]byte, error) {
	text, err := encodeStorageWriteRow(row, w.schema)
	if err != nil {
		return nil, err
	}
	message := dynamicpb.NewMessage(w.descriptor)
	if err := protojson.Unmarshal(text, message); err != nil {
		return nil, fmt.Errorf("bqsink: fit the row to the table's proto descriptor: %w", err)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("bqsink: marshal the row: %w", err)
	}
	return data, nil
}

// Flush implements RowWriter. It waits for every append handed over so far and
// reports the first one BigQuery rejected.
func (w *storageWriteWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()

	var firstErr error
	for _, result := range pending {
		if _, err := result.GetResult(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("bqsink: an append to %s was rejected: %w", w.stream.StreamName(), err)
		}
	}
	return firstErr
}

// Close implements RowWriter. It waits for the outstanding appends and then closes
// both the stream and the client, so that neither leaks its gRPC connection.
//
// Its error reports rows that never reached BigQuery and must not be discarded.
func (w *storageWriteWriter) Close(ctx context.Context) error {
	err := w.Flush(ctx)
	// managedwriter reports a cleanly closed stream as io.EOF: "For normal
	// operation, mark the stream error as io.EOF."
	if serr := w.stream.Close(); serr != nil && !errors.Is(serr, io.EOF) && err == nil {
		err = fmt.Errorf("bqsink: close the write stream: %w", serr)
	}
	if cerr := w.client.Close(); cerr != nil && err == nil {
		err = fmt.Errorf("bqsink: close the Storage Write API client: %w", cerr)
	}
	return err
}
