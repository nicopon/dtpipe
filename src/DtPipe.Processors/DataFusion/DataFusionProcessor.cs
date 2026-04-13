using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Processors.Sql;
using Microsoft.Extensions.Logging;
using System.IO.Pipes;
using System.Threading.Channels;

namespace DtPipe.Processors.DataFusion;

public sealed class DataFusionProcessor : IColumnarStreamReader
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;        // logical SQL table name
    private readonly string _mainChannelAlias; // physical channel alias for registry lookup
    private readonly string[] _refAliases;        // logical SQL table names
    private readonly string[] _refChannelAliases; // physical channel aliases for registry lookup
    private readonly ILogger<DataFusionProcessor> _logger;

    private nint _runtime = nint.Zero;
    private nint _ctx = nint.Zero;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private readonly List<IArrowArrayStream> _activeStreams = new();

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;

    public DataFusionProcessor(
        IMemoryChannelRegistry registry,
        string query,
        string mainAlias,
        string mainChannelAlias,
        string[] refAliases,
        string[] refChannelAliases,
        ILogger<DataFusionProcessor> logger)
    {
        _registry = registry;
        _query = query;
        _mainAlias = mainAlias;
        _mainChannelAlias = mainChannelAlias;
        _refAliases = refAliases;
        _refChannelAliases = refChannelAliases;
        _logger = logger;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("DataFusionProcessor: OpenAsync — query={Query}", _query);
        try
        {
            _runtime = DataFusionBridge.RuntimeNew();
            if (_runtime == nint.Zero) throw new Exception("Failed to create DataFusion runtime");

            ValidateAliases();

            _ctx = DataFusionBridge.ContextNew(_runtime);
            if (_ctx == nint.Zero) throw new Exception("Failed to create DataFusion context");

            if (_refAliases.Length > 0)
            {
                // _refAliases[i] = logical SQL table name; _refChannelAliases[i] = physical channel alias.
                var materializationTasks = _refAliases
                    .Select((alias, i) => RegisterChannelSourceAsync(alias, _refChannelAliases[i], ct))
                    .ToList();
                await Task.WhenAll(materializationTasks);
            }

            if (!string.IsNullOrEmpty(_mainAlias))
            {
                // _mainAlias = logical SQL table name; _mainChannelAlias = physical channel alias.
                // We always use streaming FFI path for the main source to enable zero-copy processing.
                await RegisterStreamingChannelSourceAsync(_mainAlias, _mainChannelAlias, ct);
            }

            _logger.LogDebug("DataFusionProcessor: All sources registered. Inspecting schema...");
            InspectSchema();

            _columns = _resultSchema!.FieldsList
                .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrTypeFromField(f), f.IsNullable))
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DataFusionProcessor: OpenAsync FAILED: {Message}", ex.Message);
            throw;
        }
    }

    private unsafe void InspectSchema()
    {
        var ffiSchema = new CArrowSchema();
        if (DataFusionBridge.GetSchema(_ctx, _query, &ffiSchema) != 0)
            throw new Exception("Failed to get query schema from DataFusion");

        _resultSchema = CArrowSchemaImporter.ImportSchema(&ffiSchema);
    }

    private async Task RegisterStreamingChannelSourceAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Channel not found");
        var streamAdapter = new ChannelArrowStream(StripUnsupportedExtensions(schema), channelTuple.Channel.Reader, _logger, ct);

        _activeStreams.Add(streamAdapter);
        ExportAndRegisterStream(alias, streamAdapter);
    }

    private void ExportAndRegisterStream(string alias, IArrowArrayStream stream)
    {
        RegisterStreamSafe(alias, stream);
    }

    private unsafe void RegisterStreamSafe(string alias, IArrowArrayStream stream)
    {
        // Allocate on unmanaged heap because DataFusion stores this pointer
        // and reads from it asynchronously on background Tokio threads.
        var ffiStreamPtr = (CArrowArrayStream*)Marshal.AllocHGlobal(sizeof(CArrowArrayStream));

        CArrowArrayStreamExporter.ExportArrayStream(stream, ffiStreamPtr);
        if (DataFusionBridge.RegisterStream(_ctx, alias, ffiStreamPtr) != 0)
        {
            CArrowArrayStreamImporter.ImportArrayStream(ffiStreamPtr).Dispose();
            Marshal.FreeHGlobal((nint)ffiStreamPtr);
            throw new Exception($"Failed to register stream table {alias}");
        }
    }

    private async Task RegisterChannelSourceAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Channel not found");

        // Drain the channel upfront so all data is available before DataFusion plans the query.
        // This allows DataFusion to see the data as a MemTable with statistics, enabling better optimization (e.g., Broadcast Joins).
        var batches = new List<RecordBatch>();
        await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
            batches.Add(batch);

        RegisterBatchesSafe(alias, schema, batches);
    }

    private unsafe void RegisterBatchesSafe(string alias, Schema schema, List<RecordBatch> batches)
    {
        var exportSchema = ArrowFfiWorkaround.ReorderSchema(StripUnsupportedExtensions(schema));
        var ffiSchema = new CArrowSchema();
        CArrowSchemaExporter.ExportSchema(exportSchema, &ffiSchema);

        var numBatches = (nuint)batches.Count;
        var ffiBatchPointers = (CArrowArray**)Marshal.AllocHGlobal(IntPtr.Size * batches.Count);
        var allocatedArrays = new List<IntPtr>();

        try
        {
            for (int i = 0; i < batches.Count; i++)
            {
                var ffiArrayPtr = (CArrowArray*)Marshal.AllocHGlobal(Marshal.SizeOf<CArrowArray>());
                CArrowArrayExporter.ExportRecordBatch(ArrowFfiWorkaround.ReorderBatch(batches[i], exportSchema), ffiArrayPtr);
                ffiBatchPointers[i] = ffiArrayPtr;
                allocatedArrays.Add((IntPtr)ffiArrayPtr);
            }

            if (DataFusionBridge.RegisterBatches(_ctx, alias, &ffiSchema, ffiBatchPointers, numBatches) != 0)
            {
                throw new Exception($"Failed to register materialized table {alias}");
            }
        }
        finally
        {
            // Note: Rust side reads the structs from the pointers and takes ownership of the contents (release callbacks).
            // We only free the memory used to hold the pointers and the structs themselves.
            foreach (var ptr in allocatedArrays)
            {
                Marshal.FreeHGlobal(ptr);
            }
            Marshal.FreeHGlobal((IntPtr)ffiBatchPointers);
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_ctx == nint.Zero) yield break;

        nint ffiStreamPtr = System.Runtime.InteropServices.Marshal.AllocHGlobal(System.Runtime.InteropServices.Marshal.SizeOf<Apache.Arrow.C.CArrowArrayStream>());
        try
        {
            unsafe
            {
                if (DataFusionBridge.ExecuteStream(_ctx, _query, (Apache.Arrow.C.CArrowArrayStream*)ffiStreamPtr) != 0)
                {
                    throw new Exception("DataFusion native execution failed. Check logs for details.");
                }
            }

            IArrowArrayStream arrowStream;
            unsafe
            {
                arrowStream = Apache.Arrow.C.CArrowArrayStreamImporter.ImportArrayStream((Apache.Arrow.C.CArrowArrayStream*)ffiStreamPtr);
            }

            using (arrowStream as IDisposable)
            {
                while (true)
                {
                    RecordBatch? batch;
                    try
                    {
                        // Block the execution on a separate Task to avoid deadlocks on the ThreadPool from the synchronous C-API blocking loop inside Tokio Rust
                        batch = await Task.Run(async () => await arrowStream.ReadNextRecordBatchAsync(ct), ct);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error reading from Zero-Copy FFI stream: {Message}", ex.Message);
                        break;
                    }

                    if (batch == null) break;
                    yield return batch;
                }
            }
        }
        finally
        {
            System.Runtime.InteropServices.Marshal.FreeHGlobal(ffiStreamPtr);
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
            {
                yield return ConvertBatchToRows(recordBatch);
            }
        }
    }

    private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
        => SqlProcessorHelpers.ConvertBatchToRows(batch);

    public async ValueTask DisposeAsync()
    {
        if (_ctx != nint.Zero) { DataFusionBridge.ContextDestroy(_ctx); _ctx = nint.Zero; }
        if (_runtime != nint.Zero) { DataFusionBridge.RuntimeDestroy(_runtime); _runtime = nint.Zero; }
        await Task.CompletedTask;
    }

    private void ValidateAliases()
        => SqlProcessorHelpers.ValidateAliases(_mainChannelAlias, _refChannelAliases);

    private static void ValidateSchema(string alias, Schema schema)
        => SqlProcessorHelpers.ValidateSchema(alias, schema);

    // DataFusion 53.x does not handle Arrow extension types (e.g. arrow.uuid on FixedSizeBinary(16)):
    // it either crashes during execution or converts the column to Utf8.
    // Strip extension metadata so DataFusion sees plain storage types (e.g. FixedSizeBinary(16)).
    // The raw bytes are preserved unchanged; only the semantic annotation is removed.
    private static Schema StripUnsupportedExtensions(Schema schema)
    {
        bool anyStripped = false;
        var newFields = new List<Apache.Arrow.Field>(schema.FieldsList.Count);
        foreach (var field in schema.FieldsList)
        {
            if (field.Metadata != null &&
                (field.Metadata.ContainsKey("ARROW:extension:name") ||
                 field.Metadata.ContainsKey("ARROW:extension:metadata")))
            {
                var stripped = field.Metadata
                    .Where(kv => kv.Key != "ARROW:extension:name" && kv.Key != "ARROW:extension:metadata")
                    .ToDictionary(kv => kv.Key, kv => kv.Value);
                newFields.Add(new Apache.Arrow.Field(field.Name, field.DataType, field.IsNullable,
                    stripped.Count > 0 ? stripped : null));
                anyStripped = true;
            }
            else
            {
                newFields.Add(field);
            }
        }
        return anyStripped ? new Schema(newFields, schema.Metadata) : schema;
    }

}
