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
                // Fallback to materialization if the schema contains nested structures.
                // DataFusion FFI string stream has difficulties passing structs on initial registration
                // before full row evaluations (or the crate has a CData limitation).
                var schema = await _registry.WaitForArrowChannelSchemaAsync(_mainChannelAlias, ct);
                bool hasComplexTypes = schema.FieldsList.Any(f => f.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.Struct || f.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.List);
                
                if (hasComplexTypes)
                {
                    await RegisterChannelSourceAsync(_mainAlias, _mainChannelAlias, ct);
                }
                else
                {
                    await RegisterStreamingChannelSourceAsync(_mainAlias, _mainChannelAlias, ct);
                }
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
        var streamAdapter = new ChannelArrowStream(schema, channelTuple.Channel.Reader, _logger, ct);

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
        // This is required for struct/list columns whose schema must be known at registration time.
        var batches = new List<RecordBatch>();
        await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
            batches.Add(batch);

        // Use the streaming FFI path (dtfb_register_stream) rather than dtfb_register_batches.
        // CArrowArrayExporter.ExportRecordBatch fails on dictionary-encoded columns (e.g. Parquet
        // string columns read back as DictionaryArray<Int32, String>) — buffer #1 (data/indices)
        // is treated as null by the exporter for those types. The stream path uses
        // CArrowArrayStreamExporter which handles all Arrow types correctly.
        var streamAdapter = new BatchListArrowStream(schema, batches);
        _activeStreams.Add(streamAdapter); // prevent GC until DataFusion releases the stream
        ExportAndRegisterStream(alias, streamAdapter);
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

    /// <summary>
    /// Wraps a pre-drained list of RecordBatches as an IArrowArrayStream.
    /// Allows using the stream FFI path (dtfb_register_stream) for materialized data,
    /// avoiding CArrowArrayExporter limitations with dictionary-encoded columns.
    /// </summary>
    private sealed class BatchListArrowStream : IArrowArrayStream
    {
        private readonly Schema _schema;
        private readonly List<RecordBatch> _batches;
        private int _index;

        public BatchListArrowStream(Schema schema, List<RecordBatch> batches)
        {
            _schema = schema;
            _batches = batches;
        }

        public Schema Schema => _schema;

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            => new(_index < _batches.Count ? _batches[_index++] : null);

        public void Dispose() { }
    }

    private sealed class ChannelArrowStream : IArrowArrayStream
    {
        private readonly Schema _schema;
        private readonly ChannelReader<RecordBatch> _reader;
        private readonly ILogger _logger;
        private readonly CancellationToken _ct;

        public ChannelArrowStream(Schema schema, ChannelReader<RecordBatch> reader, ILogger logger, CancellationToken ct)
        {
            _schema = schema;
            _reader = reader;
            _logger = logger;
            _ct = ct;
        }
        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_ct, cancellationToken);
                if (await _reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false) && _reader.TryRead(out var batch))
                {
                    return batch;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "ChannelArrowStream Error: {Message}", ex.Message); throw; }
            return null;
        }

        public void Dispose() { }
    }
}
