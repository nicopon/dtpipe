using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using System.IO;
using System.IO.Pipes;
using System.Threading.Channels;

namespace DtPipe.XStreamers.DataFusion;

public sealed class DataFusionXStreamer : IColumnarStreamReader
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string[] _refAliases;
    private readonly string _srcMain;
    private readonly string[] _srcRefs;
    private readonly ILogger<DataFusionXStreamer> _logger;

    private nint _runtime = nint.Zero;
    private nint _ctx = nint.Zero;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private readonly List<string> _tempFiles = new();
    private readonly List<IArrowArrayStream> _activeStreams = new();

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _resultSchema;

    public DataFusionXStreamer(
        IMemoryChannelRegistry registry,
        string query,
        string mainAlias,
        string[] refAliases,
        string srcMain,
        string[] srcRefs,
        ILogger<DataFusionXStreamer> logger)
    {
        _registry = registry;
        _query = query;
        _mainAlias = mainAlias;
        _refAliases = refAliases;
        _srcMain = srcMain;
        _srcRefs = srcRefs;
        _logger = logger;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _logger.LogDebug("DataFusionXStreamer: OpenAsync — query={Query}", _query);
        try
        {
            _runtime = DataFusionBridge.RuntimeNew();
            if (_runtime == nint.Zero) throw new Exception("Failed to create DataFusion runtime");

            ValidateAliases();

            _ctx = DataFusionBridge.ContextNew(_runtime);
            if (_ctx == nint.Zero) throw new Exception("Failed to create DataFusion context");

            if (!string.IsNullOrEmpty(_srcMain))
            {
                RegisterFileSource("main", _srcMain);
                for (int i = 0; i < _refAliases.Length && i < _srcRefs.Length; i++)
                    RegisterFileSource(_refAliases[i], _srcRefs[i]);
            }
            else
            {
                if (_refAliases.Length > 0)
                {
                    var materializationTasks = _refAliases.Select(alias => RegisterChannelSourceAsync(alias, alias, ct)).ToList();
                    await Task.WhenAll(materializationTasks);
                }

                if (!string.IsNullOrEmpty(_mainAlias))
                {
                    await RegisterStreamingChannelSourceAsync(_mainAlias, _mainAlias, ct);
                }
            }

            _logger.LogDebug("DataFusionXStreamer: All sources registered. Inspecting schema...");
            InspectSchema();

            _columns = _resultSchema!.FieldsList
                .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrType(f.DataType), f.IsNullable))
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DataFusionXStreamer: OpenAsync FAILED: {Message}", ex.Message);
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

    private void RegisterFileSource(string alias, string srcSpec)
    {
        var colonIdx = srcSpec.IndexOf(':');
        if (colonIdx < 0) throw new ArgumentException("Source spec invalide: " + srcSpec);
        var provider = srcSpec[..colonIdx].ToLowerInvariant();
        var path = srcSpec[(colonIdx + 1)..];

        switch (provider)
        {
            case "parquet":
                if (DataFusionBridge.RegisterParquet(_ctx, alias, path) != 0)
                    throw new Exception($"Failed to register parquet table {alias}");
                break;
            case "csv":
                if (DataFusionBridge.RegisterCsv(_ctx, alias, path) != 0)
                    throw new Exception($"Failed to register csv table {alias}");
                break;
            default: throw new NotSupportedException("Provider non supporté (bridged mode): " + provider);
        }
    }

    private async Task RegisterStreamingChannelSourceAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        ValidateSchema(channelAlias, schema);
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Canal introuvable");
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
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Canal introuvable");

        var batches = new List<RecordBatch>();
        await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
        {
            batches.Add(batch);
        }

        if (batches.Count == 0) return;

        RegisterBatchesSafe(alias, schema, batches);
    }

    private unsafe void RegisterBatchesSafe(string alias, Schema schema, List<RecordBatch> batches)
    {
        var ffiSchema = new CArrowSchema();
        CArrowSchemaExporter.ExportSchema(schema, &ffiSchema);

        var ffiArrays = new CArrowArray[batches.Count];
        var ffiBatchPtrs = new CArrowArray*[batches.Count];

        fixed (CArrowArray* pArrays = ffiArrays)
        {
            for (int i = 0; i < batches.Count; i++)
            {
                CArrowArrayExporter.ExportRecordBatch(batches[i], &pArrays[i]);
                ffiBatchPtrs[i] = &pArrays[i];
            }

            fixed (CArrowArray** ppBatches = ffiBatchPtrs)
            {
                if (DataFusionBridge.RegisterBatches(_ctx, alias, &ffiSchema, ppBatches, (nuint)batches.Count) != 0)
                {
                    throw new Exception($"Failed to register batches for {alias}");
                }
            }
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_ctx == nint.Zero) yield break;

        using var pipeServer = new AnonymousPipeServerStream(PipeDirection.In, HandleInheritability.None);

#if NET
        int writeFd = (int)pipeServer.ClientSafePipeHandle.DangerousGetHandle();
#else
        int writeFd = (int)pipeServer.SafePipeHandle.DangerousGetHandle();
#endif

        var sql = _query;
        var ctx = _ctx;
        var writeTask = Task.Run(() => {
            try
            {
                if (DataFusionBridge.ExecuteToFd(ctx, sql, writeFd) != 0)
                {
                    _logger.LogError("Bridge execution failed for query: {Query}", sql);
                    return false;
                }
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception in bridge execution: {Message}", ex.Message);
                return false;
            }
        });

        using (var reader = new ArrowStreamReader(pipeServer))
        {
            while (true)
            {
                RecordBatch? batch;
                try
                {
                    batch = await reader.ReadNextRecordBatchAsync(ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading from IPC pipe: {Message}", ex.Message);
                    break;
                }

                if (batch == null) break;
                yield return batch;
            }
        }

        var success = await writeTask;
        if (!success)
        {
            throw new Exception("DataFusion native execution failed. Check logs for details.");
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
	{
		var rows = new object?[batch.Length][];
		for (int r = 0; r < batch.Length; r++)
		{
			rows[r] = new object?[batch.ColumnCount];
			for (int c = 0; c < batch.ColumnCount; c++)
			{
				var col = batch.Column(c);
				rows[r][c] = col == null ? null : ArrowTypeMapper.GetValue(col, r);
			}
		}
		return rows;
	}

    public async ValueTask DisposeAsync()
    {
        if (_ctx != nint.Zero) { DataFusionBridge.ContextDestroy(_ctx); _ctx = nint.Zero; }
        if (_runtime != nint.Zero) { DataFusionBridge.RuntimeDestroy(_runtime); _runtime = nint.Zero; }
        foreach (var f in _tempFiles) try { File.Delete(f); } catch { }
        await Task.CompletedTask;
    }

    private void ValidateAliases()
    {

        var aliases = new List<string>();
        if (!string.IsNullOrEmpty(_mainAlias)) aliases.Add(_mainAlias);
        aliases.AddRange(_refAliases);

        var groups = aliases.GroupBy(a => a.ToLowerInvariant())
                            .Where(g => g.Count() > 1)
                            .ToList();

        if (groups.Any())
        {
            var duplicates = string.Join(", ", groups.Select(g => $"'{string.Join("' vs '", g)}'"));
            throw new InvalidOperationException($"Case ambiguity detected in stream aliases: {duplicates}");
        }
    }

    private void ValidateSchema(string alias, Schema schema)
    {

        var groups = schema.FieldsList.GroupBy(f => f.Name.ToLowerInvariant())
                                     .Where(g => g.Count() > 1)
                                     .ToList();

        if (groups.Any())
        {
            var duplicates = string.Join(", ", groups.Select(g => $"'{string.Join("' vs '", g)}'"));
            throw new InvalidOperationException($"Case ambiguity detected in columns for stream '{alias}': {duplicates}");
        }
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
