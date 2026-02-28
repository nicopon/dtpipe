using System.Text;
using System.Runtime.InteropServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DataFusionSharp;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;
using Apache.Arrow.Ipc;

namespace DtPipe.XStreamers.DataFusion;

public sealed class DataFusionXStreamer : IStreamReader
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _query;
    private readonly string _mainAlias;
    private readonly string[] _refAliases;
    private readonly string _srcMain;
    private readonly string[] _srcRefs;
    private readonly ILogger<DataFusionXStreamer> _logger;

    private DataFusionRuntime? _runtime;
    private SessionContext? _ctx;
    private DataFrame? _df;
    private DataFrameStream? _stream;
    private Schema? _resultSchema;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private readonly List<string> _tempFiles = new();

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

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
        _logger.LogInformation("DataFusionXStreamer: OpenAsync — query=" + _query);
        try
        {
            _runtime = DataFusionRuntime.Create();
            _ctx = _runtime.CreateSessionContext();

            if (!string.IsNullOrEmpty(_srcMain))
            {
                await RegisterFileSourceAsync("main", _srcMain, ct);
                for (int i = 0; i < _refAliases.Length && i < _srcRefs.Length; i++)
                    await RegisterFileSourceAsync(_refAliases[i], _srcRefs[i], ct);
            }
            else
            {
                // CRITICAL: First register and collect reference tables
                for (int i = 0; i < _refAliases.Length; i++)
                {
                    _logger.LogInformation("DataFusionXStreamer: Registering ref [" + _refAliases[i] + "]...");
                    await RegisterChannelSourceAsync(_refAliases[i], _refAliases[i], ct);
                }

                if (!string.IsNullOrEmpty(_mainAlias))
                {
                    _logger.LogInformation("DataFusionXStreamer: Registering main [" + _mainAlias + "]...");
                    await RegisterStreamingChannelSourceAsync("main", _mainAlias, ct);
                }
            }

            _logger.LogInformation("DataFusionXStreamer: All sources registered. Executing SQL...");
            _df = await _ctx.SqlAsync(_query);
            _resultSchema = await _df.GetSchemaAsync();
            _stream = await _df.ExecuteStreamAsync();
            _columns = _resultSchema.FieldsList
                .Select(f => new PipeColumnInfo(f.Name, MapArrowType(f.DataType), f.IsNullable))
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DataFusionXStreamer: OpenAsync FAILED: " + ex.Message);
            throw;
        }
    }

    private async Task RegisterFileSourceAsync(string alias, string srcSpec, CancellationToken ct)
    {
        var colonIdx = srcSpec.IndexOf(':');
        if (colonIdx < 0) throw new ArgumentException("Source spec invalide: " + srcSpec);
        var provider = srcSpec[..colonIdx].ToLowerInvariant();
        var path = srcSpec[(colonIdx + 1)..];

        switch (provider)
        {
            case "parquet": await _ctx!.RegisterParquetAsync(alias, path); break;
            case "csv": await _ctx!.RegisterCsvAsync(alias, path); break;
            default: throw new NotSupportedException("Provider non supporté: " + provider);
        }
    }

    private async Task RegisterStreamingChannelSourceAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Canal introuvable");
        var streamAdapter = new ChannelArrowStream(schema, channelTuple.Channel.Reader, ct);
        await _ctx!.RegisterStreamAsync(alias, streamAdapter);
    }

    private async Task RegisterChannelSourceAsync(string alias, string channelAlias, CancellationToken ct)
    {
        var schema = await _registry.WaitForArrowChannelSchemaAsync(channelAlias, ct);
        var channelTuple = _registry.GetArrowChannel(channelAlias) ?? throw new Exception("Canal introuvable");
        var batches = new List<RecordBatch>();
        try
        {
            await foreach (var batch in channelTuple.Channel.Reader.ReadAllAsync(ct))
            {
                batches.Add(batch); // Zero-Copy Transfer
            }
            await _ctx!.RegisterBatchesAsync(alias, batches);
        }
        finally
        {
            // After RegisterBatchesAsync, DataFusion has imported them via FFI.
            // We can dispose the .NET handles; the native memory stays alive until Rust calls release.
            foreach (var b in batches) b.Dispose();
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_stream == null) yield break;
        await foreach (var recordBatch in _stream)
        {
            if (ct.IsCancellationRequested) yield break;
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
                rows[r][c] = col == null ? null : GetValue(col, r);
            }
        }
        return rows;
    }

    private static object? GetValue(IArrowArray col, int idx) => col switch
    {
        Int64Array a  => a.GetValue(idx),
        Int32Array a  => a.GetValue(idx),
        Int16Array a  => a.GetValue(idx),
        Int8Array a   => a.GetValue(idx),
        DoubleArray a => a.GetValue(idx),
        FloatArray a  => a.GetValue(idx),
        BooleanArray a => a.GetValue(idx),
        StringArray a => a.GetString(idx),
        Date32Array a => a.GetDateTimeOffset(idx),
        TimestampArray a => a.GetTimestamp(idx),
        _ => col.ToString()
    };

    private static Type MapArrowType(Apache.Arrow.Types.IArrowType t) => t.TypeId switch
    {
        Apache.Arrow.Types.ArrowTypeId.Int64   => typeof(long),
        Apache.Arrow.Types.ArrowTypeId.Int32   => typeof(int),
        Apache.Arrow.Types.ArrowTypeId.Int16   => typeof(short),
        Apache.Arrow.Types.ArrowTypeId.Double  => typeof(double),
        Apache.Arrow.Types.ArrowTypeId.Float   => typeof(float),
        Apache.Arrow.Types.ArrowTypeId.Boolean => typeof(bool),
        Apache.Arrow.Types.ArrowTypeId.String  => typeof(string),
        _                                      => typeof(string)
    };

    public async ValueTask DisposeAsync()
    {
        _stream?.Dispose();
        _df?.Dispose();
        _ctx?.Dispose();
        _runtime?.Dispose();
        foreach (var f in _tempFiles) try { File.Delete(f); } catch { }
        await Task.CompletedTask;
    }

    private sealed class ChannelArrowStream : IArrowArrayStream
    {
        private readonly Schema _schema;
        private readonly ChannelReader<RecordBatch> _reader;
        private readonly CancellationToken _ct;

        public ChannelArrowStream(Schema schema, ChannelReader<RecordBatch> reader, CancellationToken ct) { _schema = schema; _reader = reader; _ct = ct; }
        public Schema Schema => _schema;

        public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_ct, cancellationToken);
                if (await _reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false) && _reader.TryRead(out var batch))
                {
                    // Zero-Copy: Return the batch directly.
                    // DataFusion (via FFI importer) will take ownership and handle release.
                    return batch;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.WriteLine("[ERROR] ChannelArrowStream: " + ex.Message); throw; }
            return null;
        }

        public void Dispose() { }
    }
}
