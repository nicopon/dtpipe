using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using System.IO.Pipes;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

/// <summary>
/// Writes data to Arrow IPC format.
/// This writer is now purely columnar and relies on the engine to provide RecordBatches.
/// </summary>
public sealed class ArrowAdapterDataWriter : IColumnarDataWriter, IRequiresOptions<ArrowWriterOptions>, ISchemaInspector
{
    private readonly string _path;
    private readonly ArrowWriterOptions _options;

    private Stream? _outputStream;
    private ArrowStreamWriter? _arrowStreamWriter;
    private ArrowFileWriter? _arrowFileWriter;
    private bool _isIpcFile;
    private Schema? _schema;

    public ArrowAdapterDataWriter(string path) : this(path, new ArrowWriterOptions())
    {
    }

    public ArrowAdapterDataWriter(string path, ArrowWriterOptions options)
    {
        _path = path;
        _options = options;
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        using (batch)
        {
            // The first batch declares the stream: only a batch carries a timestamp's zone and the
            // metadata of an extension type, which the columns cannot. A later batch is written
            // under that header, so one that differs is refused rather than written unreadable.
            if (!IsOpen) OpenIpcWriter(batch.Schema);
            else RequireHeaderSchema(batch.Schema);
            if (_isIpcFile)
                await _arrowFileWriter!.WriteRecordBatchAsync(batch, ct);
            else
                await _arrowStreamWriter!.WriteRecordBatchAsync(batch, ct);
        }
    }

    public bool RequiresTargetInspection => false;

    public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        if (_path == "-")
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
        }

        if (string.IsNullOrEmpty(_path))
        {
            throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

        // A pipe location is never a filesystem path: File.Exists would read it as one relative to
        // the current directory and (harmlessly, but wrongly) report it as absent. Its state is
        // unknown until connected, which InitializeAsync is about to do.
        if (ArrowPipeLocation.TryParse(_path, out _))
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, null, null, null));
        }

        if (!File.Exists(_path))
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
        }

        try
        {
            using var fs = File.OpenRead(_path);

            // Try reading as file first (IPC file format)
            if (_path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) || _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    using var reader = new ArrowFileReader(fs);
                    var schema = reader.Schema;
                    var columns = MapArrowSchema(schema);
                    return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo(columns, true, null, fs.Length, null));
                }
                catch { /* Fallback to stream */ }
            }

            // Try as stream (IPC stream format)
            fs.Position = 0;
            using var streamReader = new ArrowStreamReader(fs);
            var streamSchema = streamReader.Schema;
            var streamColumns = MapArrowSchema(streamSchema);
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo(streamColumns, true, null, fs.Length, null));
        }
        catch
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, null, new FileInfo(_path).Length, null));
        }
    }

    private IReadOnlyList<TargetColumnInfo> MapArrowSchema(Schema schema)
    {
        var columns = new List<TargetColumnInfo>();
        foreach (var field in schema.FieldsList)
        {
            var clrType = ArrowTypeMapper.GetClrTypeFromField(field);
            columns.Add(new TargetColumnInfo(
                field.Name,
                field.DataType.Name,
                clrType,
                field.IsNullable,
                false, false, null, null, null));
        }
        return columns;
    }

    // See ArrowAdapterStreamReader.PipeConnectTimeout: the pipeline node creates the pipe before
    // launching the child that writes this side of it, so the bound only guards against a
    // misconfigured one.
    private static readonly TimeSpan PipeConnectTimeout = TimeSpan.FromSeconds(10);

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        if (_path == "-")
        {
            _outputStream = Console.OpenStandardOutput();
            _isIpcFile = false;
        }
        else if (ArrowPipeLocation.TryParse(_path, out var pipeName))
        {
            var client = new NamedPipeClientStream(".", pipeName, PipeDirection.Out, PipeOptions.Asynchronous);
            try
            {
                await client.ConnectAsync((int)PipeConnectTimeout.TotalMilliseconds, ct);
            }
            catch (TimeoutException ex)
            {
                client.Dispose();
                throw new TimeoutException(
                    $"Arrow writer could not connect to named pipe '{pipeName}' within {PipeConnectTimeout.TotalSeconds:F0}s. " +
                    "Nothing was listening as a server on it.", ex);
            }
            _outputStream = client;
            _isIpcFile = false;
        }
        else
        {
            _outputStream = new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.Asynchronous);
            _isIpcFile = _path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
                         _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
        }

        // What a stream with no batch declares; replaced by the first batch's own schema.
        _schema = BuildSchema(columns);
    }

    private bool IsOpen => _arrowStreamWriter != null || _arrowFileWriter != null;

    private void RequireHeaderSchema(Schema batchSchema)
    {
        var header = _schema!;
        if (ReferenceEquals(header, batchSchema)) return;

        var expected = header.FieldsList;
        var actual = batchSchema.FieldsList;
        if (expected.Count != actual.Count)
            throw Drift($"it carries {actual.Count} columns where the stream declared {expected.Count}");

        for (var i = 0; i < expected.Count; i++)
        {
            var key = FieldKey(expected[i]);
            if (key == FieldKey(actual[i])) continue;
            throw Drift($"column {i} '{actual[i].Name}' is {key} in the stream and {FieldKey(actual[i])} in this batch");
        }
    }

    private static string FieldKey(Field field)
        => ArrowSchemaSerializer.SerializeCompact(new Schema([field], null));

    private static InvalidOperationException Drift(string detail)
        => new($"Arrow writer: a batch differs from the stream's schema, which the first batch fixed: {detail}.");

    private void OpenIpcWriter(Schema schema)
    {
        if (_arrowStreamWriter != null || _arrowFileWriter != null) return;

        _schema = schema;
        if (_isIpcFile)
            _arrowFileWriter = new ArrowFileWriter(_outputStream!, schema, leaveOpen: true);
        else
            _arrowStreamWriter = new ArrowStreamWriter(_outputStream!, schema, leaveOpen: true);
    }

    private static Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
        => ArrowSchemaFactory.Create(columns);

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (_arrowStreamWriter == null && _arrowFileWriter == null && _schema != null)
        {
            // No batch arrived. The end marker alone carries no schema, and a reader of an empty
            // stream (the next fragment of a distributed pipeline) must still learn the columns.
            OpenIpcWriter(_schema);
            if (_arrowStreamWriter != null) await _arrowStreamWriter.WriteStartAsync(ct);
            if (_arrowFileWriter != null) await _arrowFileWriter.WriteStartAsync(ct);
        }

        if (_arrowStreamWriter != null)
        {
            await _arrowStreamWriter.WriteEndAsync(ct);
        }
        if (_arrowFileWriter != null)
        {
            await _arrowFileWriter.WriteEndAsync(ct);
        }
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        throw new NotSupportedException("Executing raw commands is not supported for Arrow targets.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_arrowStreamWriter != null)
        {
            _arrowStreamWriter.Dispose();
            _arrowStreamWriter = null;
        }
        if (_arrowFileWriter != null)
        {
            _arrowFileWriter.Dispose();
            _arrowFileWriter = null;
        }
        if (_outputStream != null)
        {
            await _outputStream.DisposeAsync();
            _outputStream = null;
        }
    }
}
