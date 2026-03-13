using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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
			var clrType = ArrowTypeMapper.GetClrType(field.DataType);
			columns.Add(new TargetColumnInfo(
				field.Name,
				field.DataType.Name,
				clrType,
				field.IsNullable,
				false, false, null, null, null));
		}
		return columns;
	}

	public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_path == "-")
		{
			_outputStream = Console.OpenStandardOutput();
            _isIpcFile = false;
		}
		else
		{
			_outputStream = new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
            _isIpcFile = _path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
                         _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
		}

		_schema = BuildSchema(columns);

        if (_isIpcFile)
            _arrowFileWriter = new ArrowFileWriter(_outputStream, _schema, leaveOpen: true);
        else
    		_arrowStreamWriter = new ArrowStreamWriter(_outputStream, _schema, leaveOpen: true);

		return ValueTask.CompletedTask;
	}

	private Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
	{
		var builder = new Schema.Builder();
		foreach (var col in columns)
		{
			builder.Field(new Field(col.Name, ArrowTypeMapper.GetArrowType(col.ClrType), col.IsNullable));
		}
		return builder.Build();
	}

	private IArrowType GetArrowType(Type type) => ArrowTypeMapper.GetArrowType(type);

	public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
        throw new NotSupportedException("ArrowAdapterDataWriter is purely columnar. Use IColumnarDataWriter.WriteRecordBatchAsync instead.");
	}

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
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
