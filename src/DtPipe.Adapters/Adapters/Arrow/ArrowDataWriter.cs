using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

public sealed class ArrowAdapterDataWriter : IDataWriter, IRequiresOptions<ArrowWriterOptions>, ISchemaInspector
{
	private readonly string _path;
	private readonly ArrowWriterOptions _options;

	private Stream? _outputStream;
	private ArrowStreamWriter? _arrowStreamWriter;
    private ArrowFileWriter? _arrowFileWriter;
    private bool _isIpcFile;
	private Schema? _schema;
	private List<IArrowArrayBuilder>? _builders;
	private int _rowsInBuffer;

	public ArrowAdapterDataWriter(string path) : this(path, new ArrowWriterOptions())
	{
	}

	public ArrowAdapterDataWriter(string path, ArrowWriterOptions options)
	{
		_path = path;
		_options = options;
	}

	public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_path) || _path == "-")
		{
			return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
		}

		if (!File.Exists(_path))
		{
			return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
		}

		return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, null, new FileInfo(_path).Length, null));
	}

	public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_path) || _path == "-")
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
		_builders = CreateBuilders(_schema);

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
			builder.Field(new Field(col.Name, GetArrowType(col.ClrType), col.IsNullable));
		}
		return builder.Build();
	}

	private IArrowType GetArrowType(Type type)
	{
		var baseType = Nullable.GetUnderlyingType(type) ?? type;

		if (baseType == typeof(string)) return StringType.Default;
		if (baseType == typeof(bool)) return BooleanType.Default;
		if (baseType == typeof(int)) return Int32Type.Default;
		if (baseType == typeof(long)) return Int64Type.Default;
		if (baseType == typeof(float)) return FloatType.Default;
		if (baseType == typeof(double)) return DoubleType.Default;
		if (baseType == typeof(decimal)) return new Decimal128Type(38, 10);
		if (baseType == typeof(DateTime)) return Date64Type.Default;
		if (baseType == typeof(DateTimeOffset)) return TimestampType.Default;
		if (baseType == typeof(byte[])) return BinaryType.Default;

		return StringType.Default;
	}

	private List<IArrowArrayBuilder> CreateBuilders(Schema schema)
	{
		var builders = new List<IArrowArrayBuilder>();
		foreach (var field in schema.FieldsList)
		{
			builders.Add(CreateBuilder(field.DataType));
		}
		return builders;
	}

	private IArrowArrayBuilder CreateBuilder(IArrowType type)
	{
		return type.TypeId switch
		{
			ArrowTypeId.Boolean => new BooleanArray.Builder(),
			ArrowTypeId.Int32 => new Int32Array.Builder(),
			ArrowTypeId.Int64 => new Int64Array.Builder(),
			ArrowTypeId.Double => new DoubleArray.Builder(),
			ArrowTypeId.Float => new FloatArray.Builder(),
			ArrowTypeId.String => new StringArray.Builder(),
			ArrowTypeId.Timestamp => new TimestampArray.Builder(),
			ArrowTypeId.Date64 => new Date64Array.Builder(),
			ArrowTypeId.Binary => new BinaryArray.Builder(),
			_ => new StringArray.Builder()
		};
	}

	public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_builders == null || (_arrowStreamWriter == null && _arrowFileWriter == null))
			throw new InvalidOperationException("Call InitializeAsync first.");

		foreach (var row in rows)
		{
			for (int i = 0; i < row.Length; i++)
			{
				AppendValue(_builders[i], row[i]);
			}
			_rowsInBuffer++;

			if (_rowsInBuffer >= _options.BatchSize)
			{
				await FlushCurrentBatchAsync(ct);
			}
		}
	}

	private void AppendValue(IArrowArrayBuilder builder, object? value)
	{
		if (value == null)
		{
            ((dynamic)builder).AppendNull();
			return;
		}

        ((dynamic)builder).Append((dynamic)value);
	}

	private async Task FlushCurrentBatchAsync(CancellationToken ct)
	{
		if (_builders == null || (_arrowStreamWriter == null && _arrowFileWriter == null) || _rowsInBuffer == 0) return;

		var arrays = _builders.Select(b => (IArrowArray)((dynamic)b).Build(null)).ToList();
		var batch = new RecordBatch(_schema, arrays, _rowsInBuffer);

        if (_isIpcFile)
            await _arrowFileWriter!.WriteRecordBatchAsync(batch, ct);
        else
		    await _arrowStreamWriter!.WriteRecordBatchAsync(batch, ct);

        _builders = CreateBuilders(_schema!);
		_rowsInBuffer = 0;
	}

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		await FlushCurrentBatchAsync(ct);
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
