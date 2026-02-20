using Apache.Arrow;
using Apache.Arrow.Ipc;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.Arrow;

public class ArrowAdapterStreamReader : IStreamReader
{
	private readonly string _path;
	private readonly ArrowReaderOptions _options;
	private readonly ILogger _logger;

	private Stream? _inputStream;
	private Apache.Arrow.Ipc.ArrowStreamReader? _arrowReader;
	private ArrowFileReader? _arrowFileReader;
    private bool _isIpcFile;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	public ArrowAdapterStreamReader(string path, ArrowReaderOptions options, ILogger? logger = null)
	{
		_path = path;
		_options = options;
		_logger = logger ?? NullLogger.Instance;
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_path) || _path == "-")
		{
			if (!Console.IsInputRedirected)
			{
				throw new InvalidOperationException("Structure input (STDIN) is not redirected.");
			}
			_inputStream = Console.OpenStandardInput();
            _isIpcFile = false;
		}
		else
		{
			if (!File.Exists(_path))
				throw new FileNotFoundException($"Arrow file not found: {_path}", _path);

			_inputStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);

            _isIpcFile = _path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
                         _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
		}

        if (_isIpcFile)
        {
            _arrowFileReader = new ArrowFileReader(_inputStream);
            var schema = _arrowFileReader.Schema;
            Columns = MapSchema(schema);
        }
        else
        {
            _arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(_inputStream);
            var schema = _arrowReader.Schema;
            Columns = MapSchema(schema);
        }
	}

    private List<PipeColumnInfo> MapSchema(Schema schema)
    {
        return schema.FieldsList.Select(f => new PipeColumnInfo(
            f.Name,
            MapType(f.DataType),
            f.IsNullable
        )).ToList();
    }

    private Type MapType(Apache.Arrow.Types.IArrowType type)
    {
        return type.TypeId switch
        {
            Apache.Arrow.Types.ArrowTypeId.Boolean => typeof(bool),
            Apache.Arrow.Types.ArrowTypeId.Int8 => typeof(sbyte),
            Apache.Arrow.Types.ArrowTypeId.UInt8 => typeof(byte),
            Apache.Arrow.Types.ArrowTypeId.Int16 => typeof(short),
            Apache.Arrow.Types.ArrowTypeId.UInt16 => typeof(ushort),
            Apache.Arrow.Types.ArrowTypeId.Int32 => typeof(int),
            Apache.Arrow.Types.ArrowTypeId.UInt32 => typeof(uint),
            Apache.Arrow.Types.ArrowTypeId.Int64 => typeof(long),
            Apache.Arrow.Types.ArrowTypeId.UInt64 => typeof(ulong),
            Apache.Arrow.Types.ArrowTypeId.Float => typeof(float),
            Apache.Arrow.Types.ArrowTypeId.Double => typeof(double),
            Apache.Arrow.Types.ArrowTypeId.String => typeof(string),
            Apache.Arrow.Types.ArrowTypeId.Binary => typeof(byte[]),
            Apache.Arrow.Types.ArrowTypeId.Timestamp => typeof(DateTimeOffset),
            Apache.Arrow.Types.ArrowTypeId.Date32 => typeof(DateTime),
            Apache.Arrow.Types.ArrowTypeId.Date64 => typeof(DateTime),
            _ => typeof(string)
        };
    }

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (Columns is null) throw new InvalidOperationException("Call OpenAsync first.");

        if (_isIpcFile && _arrowFileReader != null)
        {
            for (int i = 0; ; i++)
            {
                RecordBatch? batch = null;
                try { batch = await _arrowFileReader.ReadRecordBatchAsync(i, ct); } catch { break; }
                if (batch == null) break;

                foreach (var memory in FlattenBatch(batch, batchSize))
                {
                    yield return memory;
                }
            }
        }
        else if (_arrowReader != null)
        {
            while (true)
            {
                var batch = await _arrowReader.ReadNextRecordBatchAsync(ct);
                if (batch == null) break;

                foreach (var memory in FlattenBatch(batch, batchSize))
                {
                    yield return memory;
                }
            }
        }
	}

    private IEnumerable<ReadOnlyMemory<object?[]>> FlattenBatch(RecordBatch batch, int requestedBatchSize)
    {
        var rowCount = batch.Length;
        var colCount = batch.ColumnCount;
        var flatBatch = new object?[requestedBatchSize][];
        var currentIndex = 0;

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
        {
            var row = new object?[colCount];
            for (int colIdx = 0; colIdx < colCount; colIdx++)
            {
                var column = batch.Column(colIdx);
                row[colIdx] = GetValue(column, rowIdx);
            }

            flatBatch[currentIndex++] = row;

            if (currentIndex >= requestedBatchSize)
            {
                yield return new ReadOnlyMemory<object?[]>(flatBatch, 0, currentIndex);
                flatBatch = new object?[requestedBatchSize][];
                currentIndex = 0;
            }
        }

        if (currentIndex > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(flatBatch, 0, currentIndex);
        }
    }

    private object? GetValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            StringArray s => s.GetString(index),
            Int32Array i => i.GetValue(index),
            Int64Array l => l.GetValue(index),
            DoubleArray d => d.GetValue(index),
            BooleanArray b => b.GetValue(index),
            FloatArray f => f.GetValue(index),
            TimestampArray t => t.GetTimestamp(index),
            Date32Array d32 => d32.GetDateTime(index),
            BinaryArray bin => bin.GetBytes(index).ToArray(),
            _ => array.GetValue(index) // Fallback for other simple types
        };
    }

	public async ValueTask DisposeAsync()
	{
		if (_inputStream != null)
		{
			await _inputStream.DisposeAsync();
			_inputStream = null;
		}
	}
}

internal static class ArrowExtensions
{
    // Basic helper to get value from array if not handled explicitly
    public static object? GetValue(this IArrowArray array, int index)
    {
         // This is a simplified fallback. Real implementation might need more depth.
         var value = array.GetType().GetMethod("GetValue")?.Invoke(array, [index]);
         return value;
    }
}
