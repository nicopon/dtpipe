using Apache.Arrow;
using Apache.Arrow.Ipc;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.Arrow;

public class ArrowAdapterStreamReader : IColumnarStreamReader
{
	private readonly string _path;
	private readonly ArrowReaderOptions _options;
	private readonly ILogger _logger;

	private Stream? _inputStream;
	private Apache.Arrow.Ipc.ArrowStreamReader? _arrowReader;
	private ArrowFileReader? _arrowFileReader;
    private bool _isIpcFile;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema => _isIpcFile ? _arrowFileReader?.Schema : _arrowReader?.Schema;

	public ArrowAdapterStreamReader(string path, ArrowReaderOptions options, ILogger? logger = null)
	{
		_path = path;
		_options = options;
		_logger = logger ?? NullLogger.Instance;
	}

	public Task OpenAsync(CancellationToken ct = default)
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

        return Task.CompletedTask;
	}

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Columns is null) throw new InvalidOperationException("Call OpenAsync first.");

        if (_isIpcFile && _arrowFileReader != null)
        {
            for (int i = 0; ; i++)
            {
                RecordBatch? batch = null;
                try { batch = await _arrowFileReader.ReadRecordBatchAsync(i, ct); } catch { break; }
                if (batch == null) break;
                yield return batch;
            }
        }
        else if (_arrowReader != null)
        {
            while (true)
            {
                var batch = await _arrowReader.ReadNextRecordBatchAsync(ct);
                if (batch == null) break;
                yield return batch;
            }
        }
    }

    private List<PipeColumnInfo> MapSchema(Schema schema)
    {
        return schema.FieldsList.Select(f => new PipeColumnInfo(
            f.Name,
            ArrowTypeMapper.GetClrTypeFromField(f),
            f.IsNullable
        )).ToList();
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
                row[colIdx] = ArrowTypeMapper.GetValueForField(column, batch.Schema.GetFieldByIndex(colIdx), rowIdx);
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


	public async ValueTask DisposeAsync()
	{
		if (_inputStream != null)
		{
			await _inputStream.DisposeAsync();
			_inputStream = null;
		}
	}
}
