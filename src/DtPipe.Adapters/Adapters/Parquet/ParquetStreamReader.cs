using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet;
using Parquet.Schema;

namespace DtPipe.Adapters.Parquet;

public class ParquetStreamReader : IColumnarStreamReader
{
	private readonly string _filePath;
	private readonly ILogger _logger;
	private ParquetReader? _reader;
	private readonly SemaphoreSlim _semaphore = new(1, 1);
	private FileStream? _fileStream;
	private bool _isReading;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema { get; private set; }

	public ParquetStreamReader(string filePath, ILogger? logger = null)
	{
		_filePath = filePath;
		_logger = logger ?? NullLogger.Instance;
	}

	private static async Task ValidateFileAccessAsync(string filePath, CancellationToken ct)
	{
		if (!File.Exists(filePath))
			throw new FileNotFoundException($"Parquet file not found: {filePath}", filePath);

		var fileInfo = new FileInfo(filePath);
		if (fileInfo.Length == 0)
			throw new InvalidOperationException($"Parquet file is empty: {filePath}");

		// Retry loop to handle transient locks or filesystem lag (especially on Mac/Unix)
		int retries = 5;
		while (retries > 0)
		{
			try
			{
				// Test read access with minimal I/O
				await using var testStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1, FileOptions.None);
				var buffer = new byte[1];
				_ = await testStream.ReadAsync(buffer.AsMemory(0, 1), ct);
				return;
			}
			catch (Exception ex) when (retries > 1 && (ex is IOException || ex.GetType().Name == "AccessViolationException"))
			{
				retries--;
				await Task.Delay(100, ct);
			}
			catch
			{
				if (retries <= 1) throw;
				retries--;
				await Task.Delay(100, ct);
			}
		}
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		// Validate file access before opening
		await ValidateFileAccessAsync(_filePath, ct);

		if (_logger.IsEnabled(LogLevel.Debug))
			_logger.LogDebug("Opening Parquet file: {FilePath}", _filePath);

		_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
		_reader = await ParquetReader.CreateAsync(_fileStream, leaveStreamOpen: true, cancellationToken: ct);

		var schema = _reader.Schema;
		var columns = new List<PipeColumnInfo>();

		foreach (var field in schema.Fields)
		{
			if (field is DataField dataField)
			{
				columns.Add(new PipeColumnInfo(dataField.Name, dataField.ClrType, dataField.IsNullable));
			}
		}

		Columns = columns;
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		await _semaphore.WaitAsync(ct);
		try
		{
			if (_isReading)
				throw new InvalidOperationException("Concurrent reads from the same ParquetStreamReader are not supported.");
			_isReading = true;
		}
		finally
		{
			_semaphore.Release();
		}

		try
		{
			var schema = DtPipe.Core.Infrastructure.Arrow.ArrowSchemaFactory.Create(Columns);

			for (int rowGroupIndex = 0; rowGroupIndex < _reader.RowGroupCount; rowGroupIndex++)
			{
				ct.ThrowIfCancellationRequested();

				using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
				var rowCount = (int)rowGroupReader.RowCount;
				var arrays = new List<IArrowArray>();

				foreach (var field in _reader.Schema.DataFields)
				{
					var columnData = await ReadColumnDataAsArrayAsync(rowGroupReader, field, ct);
					arrays.Add(DtPipe.Core.Infrastructure.Arrow.ArrowArrayFactory.Create(columnData, field.ClrType, field.IsNullable));
				}

				yield return new RecordBatch(schema, arrays, rowCount);
			}
		}
		finally
		{
			_isReading = false;
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		// Mark as reading to prevent concurrent reads
		await _semaphore.WaitAsync(ct);
		try
		{
			if (_isReading)
				throw new InvalidOperationException("Concurrent reads from the same ParquetStreamReader are not supported.");
			_isReading = true;
		}
		finally
		{
			_semaphore.Release();
		}

		try
		{
			var batch = new object?[batchSize][];
			var index = 0;

			// Read all row groups
			for (int rowGroupIndex = 0; rowGroupIndex < _reader.RowGroupCount; rowGroupIndex++)
			{
				ct.ThrowIfCancellationRequested();

				// Check if reader was disposed
				await _semaphore.WaitAsync(ct);
				try
				{
					if (_reader == null) yield break;
				}
				finally
				{
					_semaphore.Release();
				}

				using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
				var rowCount = (int)rowGroupReader.RowCount;

				// Read all columns for this row group
				var columnDataArrays = new System.Array[Columns.Count];
				for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
				{
					var dataField = _reader.Schema.DataFields[colIndex];
					columnDataArrays[colIndex] = await ReadColumnDataAsArrayAsync(rowGroupReader, dataField, ct);
				}

				// Yield rows
				for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
				{
					ct.ThrowIfCancellationRequested();

					var row = new object?[Columns.Count];
					for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
					{
						row[colIndex] = columnDataArrays[colIndex].GetValue(rowIndex);
					}

					batch[index++] = row;

					if (index >= batchSize)
					{
						yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
						batch = new object?[batchSize][];
						index = 0;
					}
				}
			}

			if (index > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
			}
		}
		finally
		{
			_isReading = false;
		}
	}

	private static async Task<System.Array> ReadColumnDataAsArrayAsync(ParquetRowGroupReader rowGroupReader, DataField field, CancellationToken ct)
	{
		int rowCount = (int)rowGroupReader.RowCount;
		Type baseType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

		if (baseType == typeof(string))
		{
			var data = new string[rowCount];
			await rowGroupReader.ReadAsync(field, data.AsMemory(), null, ct);
			return data;
		}
		if (baseType == typeof(byte[]))
		{
			var data = new ReadOnlyMemory<byte>?[rowCount];
			await rowGroupReader.ReadAsync<ReadOnlyMemory<byte>>(field, data.AsMemory(), null, ct);
			var result = new byte[rowCount][];
			for (int i = 0; i < rowCount; i++)
				result[i] = data[i]?.ToArray()!;
			return result;
		}

		// Other types use the generic ReadAsync<T>(..., Memory<T?>, ...)
		return baseType switch
		{
			_ when baseType == typeof(bool) => await ReadTypedAsync<bool>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(byte) => await ReadTypedAsync<byte>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(sbyte) => await ReadTypedAsync<sbyte>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(short) => await ReadTypedAsync<short>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(int) => await ReadTypedAsync<int>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(long) => await ReadTypedAsync<long>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(float) => await ReadTypedAsync<float>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(double) => await ReadTypedAsync<double>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(decimal) => await ReadTypedAsync<decimal>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(DateTime) => await ReadTypedAsync<DateTime>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(DateTimeOffset) => await ReadTypedAsync<DateTimeOffset>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(TimeSpan) => await ReadTypedAsync<TimeSpan>(rowGroupReader, field, rowCount, ct),
			_ when baseType == typeof(Guid) => await ReadTypedAsync<Guid>(rowGroupReader, field, rowCount, ct),
			_ => throw new NotSupportedException($"Unsupported Parquet type: {baseType.Name}")
		};
	}


	private static async Task<T?[]> ReadTypedAsync<T>(ParquetRowGroupReader rowGroupReader, DataField field, int rowCount, CancellationToken ct)
		where T : struct
	{
		var data = new T?[rowCount];
		await rowGroupReader.ReadAsync<T>(field, data, cancellationToken: ct);
		return data;
	}

	public async ValueTask DisposeAsync()
	{
		await _semaphore.WaitAsync();
		try
		{
			if (_reader != null)
			{
				await _reader.DisposeAsync();
				_reader = null;
			}
			if (_fileStream != null)
			{
				await _fileStream.DisposeAsync();
				_fileStream = null;
			}
		}
		finally
		{
			_semaphore.Release();
			_semaphore.Dispose();
		}
	}
}
