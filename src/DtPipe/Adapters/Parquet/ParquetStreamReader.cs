using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Parquet;
using Parquet.Schema;

namespace DtPipe.Adapters.Parquet;

public class ParquetStreamReader : IStreamReader
{
	private readonly string _filePath;
	private readonly ILogger _logger;
	private ParquetReader? _reader;
	private readonly SemaphoreSlim _semaphore = new(1, 1);
	private FileStream? _fileStream;
	private bool _isReading;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

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

		// Test read access with minimal I/O
		await using var testStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1, FileOptions.None);
		var buffer = new byte[1];
		_ = await testStream.ReadAsync(buffer.AsMemory(0, 1), ct);
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
				var columnData = new object?[Columns.Count][];
				for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
				{
					var dataField = _reader.Schema.DataFields[colIndex];
					var dataColumn = await rowGroupReader.ReadColumnAsync(dataField, ct);
					// Convert DataColumn.Data to array
					columnData[colIndex] = dataColumn.Data.Cast<object?>().ToArray();
				}

				// Yield rows
				for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
				{
					ct.ThrowIfCancellationRequested();

					var row = new object?[Columns.Count];
					for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
					{
						row[colIndex] = columnData[colIndex]?[rowIndex];
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

	public async ValueTask DisposeAsync()
	{
		await _semaphore.WaitAsync();
		try
		{
			_reader?.Dispose();
			_reader = null;
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
