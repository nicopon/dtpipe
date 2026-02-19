using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.Csv;

public class CsvStreamReader : IStreamReader
{
	private readonly string _filePath;
	private readonly CsvReaderOptions _options;
	private readonly ILogger _logger;

	private FileStream? _fileStream;
	private StreamReader? _streamReader;
	private CsvReader? _csvReader;
	private string[]? _headers;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	public CsvStreamReader(string filePath, CsvReaderOptions options, ILogger? logger = null)
	{
		_filePath = filePath;
		_options = options;
		_logger = logger ?? NullLogger.Instance;
	}

	private static async Task ValidateFileAccessAsync(string filePath, CancellationToken ct)
	{
		if (!File.Exists(filePath))
			throw new FileNotFoundException($"CSV file not found: {filePath}", filePath);

		var fileInfo = new FileInfo(filePath);
		if (fileInfo.Length == 0)
			throw new InvalidOperationException($"CSV file is empty: {filePath}");

		// Retry loop to handle transient locks or filesystem lag (especially on Mac/Unix)
		int retries = 5;
		while (retries > 0)
		{
			try
			{
				// Test read access with minimal I/O
				await using var testStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1, FileOptions.None);
				var buffer = new byte[1];
				// Use Memory-based ReadAsync to avoid CA2022 and potentially mitigate runtime async pooling issues
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
		var encoding = Encoding.GetEncoding(_options.Encoding);

		if (string.IsNullOrEmpty(_filePath) || _filePath == "-")
		{
			if (!Console.IsInputRedirected)
			{
				throw new InvalidOperationException("Structure input (STDIN) is not redirected. To read from STDIN, pipe data into dtpipe (e.g. 'cat file.csv | dtpipe ...').");
			}

			// Use Standard Input (Console.OpenStandardInput returns a Stream that shouldn't be generally disposed by us, or at least we don't own it fully, but StreamReader defaults to closing it)
			// For stdin, we usually let it close.
			_streamReader = new StreamReader(Console.OpenStandardInput(), encoding);
		}
		else
		{
			// Validate file access before opening
			await ValidateFileAccessAsync(_filePath, ct);

			if (_logger.IsEnabled(LogLevel.Debug))
				_logger.LogDebug("Opening CSV file: {FilePath}", _filePath);

			_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
			_streamReader = new StreamReader(_fileStream, encoding, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
		}

		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _options.Delimiter,
			HasHeaderRecord = _options.HasHeader,
			MissingFieldFound = null,
			BadDataFound = null
		};

		_csvReader = new CsvReader(_streamReader, config, leaveOpen: true);

		if (_options.HasHeader)
		{
			await _csvReader.ReadAsync();
			_csvReader.ReadHeader();
			_headers = _csvReader.HeaderRecord ?? Array.Empty<string>();
		}
		else
		{
			// Read first row to determine column count
			if (await _csvReader.ReadAsync())
			{
				var fieldCount = _csvReader.Parser.Count;
				_headers = Enumerable.Range(0, fieldCount).Select(i => $"Column{i}").ToArray();
				// Note: First data row will be yielded in ReadBatchesAsync
			}
			else
			{
				_headers = Array.Empty<string>();
			}
		}

		// All CSV columns are strings
		Columns = _headers.Select(h => new PipeColumnInfo(h, typeof(string), true)).ToList();
	}

	private readonly SemaphoreSlim _lock = new(1, 1);
	private bool _isDisposed;

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_csvReader is null || _headers is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new object?[batchSize][];
		var index = 0;

		try
		{
			// For no-header mode, first row is already read in OpenAsync
			if (!_options.HasHeader && _csvReader.Parser.Row == 1)
			{
				var row = new object?[_headers.Length];
				for (int i = 0; i < _headers.Length; i++)
				{
					row[i] = _csvReader.GetField(i);
				}
				batch[index++] = row;
			}

			while (true)
			{
				object?[] row;
				bool hasMore;

				await _lock.WaitAsync(ct);
				try
				{
					if (_isDisposed) break;
					ct.ThrowIfCancellationRequested();

					hasMore = await _csvReader.ReadAsync();
					if (!hasMore) break;

					// CRITICAL: GetField() must be called INSIDE the lock
					// to prevent buffer corruption during concurrent access
					row = new object?[_headers.Length];
					for (int i = 0; i < _headers.Length; i++)
					{
						row[i] = _csvReader.GetField(i);
					}
				}
				finally
				{
					_lock.Release();
				}

				batch[index++] = row;

				if (index >= batchSize)
				{
					yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
					batch = new object?[batchSize][];
					index = 0;
				}
			}

			if (index > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
			}
		}
		finally
		{
		}
	}

	public async ValueTask DisposeAsync()
	{
		await _lock.WaitAsync();
		try
		{
			if (_isDisposed) return;
			_isDisposed = true;

			if (_csvReader != null)
			{
				_csvReader.Dispose();
				_csvReader = null;
			}

			if (_streamReader != null)
			{
				_streamReader.Dispose();
				_streamReader = null;
			}

			if (_fileStream != null)
			{
				await _fileStream.DisposeAsync();
				_fileStream = null;
			}
		}
		finally
		{
			_lock.Release();
			_lock.Dispose();
		}
	}
}
