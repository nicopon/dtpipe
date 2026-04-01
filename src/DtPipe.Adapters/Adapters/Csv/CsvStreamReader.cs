using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.Csv;

public class CsvStreamReader : IStreamReader, IColumnTypeInferenceCapable
{
	private readonly string _filePath;
	private readonly CsvReaderOptions _options;
	private readonly ILogger _logger;

	private FileStream? _fileStream;
	private StreamReader? _streamReader;
	private CsvReader? _csvReader;
	private string[]? _headers;

	// Per-column parser: null means keep as string, non-null parses the raw cell value
	private Func<string, object?>[]? _columnParsers;

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
		var encoding = Encoding.GetEncoding(_options.Encoding);

		if (string.IsNullOrEmpty(_filePath) || _filePath == "-")
		{
			if (!Console.IsInputRedirected)
			{
				throw new InvalidOperationException("Structure input (STDIN) is not redirected. To read from STDIN, pipe data into dtpipe (e.g. 'cat file.csv | dtpipe ...').");
			}

			_streamReader = new StreamReader(Console.OpenStandardInput(), encoding);
		}
		else
		{
			await ValidateFileAccessAsync(_filePath, ct);

			if (_logger.IsEnabled(LogLevel.Debug))
				_logger.LogDebug("Opening CSV file: {FilePath}", _filePath);

			_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
			_streamReader = new StreamReader(_fileStream, encoding, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
		}

		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _options.Separator,
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
			if (await _csvReader.ReadAsync())
			{
				var fieldCount = _csvReader.Parser.Count;
				_headers = Enumerable.Range(0, fieldCount).Select(i => $"Column{i}").ToArray();
			}
			else
			{
				_headers = Array.Empty<string>();
			}
		}

		// Build column parsers from --column-types option (or auto-inferred types)
		var typeOverrides = ParseColumnTypesOption(_options.ColumnTypes);

		// Auto-infer types when --auto-column-types is set and no explicit --column-types given
		if (_options.AutoColumnTypes && string.IsNullOrWhiteSpace(_options.ColumnTypes)
		    && !string.IsNullOrEmpty(_filePath) && _filePath != "-")
		{
			try
			{
				const int autoSampleCount = 100;
				var inferred = await InferColumnTypesAsync(autoSampleCount, default);
				if (inferred.Count > 0)
				{
					_autoAppliedTypes = inferred;
					typeOverrides = inferred
						.Select(kv => (kv.Key, Type: ResolveHintToClrType(kv.Value)))
						.Where(x => x.Type != null)
						.ToDictionary(x => x.Key, x => x.Type!, StringComparer.OrdinalIgnoreCase);
				}
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Auto column-type inference failed, falling back to string columns.");
			}
		}

		_columnParsers = BuildColumnParsers(_headers, typeOverrides);

		// Derive column schema: use declared type when available, otherwise string
		Columns = _headers.Select((h, i) =>
		{
			var clrType = typeOverrides.TryGetValue(h, out var t) ? t : typeof(string);
			return new PipeColumnInfo(h, clrType, true);
		}).ToList();
	}

	private IReadOnlyDictionary<string, string>? _autoAppliedTypes;
	public IReadOnlyDictionary<string, string>? AutoAppliedTypes => _autoAppliedTypes;

	private readonly SemaphoreSlim _lock = new(1, 1);
	private bool _isDisposed;

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_csvReader is null || _headers is null || _columnParsers is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new object?[batchSize][];
		var index = 0;

		try
		{
			// For no-header mode, first row is already read in OpenAsync
			if (!_options.HasHeader && _csvReader.Parser.Row == 1)
			{
				var row = ParseRow(_headers.Length, _columnParsers);
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
					row = ParseRow(_headers.Length, _columnParsers);
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

	/// <inheritdoc />
	public async Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(int sampleRows, CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_filePath) || _filePath == "-")
			return new Dictionary<string, string>();

		var encoding = Encoding.GetEncoding(_options.Encoding);
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			Delimiter = _options.Separator,
			HasHeaderRecord = _options.HasHeader,
			MissingFieldFound = null,
			BadDataFound = null
		};

		// Use a separate file handle so we don't interfere with the main reader
		await using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
		using var sr = new StreamReader(fs, encoding);
		using var csv = new CsvReader(sr, config);

		string[] headers;
		if (_options.HasHeader)
		{
			await csv.ReadAsync();
			csv.ReadHeader();
			headers = csv.HeaderRecord ?? Array.Empty<string>();
		}
		else
		{
			if (!await csv.ReadAsync()) return new Dictionary<string, string>();
			var count = csv.Parser.Count;
			headers = Enumerable.Range(0, count).Select(i => $"Column{i}").ToArray();
		}

		// Per-column sample values
		var samples = headers.Select(_ => new List<string>()).ToArray();
		int rowCount = 0;

		while (rowCount < sampleRows && await csv.ReadAsync())
		{
			for (int i = 0; i < headers.Length; i++)
			{
				var raw = csv.GetField(i) ?? "";
				if (!string.IsNullOrWhiteSpace(raw))
					samples[i].Add(raw);
			}
			rowCount++;
		}

		var suggestions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < headers.Length; i++)
		{
			var hint = InferTypeHint(samples[i]);
			if (hint != null) suggestions[headers[i]] = hint;
		}
		return suggestions;
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

	// ── Helpers ───────────────────────────────────────────────────────────────

	private object?[] ParseRow(int columnCount, Func<string, object?>[] parsers)
	{
		var row = new object?[columnCount];
		for (int i = 0; i < columnCount; i++)
		{
			var raw = _csvReader!.GetField(i);
			row[i] = raw == null ? null : parsers[i](raw);
		}
		return row;
	}

	/// <summary>Parses "Col1:type1,Col2:type2" into a name → CLR type dictionary.</summary>
	private static Dictionary<string, Type> ParseColumnTypesOption(string spec)
	{
		var result = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
		if (string.IsNullOrWhiteSpace(spec)) return result;

		foreach (var entry in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
		{
			var idx = entry.IndexOf(':');
			if (idx <= 0) continue;
			var name = entry[..idx].Trim();
			var typeName = entry[(idx + 1)..].Trim();
			var clrType = ResolveHintToClrType(typeName);
			if (clrType != null) result[name] = clrType;
		}
		return result;
	}

	/// <summary>Maps a hint string to the CLR type the column will carry in the pipeline.</summary>
	private static Type? ResolveHintToClrType(string hint) => hint.ToLowerInvariant() switch
	{
		"uuid" or "guid" => typeof(Guid),
		"string" or "str" => typeof(string),
		"int" or "int32" => typeof(int),
		"long" or "int64" => typeof(long),
		"double" or "float64" => typeof(double),
		"float" or "float32" or "single" => typeof(float),
		"decimal" or "numeric" or "money" => typeof(decimal),
		"bool" or "boolean" => typeof(bool),
		"datetime" or "date" => typeof(DateTime),
		"datetimeoffset" or "timestamp" => typeof(DateTimeOffset),
		_ => null
	};

	/// <summary>Builds per-column parser functions. Null parser means "keep as string".</summary>
	private static Func<string, object?>[] BuildColumnParsers(string[] headers, Dictionary<string, Type> overrides)
	{
		var parsers = new Func<string, object?>[headers.Length];
		for (int i = 0; i < headers.Length; i++)
		{
			parsers[i] = overrides.TryGetValue(headers[i], out var t)
				? BuildParser(t)
				: static s => s; // string passthrough
		}
		return parsers;
	}

	private static Func<string, object?> BuildParser(Type clrType)
	{
		if (clrType == typeof(Guid))
			return static s =>
			{
				if (string.IsNullOrWhiteSpace(s)) return null;
				// Standard UUID string format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
				if (Guid.TryParse(s, out var g)) return g;
				// Base64 (24-char) fallback — stored as raw bytes, re-interpret as RFC 4122
				if (s.Length == 24 && s.EndsWith("==", StringComparison.Ordinal))
				{
					try
					{
						var bytes = Convert.FromBase64String(s);
						if (bytes.Length == 16)
							// Base64 bytes were produced by Guid.ToByteArray() → reconstruct Guid
							return new Guid(bytes);
					}
					catch { /* fall through */ }
				}
				return null;
			};

		if (clrType == typeof(int))
			return static s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : (object?)null;

		if (clrType == typeof(long))
			return static s => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : (object?)null;

		if (clrType == typeof(double))
			return static s => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : (object?)null;

		if (clrType == typeof(float))
			return static s => float.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : (object?)null;

		if (clrType == typeof(decimal))
			return static s => decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var v) ? v : (object?)null;

		if (clrType == typeof(bool))
			return static s =>
			{
				if (bool.TryParse(s, out var b)) return b;
				return s switch { "1" or "yes" or "true" => true, "0" or "no" or "false" => false, _ => (object?)null };
			};

		if (clrType == typeof(DateTime))
			return static s => DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var v) ? v : (object?)null;

		if (clrType == typeof(DateTimeOffset))
			return static s => DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var v) ? v : (object?)null;

		// Fallback: keep as string
		return static s => s;
	}

	/// <summary>
	/// Infers the most specific type hint for a list of sample string values.
	/// Returns null when all values are strings with no detectable pattern.
	/// </summary>
	private static string? InferTypeHint(List<string> samples)
	{
		if (samples.Count == 0) return null;

		// Try each type in precedence order — first one that matches ALL samples wins
		bool allMatch(Func<string, bool> test) => samples.All(test);

		if (allMatch(s => Guid.TryParse(s, out _)))
			return "uuid";

		if (allMatch(s => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)))
		{
			// Prefer int32 when all values fit
			return samples.All(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
				? "int32" : "int64";
		}

		if (allMatch(s => decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out _)
		                  && s.Contains('.')))
		{
			// Suggest decimal when values have a decimal point and exceed double precision
			// (any value with > 15 significant digits, or explicit decimal notation)
			bool needsDecimalPrecision = samples.Any(s =>
			{
				if (!decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var d)) return false;
				if (!double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var dbl)) return true;
				// Check if round-tripping through double loses precision
				return d != (decimal)dbl;
			});
			return needsDecimalPrecision ? "decimal" : "double";
		}

		if (allMatch(s => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out _)))
			return "double";

		if (allMatch(s => bool.TryParse(s, out _) || s is "0" or "1" or "yes" or "no"))
			return "bool";

		if (allMatch(s => DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out _)))
			return "datetime";

		return null;
	}
}
