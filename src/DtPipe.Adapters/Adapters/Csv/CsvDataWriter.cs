using System.Globalization;
using System.Text;
using System.Text.Json;
using CsvHelper;
using CsvHelper.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.IO;

namespace DtPipe.Adapters.Csv;

public sealed class CsvDataWriter : IRowDataWriter, IRequiresOptions<CsvWriterOptions>, ISchemaInspector
{
	// Shared RecyclableMemoryStreamManager for all instances
	private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new(
		new RecyclableMemoryStreamManager.Options
		{
			BlockSize = 128 * 1024,        // 128KB blocks
			LargeBufferMultiple = 1024 * 1024, // 1MB large buffers
			MaximumBufferSize = 16 * 1024 * 1024, // 16MB max
			GenerateCallStacks = false
		});

	private readonly string _outputPath;
	private readonly CsvWriterOptions _options;

	// Initialized in InitializeAsync
	private Stream? _outputStream;
	private RecyclableMemoryStream? _memoryStream;
	private StreamWriter? _streamWriter;
	private CsvWriter? _csvWriter;

	private IReadOnlyList<PipeColumnInfo>? _columns;
	private int _rowsInBuffer;
	private const int FlushThreshold = 1000; // Flush every N rows

	public CsvDataWriter(string outputPath) : this(outputPath, new CsvWriterOptions())
	{
	}

	public CsvDataWriter(string outputPath, CsvWriterOptions options)
	{
		_outputPath = outputPath;
		_options = options;
	}

	public bool RequiresTargetInspection => false;

	public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (_outputPath == "-")
		{
			// For stdout, we assume new/overwrite.
			return new TargetSchemaInfo([], false, null, null, null);
		}

        if (string.IsNullOrEmpty(_outputPath))
        {
             throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

		if (!File.Exists(_outputPath))
		{
			return new TargetSchemaInfo([], false, null, null, null);
		}

		try
		{
			// Simple CSV inspection: Read header
			using var reader = new StreamReader(_outputPath);
			var headerLine = await reader.ReadLineAsync(ct);
			if (string.IsNullOrEmpty(headerLine))
			{
				return new TargetSchemaInfo([], true, 0, new FileInfo(_outputPath).Length, null);
			}

			// Parse header using same separator options if possible, or naive split
			// Using CsvHelper to parse just the header is safer
			using var stringReader = new StringReader(headerLine);
			var config = new CsvConfiguration(CultureInfo.InvariantCulture)
			{
				Delimiter = _options.Separator,
				Quote = _options.Quote,
				HasHeaderRecord = true
			};
			using var csvParser = new CsvParser(stringReader, config);
			if (await csvParser.ReadAsync())
			{
				var headers = csvParser.Record;
				if (headers != null)
				{
					var columns = headers.Select(h => new TargetColumnInfo(
						h,
						"STRING",
						typeof(string),
						true,
						false,
						false,
						null, null, null
					)).ToList();

					return new TargetSchemaInfo(columns, true, null, new FileInfo(_outputPath).Length, null);
				}
			}

			return new TargetSchemaInfo([], true, null, new FileInfo(_outputPath).Length, null);
		}
		catch (Exception)
		{
			return null;
		}
	}

	public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		foreach (var col in columns) EnsureRenderable(col);

		if (_outputPath == "-")
		{
			_outputStream = Console.OpenStandardOutput();
		}
		else
		{
			var directory = Path.GetDirectoryName(_outputPath);
			if (!string.IsNullOrEmpty(directory))
				Directory.CreateDirectory(directory); // no-op if already exists
			_outputStream = new FileStream(_outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
		}

		// Use RecyclableMemoryStream as intermediate buffer
		_memoryStream = (RecyclableMemoryStream)MemoryStreamManager.GetStream("CsvDataWriter");
		// Avoid writing UTF-8 BOM as it can interfere with some readers (like our own CsvStreamReader when detection is off)
		var encoding = new UTF8Encoding(false);
		_streamWriter = new StreamWriter(_memoryStream, encoding, bufferSize: 65536, leaveOpen: true);

		// DuckDB-compatible CSV configuration
		var config = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			HasHeaderRecord = _options.Header,
			Delimiter = _options.Separator,
			Quote = _options.Quote,
			ShouldQuote = args => ShouldQuoteField(args.Field, _options)
		};
		_csvWriter = new CsvWriter(_streamWriter, config);

		_columns = columns;

		if (_options.Header)
		{
			foreach (var col in columns)
			{
				_csvWriter.WriteField(col.Name);
			}
			_csvWriter.NextRecord();
		}
		return ValueTask.CompletedTask;
	}

	public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_columns is null || _csvWriter is null)
			throw new InvalidOperationException("Call InitializeAsync first.");

		foreach (var row in rows)
		{
			for (var i = 0; i < row.Length; i++)
			{
				var formatted = FormatValue(row[i], _columns[i]);
				_csvWriter.WriteField(formatted);
			}
			_csvWriter.NextRecord();
			_rowsInBuffer++;
		}

		// Flush memory stream to file periodically
		if (_rowsInBuffer >= FlushThreshold)
		{
			await FlushBufferToFileAsync(ct);
		}
	}

	private async ValueTask FlushBufferToFileAsync(CancellationToken ct)
	{
		if (_csvWriter is null || _streamWriter is null || _memoryStream is null || _outputStream is null) return;

		await _csvWriter.FlushAsync();
		await _streamWriter.FlushAsync(ct);

		// Copy from memory stream to file
		_memoryStream.Position = 0;
		await _memoryStream.CopyToAsync(_outputStream, ct);
		await _outputStream.FlushAsync(ct);

		// Reset memory stream for reuse
		_memoryStream.SetLength(0);
		_rowsInBuffer = 0;
	}

	/// <summary>
	/// Rejects, at init, a column whose type FormatValue could only render through
	/// <see cref="object.ToString"/> — which yields the type name, not the data. Without this a
	/// DuckDB LIST column produced a file full of "System.Collections.Generic.List`1[...]" and
	/// exit 0. Checked once per column: the per-cell path must stay free of type inspection.
	/// </summary>
	private static void EnsureRenderable(PipeColumnInfo column)
	{
		var type = Nullable.GetUnderlyingType(column.ClrType) ?? column.ClrType;

		// A dynamic column carries no type to judge; its values are shaped at write time.
		if (type == typeof(object)) return;
		// Rendered by a dedicated case in FormatValue.
		if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type)) return;
		// Anything that overrides ToString() renders itself meaningfully (Guid, enums, numbers…).
		if (type.GetMethod("ToString", Type.EmptyTypes)?.DeclaringType != typeof(object)) return;

		throw new NotSupportedException(
			$"Column '{column.Name}' has type {type.Name}, which the CSV writer cannot render as text. " +
			"Project it to a supported type in your query, for example by casting it to JSON or text.");
	}

	private string? FormatValue(object? value, PipeColumnInfo column)
	{
		if (value is null)
		{
			// If _options.NullValue is null, CsvHelper typically writes an empty string by default for nullable types,
			// but we explicitly return the configured null value here.
			return _options.NullValue;
		}

		var baseType = Nullable.GetUnderlyingType(column.ClrType) ?? column.ClrType;

		return value switch
		{
			// Dates - ISO 8601 format for DuckDB
			DateTime dt when baseType == typeof(DateTime) && dt.TimeOfDay == TimeSpan.Zero
				=> dt.ToString(_options.DateFormat, CultureInfo.InvariantCulture),
			DateTime dt => dt.ToString(_options.TimestampFormat, CultureInfo.InvariantCulture),
			DateTimeOffset dto => dto.ToString(_options.TimestampFormat, CultureInfo.InvariantCulture),

			// Numbers - use invariant culture with configured decimal separator
			decimal d => FormatDecimal(d),
			double d => FormatDouble(d),
			float f => FormatFloat(f),

			// Binary data as Base64 (DuckDB can decode with decode(x, 'base64'))
			byte[] bytes => Convert.ToBase64String(bytes),

			// Boolean as true/false (DuckDB native format)
			bool b => b ? "true" : "false",

			// Complex structures (e.g. from Arrow StructType) as JSON
			System.Collections.IDictionary dict => JsonSerializer.Serialize(dict),

			// string is IEnumerable, so it must be matched before the collection case below —
			// otherwise every text cell would be written as a JSON array of characters.
			string s => s,

			// Collections (e.g. from Arrow ListType) as JSON. Serialized against the runtime
			// type: against IEnumerable, System.Text.Json emits an empty object.
			System.Collections.IEnumerable seq => JsonSerializer.Serialize(seq, seq.GetType()),

			// Everything else as string
			_ => Convert.ToString(value, CultureInfo.InvariantCulture)
		};
	}

	private string FormatDecimal(decimal value)
	{
		var str = value.ToString(CultureInfo.InvariantCulture);
		if (_options.DecimalSeparator != ".")
			str = str.Replace(".", _options.DecimalSeparator);
		return str;
	}

	private string FormatDouble(double value)
	{
		var str = value.ToString("G17", CultureInfo.InvariantCulture);
		if (_options.DecimalSeparator != ".")
			str = str.Replace(".", _options.DecimalSeparator);
		return str;
	}

	private string FormatFloat(float value)
	{
		var str = value.ToString("G9", CultureInfo.InvariantCulture);
		if (_options.DecimalSeparator != ".")
			str = str.Replace(".", _options.DecimalSeparator);
		return str;
	}

	private static bool ShouldQuoteField(string? field, CsvWriterOptions options)
	{
		if (string.IsNullOrEmpty(field))
			return false;

		return field.Contains(options.Separator) ||
			   field.Contains(options.Quote) ||
			   field.Contains('\n') ||
			   field.Contains('\r');
	}

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		// Unconditional. InitializeAsync writes the header into the buffer before any row exists,
		// so gating this on the row count discarded the header along with them: a pipeline whose
		// filter matched nothing produced a 0-byte file that this adapter's own reader then
		// refuses. The schema survives an empty result, the way it already does for Parquet.
		await FlushBufferToFileAsync(ct);

		if (_outputStream != null)
			await _outputStream.FlushAsync(ct);
	}

	public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		throw new NotSupportedException("Executing raw commands is not supported for CSV targets.");
	}

	public async ValueTask DisposeAsync()
	{
		if (_csvWriter != null) await _csvWriter.DisposeAsync();
		if (_streamWriter != null) await _streamWriter.DisposeAsync();
		if (_memoryStream != null) await _memoryStream.DisposeAsync();
		if (_outputStream != null) await _outputStream.DisposeAsync();
	}
}
