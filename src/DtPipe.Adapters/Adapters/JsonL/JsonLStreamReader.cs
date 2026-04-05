using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;

namespace DtPipe.Adapters.JsonL;

public class JsonLStreamReader : IStreamReader, IColumnarStreamReader
{
	private readonly string _filePath;
	private readonly JsonLReaderOptions _options;
	private readonly ILogger _logger;

	private FileStream? _fileStream;
	private StreamReader? _streamReader;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema { get; private set; }

	public JsonLStreamReader(string filePath, JsonLReaderOptions options, ILogger? logger = null)
	{
		_filePath = filePath;
		_options = options;
		_logger = logger ?? NullLogger.Instance;
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		var encoding = Encoding.GetEncoding(_options.Encoding);

		if (string.IsNullOrEmpty(_filePath) || _filePath == "-")
		{
			if (!Console.IsInputRedirected)
			{
				throw new InvalidOperationException("Structure input (STDIN) is not redirected. To read from STDIN, pipe data into dtpipe (e.g. 'cat file.jsonl | dtpipe ...').");
			}

			_streamReader = new StreamReader(Console.OpenStandardInput(), encoding);
		}
		else
		{
			if (!File.Exists(_filePath))
				throw new FileNotFoundException($"JsonL file not found: {_filePath}", _filePath);

			_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
			_streamReader = new StreamReader(_fileStream, encoding, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
		}

		// Infer schema from the first line
		await InferSchemaAsync(ct);
	}

	private async Task InferSchemaAsync(CancellationToken ct)
	{
		if (_streamReader == null) throw new InvalidOperationException("StreamReader is null.");
		
		_firstLine = await _streamReader.ReadLineAsync(ct);
		if (string.IsNullOrEmpty(_firstLine))
		{
			Columns = System.Array.Empty<PipeColumnInfo>();
			Schema = new Schema(Enumerable.Empty<Field>(), null);
			return;
		}

		try
		{
			using var doc = JsonDocument.Parse(_firstLine);
			var root = doc.RootElement;

			if (root.ValueKind != JsonValueKind.Object)
				throw new InvalidOperationException("First line of JsonL is not a JSON object.");

			var columns = new List<PipeColumnInfo>();
			var fields = new List<Field>();

			foreach (var property in root.EnumerateObject())
			{
				var (type, arrowType) = InferTypes(property.Value);
				columns.Add(new PipeColumnInfo(property.Name, type, true));
				fields.Add(new Field(property.Name, arrowType, true));
			}

			Columns = columns;
			Schema = new Schema(fields, null);
			_logger.LogInformation("JsonLStreamReader: Inferred schema with {Count} columns. Schema: {Schema}", columns.Count, Schema.ToString());
		}
		catch (JsonException ex)
		{
			throw new InvalidOperationException($"Failed to parse first line of JsonL as JSON: {ex.Message}", ex);
		}
	}

	private (Type ClrType, IArrowType ArrowType) InferTypes(JsonElement element)
	{
		return element.ValueKind switch
		{
			JsonValueKind.Number => (typeof(double), DoubleType.Default),
			JsonValueKind.True or JsonValueKind.False => (typeof(bool), BooleanType.Default),
			JsonValueKind.Object => (typeof(object), InferStructType(element)),
			JsonValueKind.Array => (typeof(object), InferListType(element)),
			_ => (typeof(string), StringType.Default)
		};
	}

	private IArrowType InferStructType(JsonElement element)
	{
		var fields = new List<Field>();
		foreach (var prop in element.EnumerateObject())
		{
			var (_, arrowType) = InferTypes(prop.Value);
			fields.Add(new Field(prop.Name, arrowType, true));
		}
		return new StructType(fields);
	}

	private IArrowType InferListType(JsonElement element)
	{
		var array = element.EnumerateArray();
		if (array.Any())
		{
			var first = array.First();
			var (_, itemArrowType) = InferTypes(first);
			return new ListType(itemArrowType);
		}
		// Default to list of strings if empty
		return new ListType(StringType.Default);
	}

	private string? _firstLine;

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		int batchSize = 1000;
		if (_streamReader is null || Columns is null || Schema is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new List<Dictionary<string, object?>>(batchSize);

		if (_firstLine != null)
		{
			batch.Add(ParseLineToDictionary(_firstLine));
			_firstLine = null;
		}

		while (true)
		{
			ct.ThrowIfCancellationRequested();
			var line = await _streamReader.ReadLineAsync(ct);
			if (line == null) break;
			if (string.IsNullOrWhiteSpace(line)) continue;

			batch.Add(ParseLineToDictionary(line));

			if (batch.Count >= batchSize)
			{
				yield return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema);
				batch.Clear();
			}
		}

		if (batch.Count > 0)
		{
			yield return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema);
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_streamReader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new object?[batchSize][];
		var index = 0;

		if (_firstLine != null)
		{
			batch[index++] = ParseLine(_firstLine);
			_firstLine = null;
		}

		while (true)
		{
			ct.ThrowIfCancellationRequested();
			var line = await _streamReader.ReadLineAsync(ct);
			if (line == null) break;
			if (string.IsNullOrWhiteSpace(line)) continue;

			batch[index++] = ParseLine(line);

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

	private object?[] ParseLine(string line)
	{
		using var doc = JsonDocument.Parse(line);
		var root = doc.RootElement;
		var row = new object?[Columns!.Count];

		for (int i = 0; i < Columns.Count; i++)
		{
			var colName = Columns[i].Name;
			if (root.TryGetProperty(colName, out var prop))
			{
				row[i] = ToValue(prop);
			}
			else
			{
				row[i] = null;
			}
		}

		return row;
	}

	private Dictionary<string, object?> ParseLineToDictionary(string line)
	{
		using var doc = JsonDocument.Parse(line);
		var root = doc.RootElement;
		var dict = new Dictionary<string, object?>();

		foreach (var prop in root.EnumerateObject())
		{
			dict[prop.Name] = ToValue(prop.Value);
		}

		return dict;
	}

	private object? ToValue(JsonElement element)
	{
		return element.ValueKind switch
		{
			JsonValueKind.String => element.GetString(),
			JsonValueKind.Number => element.GetDouble(),
			JsonValueKind.True => true,
			JsonValueKind.False => false,
			JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ToValue(p.Value)),
			JsonValueKind.Array => element.EnumerateArray().Select(ToValue).ToList(),
			JsonValueKind.Null => null,
			_ => element.GetRawText()
		};
	}

	public async ValueTask DisposeAsync()
	{
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
}
