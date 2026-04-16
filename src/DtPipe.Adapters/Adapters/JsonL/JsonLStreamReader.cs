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
using System.Collections;
using DtPipe.Core.Infrastructure.Discovery;

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

	private bool _isResetSupported;
	private Dictionary<string, object?>? _cachedFirstBatch;

	public async Task OpenAsync(CancellationToken ct = default)
	{
		_isResetSupported = !string.IsNullOrEmpty(_filePath) && _filePath != "-";

		await ResetReaderAsync();

		// Infer schema by sampling multiple records
		await InferSchemaAsync(ct);

		// Reset for final data extraction
		await ResetReaderAsync();
	}

	private async Task ResetReaderAsync()
	{
		if (!_isResetSupported)
		{
			// STDIN: We cannot reset. If we already have a stream, we keep it.
			if (_streamReader == null)
			{
				var encoding = Encoding.GetEncoding(_options.Encoding);
				_streamReader = new StreamReader(Console.OpenStandardInput(), encoding);
			}
			return;
		}

		// File-based: Close and re-open to ensure fresh start
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

		var encoding2 = Encoding.GetEncoding(_options.Encoding);
		_fileStream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
		_streamReader = new StreamReader(_fileStream, encoding2, detectEncodingFromByteOrderMarks: false, leaveOpen: true);
	}

	private async Task InferSchemaAsync(CancellationToken ct)
	{
		if (_streamReader == null) throw new InvalidOperationException("StreamReader is null.");
		
		Dictionary<string, object?>? merged = null;
		int count = 0;

		// If we are on a non-resetable stream (STDIN), we must cache what we read during discovery
		if (!_isResetSupported)
		{
			_cachedFirstBatch = new Dictionary<string, object?>();
		}

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			if (record is Dictionary<string, object?> current)
			{
				if (merged == null)
				{
					merged = new Dictionary<string, object?>(current, StringComparer.OrdinalIgnoreCase);
				}
				else
				{
					SchemaDiscoveryHelper.DeepMergeSchemas(merged, current);
				}
			}

			count++;
			if (count >= _options.MaxSample) break;
		}

		if (merged == null)
		{
			Columns = System.Array.Empty<PipeColumnInfo>();
			Schema = new Schema(Enumerable.Empty<Field>(), null);
			return;
		}

		var columns = new List<PipeColumnInfo>();
		var fields = new List<Field>();

		foreach (var kvp in merged)
		{
			var (type, arrowType) = InferTypes(kvp.Key, kvp.Value);
			columns.Add(new PipeColumnInfo(kvp.Key, type, true));
			fields.Add(new Field(kvp.Key, arrowType, true));
		}

		Columns = columns;
		Schema = new Schema(fields, null);
		_logger.LogInformation("JsonLStreamReader: Inferred schema with {Count} columns. Path: {Path}", columns.Count, _options.Path ?? "(root)");
	}

	private async IAsyncEnumerable<object?> EnumerateRawRecordsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
	{
		if (_streamReader == null) yield break;

		if (string.IsNullOrEmpty(_options.Path))
		{
			// Classic JSONL: 1 line = 1 record
			while (!ct.IsCancellationRequested)
			{
				var line = await _streamReader.ReadLineAsync(ct);
				if (line == null) break;
				if (string.IsNullOrWhiteSpace(line)) continue;

				yield return ParseLineToDictionary(line);
			}
		}
		else
		{
			// Hierarchical JSON: Stream from Path
			// For simplicity and to avoid complex JSONPath parser, we use a basic property navigator
			var fullJson = await _streamReader.ReadToEndAsync(ct);
			if (string.IsNullOrEmpty(fullJson)) yield break;

			using var doc = JsonDocument.Parse(fullJson);
			var root = doc.RootElement;
			
			// Navigate to path (supporting simple dot notation for now)
			var current = root;
			var parts = _options.Path.Split('.', StringSplitOptions.RemoveEmptyEntries);
			bool found = true;
			foreach (var part in parts)
			{
				if (current.TryGetProperty(part, out var next))
				{
					current = next;
				}
				else
				{
					found = false;
					break;
				}
			}

			if (found)
			{
				if (current.ValueKind == JsonValueKind.Array)
				{
					foreach (var item in current.EnumerateArray())
					{
						yield return ToValue(item);
					}
				}
				else
				{
					yield return ToValue(current);
				}
			}
		}
	}

	private (Type ClrType, IArrowType ArrowType) InferTypes(string key, object? value)
	{
		return value switch
		{
			double => (typeof(double), DoubleType.Default),
			bool => (typeof(bool), BooleanType.Default),
			Dictionary<string, object?> d => (typeof(object), InferStructType(d)),
			List<object?> l => (typeof(object), InferListType(l)),
			_ => (typeof(string), StringType.Default)
		};
	}

	private IArrowType InferStructType(Dictionary<string, object?> dict)
	{
		var fields = new List<Field>();
		foreach (var prop in dict)
		{
			var (_, arrowType) = InferTypes(prop.Key, prop.Value);
			fields.Add(new Field(prop.Key, arrowType, true));
		}
		return new StructType(fields);
	}

	private IArrowType InferListType(List<object?> list)
	{
		if (list.Any())
		{
			var first = list.First();
			var (_, itemArrowType) = InferTypes("", first);
			return new ListType(itemArrowType);
		}
		return new ListType(StringType.Default);
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		int batchSize = 1000;
		if (_streamReader is null || Columns is null || Schema is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new List<Dictionary<string, object?>>(batchSize);

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			var dict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = record };
			batch.Add(dict);

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

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			batch[index++] = MapDictToRow(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = record });

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

	private object?[] MapDictToRow(Dictionary<string, object?> dict)
	{
		var row = new object?[Columns!.Count];

		for (int i = 0; i < Columns.Count; i++)
		{
			var colName = Columns[i].Name;
			row[i] = GetValueFromFlattenedDict(dict, colName);
		}

		return row;
	}

	private object? GetValueFromFlattenedDict(Dictionary<string, object?> dict, string dotPath)
	{
		if (dotPath.IndexOf('.') == -1) return dict.GetValueOrDefault(dotPath);

		var parts = dotPath.Split('.');
		object? current = dict;
		for (int i = 0; i < parts.Length; i++)
		{
			if (current is Dictionary<string, object?> d)
			{
				if (!d.TryGetValue(parts[i], out current)) return null;
			}
			else return null;
		}
		return current;
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
