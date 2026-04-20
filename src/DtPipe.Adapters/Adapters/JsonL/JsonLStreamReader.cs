using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Linq;
using System.Collections;
using DtPipe.Core.Infrastructure.Discovery;

namespace DtPipe.Adapters.JsonL;

public class JsonLStreamReader : IStreamReader, IColumnarStreamReader, IColumnTypeInferenceCapable
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
		_isResetSupported = !string.IsNullOrEmpty(_filePath) && _filePath != "-";
	}

	private bool _isResetSupported;
	// For STDIN: records consumed during schema inference are buffered here and replayed during data extraction.
	private List<Dictionary<string, object?>>? _stdinBuffer;

	private IReadOnlyDictionary<string, string>? _autoAppliedTypes;
	public IReadOnlyDictionary<string, string>? AutoAppliedTypes => _autoAppliedTypes;

	public async Task OpenAsync(CancellationToken ct = default)
	{
		if (!string.IsNullOrWhiteSpace(_options.Schema))
		{
			// Highest priority: full Arrow schema provided (from --schema-load or --export-job YAML).
			// Captures complete structure including nested StructType / ListType.
			BuildSchemaFromArrowJson(_options.Schema);
			await ResetReaderAsync();
		}
		else if (!string.IsNullOrWhiteSpace(_options.ColumnTypes))
		{
			// Flat scalar type overrides declared by the user via --json-column-types.
			BuildSchemaFromColumnTypes();
			await ResetReaderAsync();
		}
		else
		{
			await ResetReaderAsync();
			await InferSchemaAsync(ct);
			await ResetReaderAsync();
		}
	}

	private void BuildSchemaFromArrowJson(string schemaJson)
	{
		var arrowSchema = DtPipe.Core.Infrastructure.Arrow.ArrowSchemaSerializer.Deserialize(schemaJson);
		Schema  = arrowSchema;
		// Map complex types (List, Struct, Map) to typeof(object) to match the inference path.
		// This ensures ArrowSchemaFactory.Create(Columns) doesn't fail on unsupported CLR types.
		Columns = arrowSchema.FieldsList
			.Select(f => new PipeColumnInfo(f.Name, GetSimplifiedClrType(f), f.IsNullable))
			.ToList();
		_logger.LogInformation("JsonLStreamReader: Schema loaded from JSON ({Count} fields). Path: {Path}",
			arrowSchema.FieldsList.Count, _options.Path ?? "(root)");
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

		// For STDIN we cannot reset after inference — buffer records so they can be replayed during data extraction.
		if (!_isResetSupported)
			_stdinBuffer = new List<Dictionary<string, object?>>(_options.MaxSample);

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			if (record is Dictionary<string, object?> current)
			{
				if (merged == null)
					merged = new Dictionary<string, object?>(current, StringComparer.OrdinalIgnoreCase);
				else
					SchemaDiscoveryHelper.DeepMergeSchemas(merged, current);

				_stdinBuffer?.Add(current);
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
		var autoApplied = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

		foreach (var kvp in merged)
		{
			var (type, arrowType) = InferTypes(kvp.Key, kvp.Value);
			columns.Add(new PipeColumnInfo(kvp.Key, type, true));
			fields.Add(new Field(kvp.Key, arrowType, true));

			var hint = ClrTypeToHint(type);
			if (hint != null) autoApplied[kvp.Key] = hint;
		}

		_autoAppliedTypes = autoApplied;
		Columns = columns;
		Schema = new Schema(fields, null);
		_logger.LogInformation("JsonLStreamReader: Inferred schema with {Count} columns. Path: {Path}", columns.Count, _options.Path ?? "(root)");
	}

	private void BuildSchemaFromColumnTypes()
	{
		var overrides = ParseColumnTypesOption(_options.ColumnTypes);
		var columns = new List<PipeColumnInfo>();
		var fields = new List<Field>();

		foreach (var kvp in overrides)
		{
			var clrType = ResolveHintToClrType(kvp.Value) ?? typeof(string);
			var arrowResult = ArrowTypeMapper.GetLogicalType(clrType);
			columns.Add(new PipeColumnInfo(kvp.Key, clrType, true));
			if (arrowResult.Metadata != null)
				fields.Add(new Field(kvp.Key, arrowResult.ArrowType, true, arrowResult.Metadata));
			else
				fields.Add(new Field(kvp.Key, arrowResult.ArrowType, true));
		}

		Columns = columns;
		Schema = new Schema(fields, null);
		_logger.LogInformation("JsonLStreamReader: Schema built from ColumnTypes with {Count} columns. Path: {Path}", columns.Count, _options.Path ?? "(root)");
	}

	/// <summary>
	/// Maps an Arrow field to its simplified CLR type, using typeof(object) for complex types
	/// (ListType, StructType, MapType) to match the inference path. This prevents ArrowSchemaFactory
	/// from failing when building a schema from PipeColumnInfo with complex column types.
	/// </summary>
	private static Type GetSimplifiedClrType(Field f)
	{
		return f.DataType switch
		{
			ListType or LargeListType or StructType or MapType => typeof(object),
			_ => ArrowTypeMapper.GetClrTypeFromField(f)
		};
	}

	// ── IColumnTypeInferenceCapable ───────────────────────────────────────────

	public async Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(
		int sampleRows, CancellationToken ct = default)
	{
		if (!_isResetSupported)
			return new Dictionary<string, string>(); // STDIN cannot be re-read

		Dictionary<string, object?>? merged = null;
		int count = 0;
		var encoding = Encoding.GetEncoding(_options.Encoding);

		if (string.IsNullOrEmpty(_options.Path))
		{
			// Classic JSONL: read N lines from a fresh stream
			await using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
				bufferSize: 65536, FileOptions.Asynchronous);
			using var sr = new StreamReader(fs, encoding);

			while (count < sampleRows && !ct.IsCancellationRequested)
			{
				var line = await sr.ReadLineAsync(ct);
				if (line == null) break;
				if (string.IsNullOrWhiteSpace(line)) continue;

				var dict = ParseLineToDictionary(line);
				if (merged == null)
					merged = new Dictionary<string, object?>(dict, StringComparer.OrdinalIgnoreCase);
				else
					SchemaDiscoveryHelper.DeepMergeSchemas(merged, dict);
				count++;
			}
		}
		else
		{
			// Hierarchical JSON: stream elements via JsonStreamingPathReader
			var pathParts = _options.Path.Split('.', StringSplitOptions.RemoveEmptyEntries);
			await using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
				bufferSize: 65536, FileOptions.Asynchronous);

			await foreach (var element in JsonStreamingPathReader.StreamArrayAsync(fs, pathParts, ct))
			{
				var converted = ToValue(element);
				if (converted is Dictionary<string, object?> d)
				{
					if (merged == null)
						merged = new Dictionary<string, object?>(d, StringComparer.OrdinalIgnoreCase);
					else
						SchemaDiscoveryHelper.DeepMergeSchemas(merged, d);
				}
				count++;
				if (count >= sampleRows) break;
			}
		}

		if (merged == null) return new Dictionary<string, string>();

		var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		foreach (var kvp in merged)
		{
			var (clrType, _) = InferTypes(kvp.Key, kvp.Value);
			var hint = ClrTypeToHint(clrType);
			if (hint != null) result[kvp.Key] = hint;
		}
		return result;
	}

	private async IAsyncEnumerable<object?> EnumerateRawRecordsAsync([EnumeratorCancellation] CancellationToken ct)
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
			// Hierarchical JSON: stream elements from the target path array.
			var pathParts = _options.Path.Split('.', StringSplitOptions.RemoveEmptyEntries);

			if (_isResetSupported)
			{
				// File-based: open an independent stream so memory usage is O(buffer + 1 element).
				await using var rawStream = new FileStream(
					_filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
					bufferSize: 65536, FileOptions.Asynchronous);

				await foreach (var element in JsonStreamingPathReader.StreamArrayAsync(rawStream, pathParts, ct))
				{
					var converted = ToValue(element);
					if (converted is Dictionary<string, object?> d) yield return d;
					else yield return new Dictionary<string, object?> { ["value"] = converted };
				}
			}
			else
			{
				// STDIN: cannot re-open the stream, load into memory.
				// (STDIN inputs are typically small; for large inputs use a file path instead.)
				var fullJson = await _streamReader!.ReadToEndAsync(ct);
				if (string.IsNullOrEmpty(fullJson)) yield break;

				using var doc = JsonDocument.Parse(fullJson);
				var root = doc.RootElement;
				var current = root;
				bool found = true;
				foreach (var part in pathParts)
				{
					if (current.TryGetProperty(part, out var next)) current = next;
					else { found = false; break; }
				}

				if (found && current.ValueKind == JsonValueKind.Array)
				{
					foreach (var item in current.EnumerateArray())
						yield return ToValue(item) as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["value"] = ToValue(item) };
				}
			}
		}
	}

	private (Type ClrType, IArrowType ArrowType) InferTypes(string key, object? value)
	{
		return value switch
		{
			long => (typeof(long), Int64Type.Default),
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
		[EnumeratorCancellation] CancellationToken ct = default)
	{
		int batchSize = 1000;
		if (_streamReader is null || Columns is null || Schema is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new List<Dictionary<string, object?>>(batchSize);

		// Drain STDIN buffer (records consumed during schema inference)
		if (_stdinBuffer != null)
		{
			foreach (var buffered in _stdinBuffer)
			{
				batch.Add(buffered);
				if (batch.Count >= batchSize)
				{
					yield return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema);
					batch.Clear();
				}
			}
			_stdinBuffer = null;
		}

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
		[EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_streamReader is null || Columns is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var batch = new object?[batchSize][];
		var index = 0;

		// Drain STDIN buffer (records consumed during schema inference)
		if (_stdinBuffer != null)
		{
			foreach (var buffered in _stdinBuffer)
			{
				batch[index++] = MapDictToRow(buffered);
				if (index >= batchSize)
				{
					yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
					batch = new object?[batchSize][];
					index = 0;
				}
			}
			_stdinBuffer = null;
		}

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
			// Prefer int64 over double to avoid precision loss for large integer values (> 2^53).
			JsonValueKind.Number => element.TryGetInt64(out var l) ? (object?)l
			                      : element.TryGetDouble(out var d) ? d
			                      : element.GetRawText(),
			JsonValueKind.True => true,
			JsonValueKind.False => false,
			JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ToValue(p.Value)),
			JsonValueKind.Array => element.EnumerateArray().Select(ToValue).ToList<object?>(),
			JsonValueKind.Null => null,
			_ => element.GetRawText()
		};
	}

	// ── Type hint helpers ─────────────────────────────────────────────────────

	private static Dictionary<string, string> ParseColumnTypesOption(string spec)
	{
		var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		if (string.IsNullOrWhiteSpace(spec)) return result;
		foreach (var entry in spec.Split(',', StringSplitOptions.RemoveEmptyEntries))
		{
			var idx = entry.IndexOf(':');
			if (idx <= 0) continue;
			var name = entry[..idx].Trim();
			var typeName = entry[(idx + 1)..].Trim();
			if (!string.IsNullOrEmpty(name) && !string.IsNullOrEmpty(typeName))
				result[name] = typeName;
		}
		return result;
	}

	private static Type? ResolveHintToClrType(string hint) => hint.ToLowerInvariant() switch
	{
		"uuid" or "guid" => typeof(Guid),
		"string" or "text" => typeof(string),
		"int32" or "int" or "integer" => typeof(int),
		"int64" or "long" => typeof(long),
		"double" or "float64" or "number" => typeof(double),
		"decimal" => typeof(decimal),
		"bool" or "boolean" => typeof(bool),
		"datetime" => typeof(DateTime),
		"datetimeoffset" or "datetimetz" => typeof(DateTimeOffset),
		_ => null
	};

	/// <summary>Maps a CLR type back to the type hint string used in --json-column-types.</summary>
	private static string? ClrTypeToHint(Type t)
	{
		if (t == typeof(string)) return "string";
		if (t == typeof(long)) return "int64";
		if (t == typeof(double)) return "double";
		if (t == typeof(bool)) return "bool";
		if (t == typeof(Guid)) return "uuid";
		if (t == typeof(int)) return "int32";
		if (t == typeof(decimal)) return "decimal";
		if (t == typeof(DateTime)) return "datetime";
		if (t == typeof(DateTimeOffset)) return "datetimeoffset";
		return null; // object, List, etc. → omit (no portable scalar hint)
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
