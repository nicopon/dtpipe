using System.Text;
using System.Xml;
using System.Xml.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading.Channels;
using System.Globalization;

namespace DtPipe.Adapters.Xml;

public class XmlStreamReader : IStreamReader, IColumnarStreamReader, IColumnTypeInferenceCapable
{
	private readonly string _filePath;
	private readonly XmlReaderOptions _options;
	private readonly ILogger _logger;

	private Stream? _stream;
	private XmlReader? _xmlReader;
	private readonly string[] _path = new string[256];
	private int _depth = 0;
	private string[]? _targetPathParts;
	private string? _lastPathPart;
	private bool _isRecursiveSearch;

	private Dictionary<string, Func<string, object?>>? _columnParsers;
	private Dictionary<string, Type>? _typeOverrides;
	private IReadOnlyDictionary<string, string>? _autoAppliedTypes;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema { get; private set; }

	public XmlStreamReader(string filePath, XmlReaderOptions options, ILogger? logger = null)
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
				throw new InvalidOperationException("Structure input (STDIN) is not redirected. To read from STDIN, pipe data into dtpipe.");
			}
			_stream = Console.OpenStandardInput();
		}
		else
		{
			if (!File.Exists(_filePath))
				throw new FileNotFoundException($"XML file not found: {_filePath}", _filePath);

			_stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _options.BufferSize, FileOptions.Asynchronous);
		}

		var settings = new XmlReaderSettings
		{
			Async = false,
			IgnoreComments = true,
			IgnoreProcessingInstructions = true,
			IgnoreWhitespace = true
		};

		_xmlReader = XmlReader.Create(_stream, settings);

		InitializePathMatcher();

		// Build column parsers from --xml-column-types option
		_typeOverrides = ParseColumnTypesOption(_options.ColumnTypes);

		// Auto-infer types when --xml-auto-column-types is set
		if (_options.AutoColumnTypes && !string.IsNullOrEmpty(_filePath) && _filePath != "-")
		{
			try
			{
				const int autoSampleCount = 100;
				var inferred = await InferColumnTypesAsync(autoSampleCount, ct);
				if (inferred.Count > 0)
				{
					_autoAppliedTypes = inferred;
					foreach (var kv in inferred)
					{
						var clrType = ResolveHintToClrType(kv.Value);
						if (clrType != null && !_typeOverrides.ContainsKey(kv.Key))
						{
							_typeOverrides[kv.Key] = clrType;
						}
					}
				}
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Auto column-type inference failed, falling back to string columns.");
			}
		}

		_columnParsers = BuildColumnParsers(_typeOverrides);

		// Infer schema from the first matching node
		await InferSchemaAsync(ct);
	}

	private void InitializePathMatcher()
	{
		var path = _options.Path;
		if (path.StartsWith("//"))
		{
			_isRecursiveSearch = true;
			_targetPathParts = path.Substring(2).Split('/', StringSplitOptions.RemoveEmptyEntries);
		}
		else
		{
			_isRecursiveSearch = false;
			_targetPathParts = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
		}
		_lastPathPart = _targetPathParts?.Length > 0 ? _targetPathParts[^1] : null;
	}

	private bool IsMatch()
	{
		if (_xmlReader == null || _targetPathParts == null) return false;

		if (_isRecursiveSearch)
		{
			// Simple recursive search: matches if the current local name matches the last part of target path
			return _xmlReader.LocalName == _lastPathPart;
		}
		else
		{
			// Strict path from root
			if (_depth != _targetPathParts.Length) return false;
			for (int i = 0; i < _depth; i++)
			{
				if (_path[i] != _targetPathParts[i]) return false;
			}
			return true;
		}
	}

	private IEnumerable<object?> EnumerateRawRecords(CancellationToken ct)
	{
		if (_xmlReader == null) yield break;

		while (_xmlReader.Read())
		{
			if (ct.IsCancellationRequested) yield break;

			if (_xmlReader.NodeType == XmlNodeType.Element)
			{
				if (_depth < _path.Length) _path[_depth] = _xmlReader.LocalName;
				_depth++;

				bool isEmptyElement = _xmlReader.IsEmptyElement;

				if (IsMatch())
				{
					using var subReader = _xmlReader.ReadSubtree();
					subReader.Read(); 
					yield return ParseElement(subReader);

					// When ReadSubtree is disposed, the original reader is positioned on the EndElement
					// (or the empty element itself). The next Read() will advance past it, skipping the EndElement.
					// So we decrement the depth manually here.
					_depth--;
				}
				else if (isEmptyElement)
				{
					// If it's an empty element and not matched, no EndElement will be yielded by the reader.
					_depth--;
				}
			}
			else if (_xmlReader.NodeType == XmlNodeType.EndElement)
			{
				if (_depth > 0) _depth--;
			}
		}
	}

	private async Task InferSchemaAsync(CancellationToken ct)
	{
		if (_xmlReader == null) throw new InvalidOperationException("XmlReader is null.");

		await Task.Run(() =>
		{
			foreach (var record in EnumerateRawRecords(ct))
			{
				_firstNodeDict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record };
				break;
			}
		}, ct);

		UpdateSchemaFromFirstNode();
	}

	private void UpdateSchemaFromFirstNode()
	{
		var columns = new List<PipeColumnInfo>();
		var fields = new List<Field>();
		var seenPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		// 1. Add columns from the first record
		if (_firstNodeDict != null)
		{
			var flattenedFirst = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
			FlattenDictionary(_firstNodeDict, "", flattenedFirst);
			
			foreach (var path in flattenedFirst.Keys)
			{
				var clrType = _typeOverrides?.TryGetValue(path, out var t) == true ? t : typeof(string);
				var arrowType = InferArrowTypeFromPath(path, flattenedFirst[path].FirstOrDefault());
				
				columns.Add(new PipeColumnInfo(path, clrType, true));
				fields.Add(new Field(path, arrowType, true));
				seenPaths.Add(path);
			}
		}

		// 2. Add columns from auto-inference that weren't in the first record
		if (_autoAppliedTypes != null)
		{
			foreach (var kvp in _autoAppliedTypes)
			{
				if (!seenPaths.Contains(kvp.Key))
				{
					var clrType = ResolveHintToClrType(kvp.Value) ?? typeof(string);
					var arrowType = ResolveHintToArrowType(kvp.Value);
					
					columns.Add(new PipeColumnInfo(kvp.Key, clrType, true));
					fields.Add(new Field(kvp.Key, arrowType, true));
					seenPaths.Add(kvp.Key);
				}
			}
		}

		if (columns.Count == 0)
		{
			Columns = System.Array.Empty<PipeColumnInfo>();
			Schema = new Schema(Enumerable.Empty<Field>(), null);
			return;
		}

		Columns = columns;
		Schema = new Schema(fields, null);
		_logger.LogInformation("XmlStreamReader: Inferred schema with {Count} columns (including auto-types).", columns.Count);
	}

	private IArrowType InferArrowTypeFromPath(string path, object? sampleValue)
	{
		if (_autoAppliedTypes != null && _autoAppliedTypes.TryGetValue(path, out var hint))
		{
			return ResolveHintToArrowType(hint);
		}
		var (_, arrowType) = InferTypes(sampleValue);
		return arrowType;
	}

	private static IArrowType ResolveHintToArrowType(string hint) => hint.ToLowerInvariant() switch
	{
		"uuid" or "guid" => new FixedSizeBinaryType(16),
		"int32" => Int32Type.Default,
		"int64" => Int64Type.Default,
		"double" => DoubleType.Default,
		"float" => FloatType.Default,
		"decimal" => new Decimal128Type(38, 18),
		"bool" or "boolean" => BooleanType.Default,
		"datetime" => TimestampType.Default,
		"datetimeoffset" => TimestampType.Default,
		_ => StringType.Default
	};

	public IReadOnlyDictionary<string, string>? AutoAppliedTypes => _autoAppliedTypes;

	private Dictionary<string, object?>? _firstNodeDict;

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(2) 
		{ 
			FullMode = BoundedChannelFullMode.Wait,
			SingleReader = true,
			SingleWriter = true
		});

		_ = Task.Run(() => RunRecordBatchSyncLoop(channel.Writer, ct), ct);

		await foreach (var batch in channel.Reader.ReadAllAsync(ct))
		{
			yield return batch;
		}
	}

	private void RunRecordBatchSyncLoop(ChannelWriter<RecordBatch> writer, CancellationToken ct)
	{
		try
		{
			int batchSize = 10000;
			var batch = new List<Dictionary<string, object?>>(batchSize);

			if (_firstNodeDict != null)
			{
				batch.Add(_firstNodeDict);
				_firstNodeDict = null;
			}

			foreach (var record in EnumerateRawRecords(ct))
			{
				batch.Add(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record });

				if (batch.Count >= batchSize)
				{
					var recordBatch = Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!).GetAwaiter().GetResult();
					writer.TryWrite(recordBatch);
					batch.Clear();
				}
			}

			if (batch.Count > 0)
			{
				var recordBatch = Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!).GetAwaiter().GetResult();
				writer.TryWrite(recordBatch);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error in XML sync parsing loop.");
		}
		finally
		{
			writer.TryComplete();
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		var channel = Channel.CreateBounded<ReadOnlyMemory<object?[]>>(new BoundedChannelOptions(2) 
		{ 
			FullMode = BoundedChannelFullMode.Wait,
			SingleReader = true,
			SingleWriter = true
		});

		_ = Task.Run(() => RunRowBatchSyncLoop(channel.Writer, batchSize, ct), ct);

		await foreach (var batch in channel.Reader.ReadAllAsync(ct))
		{
			yield return batch;
		}
	}

	public async Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(int sampleRows, CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_filePath) || _filePath == "-")
			return new Dictionary<string, string>();

		var encoding = Encoding.GetEncoding(_options.Encoding);
		var settings = new XmlReaderSettings
		{
			Async = false,
			IgnoreComments = true,
			IgnoreProcessingInstructions = true,
			IgnoreWhitespace = true
		};

		using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _options.BufferSize, FileOptions.Asynchronous);
		using var xmlReader = XmlReader.Create(fs, settings);
		
		// Temporary instance to scan
		var tempReader = new XmlStreamReader(_filePath, _options, NullLogger.Instance);
		tempReader._xmlReader = xmlReader;
		tempReader.InitializePathMatcher();

		var sampleData = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
		int rowCount = 0;

		foreach (var record in tempReader.EnumerateRawRecords(ct))
		{
			if (record is Dictionary<string, object?> dict)
			{
				FlattenDictionary(dict, "", sampleData);
			}
			else if (record != null)
			{
				if (!sampleData.ContainsKey("_value")) sampleData["_value"] = new List<string>();
				sampleData["_value"].Add(record.ToString() ?? "");
			}

			rowCount++;
			if (rowCount >= sampleRows) break;
		}

		var suggestions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		foreach (var kvp in sampleData)
		{
			var hint = InferTypeHint(kvp.Value) ?? "string";
			suggestions[kvp.Key] = hint;
		}

		return suggestions;
	}

	private void FlattenDictionary(Dictionary<string, object?> dict, string prefix, Dictionary<string, List<string>> samples)
	{
		foreach (var kvp in dict)
		{
			var key = string.IsNullOrEmpty(prefix) ? kvp.Key : $"{prefix}.{kvp.Key}";
			if (kvp.Value is Dictionary<string, object?> nested)
			{
				FlattenDictionary(nested, key, samples);
			}
			else if (kvp.Value is List<object?> list)
			{
				// For inference, we just take the first scalar if it exists or flatten first object
				foreach (var item in list)
				{
					if (item is Dictionary<string, object?> nestedInList)
					{
						FlattenDictionary(nestedInList, key, samples);
					}
					else if (item != null)
					{
						if (!samples.ContainsKey(key)) samples[key] = new List<string>();
						samples[key].Add(item.ToString() ?? "");
					}
				}
			}
			else if (kvp.Value != null)
			{
				if (!samples.ContainsKey(key)) samples[key] = new List<string>();
				samples[key].Add(kvp.Value.ToString() ?? "");
			}
		}
	}

	private void RunRowBatchSyncLoop(ChannelWriter<ReadOnlyMemory<object?[]>> writer, int batchSize, CancellationToken ct)
	{
		try
		{
			var batchData = new object?[batchSize][];
			var index = 0;

			if (_firstNodeDict != null)
			{
				batchData[index++] = MapDictToRow(_firstNodeDict);
				_firstNodeDict = null;
			}

			foreach (var record in EnumerateRawRecords(ct))
			{
				batchData[index++] = MapDictToRow(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record });

				if (index >= batchSize)
				{
					writer.TryWrite(new ReadOnlyMemory<object?[]>(batchData, 0, index));
					batchData = new object?[batchSize][];
					index = 0;
				}
			}

			if (index > 0)
			{
				writer.TryWrite(new ReadOnlyMemory<object?[]>(batchData, 0, index));
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error in XML sync row parsing loop.");
		}
		finally
		{
			writer.TryComplete();
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

	private object? ParseElement(XmlReader reader, string currentKeyPath = "")
	{
		Dictionary<string, object?>? dict = null;

		if (reader.HasAttributes)
		{
			dict = new Dictionary<string, object?>(reader.AttributeCount + 4, StringComparer.Ordinal);
			for (int i = 0; i < reader.AttributeCount; i++)
			{
				reader.MoveToAttribute(i);
				var attrLocalName = reader.LocalName;
				var attrKey = _options.AttributePrefix + attrLocalName;
				var fullPath = string.IsNullOrEmpty(currentKeyPath) ? attrKey : $"{currentKeyPath}.{attrKey}";
				
				dict[attrKey] = ApplyParser(fullPath, reader.Value);
			}
			reader.MoveToElement();
		}

		if (reader.IsEmptyElement) return dict ?? (object?)new Dictionary<string, object?>(StringComparer.Ordinal);

		StringBuilder? textBuilder = null;
		bool hasChildElements = false;

		while (reader.Read())
		{
			switch (reader.NodeType)
			{
				case XmlNodeType.Element:
					hasChildElements = true;
					dict ??= new Dictionary<string, object?>(StringComparer.Ordinal);
					var name = reader.LocalName;
					var childPath = string.IsNullOrEmpty(currentKeyPath) ? name : $"{currentKeyPath}.{name}";
					var value = ParseElement(reader, childPath);

					if (dict.TryGetValue(name, out var existing))
					{
						if (existing is List<object?> list) list.Add(value);
						else dict[name] = new List<object?> { existing, value };
					}
					else
					{
						dict[name] = value;
					}
					break;

				case XmlNodeType.Text:
				case XmlNodeType.CDATA:
					textBuilder ??= new StringBuilder();
					textBuilder.Append(reader.Value);
					break;

				case XmlNodeType.EndElement:
					goto Done;
			}
		}

	Done:
		string? textValue = textBuilder?.ToString();

		if (!hasChildElements)
		{
			if (dict == null) return ApplyParser(currentKeyPath, textValue ?? "");
			if (textValue != null) dict["_value"] = ApplyParser(string.IsNullOrEmpty(currentKeyPath) ? "_value" : $"{currentKeyPath}._value", textValue);
			return dict;
		}

		if (textValue != null && dict != null)
		{
			dict["_value"] = ApplyParser(string.IsNullOrEmpty(currentKeyPath) ? "_value" : $"{currentKeyPath}._value", textValue);
		}

		return dict ?? (object?)new Dictionary<string, object?>(StringComparer.Ordinal);
	}

	private object? ApplyParser(string fullPath, string? value)
	{
		if (value == null) return null;
		if (_columnParsers != null && _columnParsers.TryGetValue(fullPath, out var parser))
		{
			return parser(value);
		}
		return value;
	}

	// ── Helpers ───────────────────────────────────────────────────────────────

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

	private static Dictionary<string, Func<string, object?>> BuildColumnParsers(Dictionary<string, Type> overrides)
	{
		var result = new Dictionary<string, Func<string, object?>>(StringComparer.OrdinalIgnoreCase);
		foreach (var kvp in overrides)
		{
			result[kvp.Key] = BuildParser(kvp.Value);
		}
		return result;
	}

	private static Func<string, object?> BuildParser(Type clrType)
	{
		if (clrType == typeof(Guid))
			return static s => Guid.TryParse(s, out var g) ? g : (object?)null;

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
				return s.ToLowerInvariant() switch { "1" or "yes" or "true" => true, "0" or "no" or "false" => false, _ => (object?)null };
			};

		if (clrType == typeof(DateTime))
			return static s => DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var v) ? v : (object?)null;

		if (clrType == typeof(DateTimeOffset))
			return static s => DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var v) ? v : (object?)null;

		return static s => s;
	}

	private static string? InferTypeHint(List<string> samples)
	{
		if (samples.Count == 0) return null;

		bool allMatch(Func<string, bool> test) => samples.All(test);

		if (allMatch(s => Guid.TryParse(s, out _))) return "uuid";

		if (allMatch(s => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)))
		{
			return samples.All(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)) ? "int32" : "int64";
		}

		if (allMatch(s => decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out _) && s.Contains('.')))
		{
			bool needsDecimalPrecision = samples.Any(s =>
			{
				if (!decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var d)) return false;
				if (!double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var dbl)) return true;
				return d != (decimal)dbl;
			});
			return needsDecimalPrecision ? "decimal" : "double";
		}

		if (allMatch(s => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out _))) return "double";

		if (allMatch(s => bool.TryParse(s, out _) || s.ToLowerInvariant() is "0" or "1" or "yes" or "no" or "true" or "false")) return "bool";

		if (allMatch(s => DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))) return "datetime";

		return null;
	}

	private (Type ClrType, IArrowType ArrowType) InferTypes(object? value)
	{
		return value switch
		{
			bool => (typeof(bool), BooleanType.Default),
			double => (typeof(double), DoubleType.Default),
			long => (typeof(long), Int64Type.Default),
			int => (typeof(int), Int32Type.Default),
			Dictionary<string, object?> => (typeof(object), InferStructType((Dictionary<string, object?>)value)),
			List<object?> => (typeof(object), InferListType((List<object?>)value)),
			_ => (typeof(string), StringType.Default)
		};
	}

	private IArrowType InferStructType(Dictionary<string, object?> dict)
	{
		var fields = new List<Field>();
		foreach (var kvp in dict)
		{
			var (_, arrowType) = InferTypes(kvp.Value);
			fields.Add(new Field(kvp.Key, arrowType, true));
		}
		return new StructType(fields);
	}

	private IArrowType InferListType(List<object?> list)
	{
		if (list.Any())
		{
			var first = list.First();
			var (_, itemArrowType) = InferTypes(first);
			return new ListType(itemArrowType);
		}
		return new ListType(StringType.Default);
	}

	public async ValueTask DisposeAsync()
	{
		if (_xmlReader != null)
		{
			_xmlReader.Dispose();
			_xmlReader = null;
		}

		if (_stream != null)
		{
			await _stream.DisposeAsync();
			_stream = null;
		}
	}
}
