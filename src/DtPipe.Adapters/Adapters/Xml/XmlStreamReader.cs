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

	private readonly XmlReaderSettings _xmlReaderSettings;
	private Stream? _stream;
	private XmlReader? _xmlReader;
	private readonly string[] _path = new string[256];
	private int _depth = 0;
	private string[]? _targetPathParts;
	private string? _lastPathPart;
	private bool _isRecursiveSearch;

	private bool _isResetSupported;
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
		_xmlReaderSettings = new XmlReaderSettings 
		{ 
			Async = true, 
			IgnoreWhitespace = true,
			IgnoreComments = true,
			IgnoreProcessingInstructions = true,
			ConformanceLevel = ConformanceLevel.Fragment
		};
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

			_stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _options.BufferSize, FileOptions.None);
		}

		InitializePathMatcher();
		_stream = new FragmentXmlStream(_stream);

		var settings = new XmlReaderSettings
		{
			Async = false,
			IgnoreComments = true,
			IgnoreProcessingInstructions = true,
			IgnoreWhitespace = true,
			ConformanceLevel = ConformanceLevel.Fragment
		};

		_xmlReader = XmlReader.Create(_stream, settings);

		InitializePathMatcher();

		_typeOverrides = ParseColumnTypesOption(_options.ColumnTypes);
		_columnParsers = BuildColumnParsers(_typeOverrides);
		_isResetSupported = !string.IsNullOrEmpty(_filePath) && _filePath != "-";
		await ResetReaderAsync(ct);
		
		// 1. Auto-discover types (Pass 1)
		if (_options.AutoColumnTypes)
		{
			var autoSampleCount = 100;
			var inferred = await InferColumnTypesAsync(autoSampleCount, ct);
			_autoAppliedTypes = inferred;
			
			// Reset for next pass
			await ResetReaderAsync(ct);
		}

		// 2. Infer schema (Pass 2)
		await InferSchemaAsync(ct);
		
		// 3. Reset for final data extraction (Pass 3)
		await ResetReaderAsync(ct);
	}

	private async Task ResetReaderAsync(CancellationToken ct)
	{
		_depth = 0;
		if (!_isResetSupported)
		{
			// STDIN: We cannot reset. We must have already opened it in OpenAsync
			if (_xmlReader == null)
			{
				var standardInput = Console.OpenStandardInput();
				var fragmentStream = new FragmentXmlStream(standardInput);
				_xmlReader = XmlReader.Create(fragmentStream, _xmlReaderSettings);
			}
			return;
		}

		// File-based: Close and re-open to ensure fresh start
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

		if (!File.Exists(_filePath))
			throw new FileNotFoundException($"XML file not found: {_filePath}", _filePath);

		_stream = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _options.BufferSize, FileOptions.None);
		var fragStream = new FragmentXmlStream(_stream);
		_xmlReader = XmlReader.Create(fragStream, _xmlReaderSettings);
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

	private async IAsyncEnumerable<object?> EnumerateRawRecordsAsync(CancellationToken ct)
	{
		if (_xmlReader == null) yield break;

		while (await _xmlReader.ReadAsync())
		{
			if (ct.IsCancellationRequested) yield break;

			if (_xmlReader.NodeType == XmlNodeType.Element)
			{
				var name = _xmlReader.LocalName;
				if (_depth < _path.Length) _path[_depth] = name;
				_depth++;

				bool isEmptyElement = _xmlReader.IsEmptyElement;

				if (IsMatch())
				{
					// For matched elements, we consume them entirely with ReadSubtree
					using (var subReader = _xmlReader.ReadSubtree())
					{
						// subReader itself is unfortunately not fully async for all operations in some .NET versions
						// but its base reader is.
						subReader.Read(); // Must be called once to position on the element
						object? record = ParseElement(subReader, "");
						if (record != null) yield return record;
					}
					
					// After ReadSubtree, decrement depth since we're done with the matched element
					_depth--;
				}
				else if (isEmptyElement)
				{
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

		Dictionary<string, object?>? merged = null;
		int count = 0;
		int maxSample = 1000;

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			var current = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record };
			if (merged == null)
			{
				merged = new Dictionary<string, object?>(current, StringComparer.OrdinalIgnoreCase);
			}
			else
			{
				DeepMergeSchemas(merged, current);
			}

			count++;
			if (count >= maxSample) break;
		}

		_firstNodeDict = merged;
		UpdateSchemaFromFirstNode();
	}

	private void DeepMergeSchemas(Dictionary<string, object?> target, Dictionary<string, object?> source)
	{
		foreach (var kvp in source)
		{
			if (!target.TryGetValue(kvp.Key, out var existing))
			{
				target[kvp.Key] = kvp.Value;
			}
			else if (existing is Dictionary<string, object?> targetDict && kvp.Value is Dictionary<string, object?> sourceDict)
			{
				DeepMergeSchemas(targetDict, sourceDict);
			}
			else if (existing == null && kvp.Value != null)
			{
				target[kvp.Key] = kvp.Value;
			}
		}
	}

	private void UpdateSchemaFromFirstNode()
	{
		var fields = new List<Field>();
		var seenPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

		// 1. Add columns from the top-level keys of the first record
		if (_firstNodeDict != null)
		{
			foreach (var kvp in _firstNodeDict)
			{
				var name = kvp.Key;
				var sampleValue = kvp.Value;

				var (clrType, arrowType) = InferTypes(sampleValue, name);
				fields.Add(new Field(name, arrowType, true));
				seenPaths.Add(name);
			}
		}

		// 2. Add columns from auto-inference that were discovered during sampling
		// (Only include TOP-LEVEL keys gathered from other nodes to handle sparse schemas)
		if (_autoAppliedTypes != null)
		{
			foreach (var kvp in _autoAppliedTypes)
			{
				// If it's a top-level key (no dots) and hasn't been seen yet, add it
				if (!kvp.Key.Contains('.') && !seenPaths.Contains(kvp.Key))
				{
					var arrowType = ResolveHintToArrowType(kvp.Value);
					fields.Add(new Field(kvp.Key, arrowType, true));
					seenPaths.Add(kvp.Key);
				}
			}
		}

		if (fields.Count == 0)
		{
			Columns = System.Array.Empty<PipeColumnInfo>();
			Schema = new Schema(Enumerable.Empty<Field>(), null);
			return;
		}

		// Columns property is used for UI and SQL/transformers mapping
		Columns = fields.Select(f => {
			var clrType = typeof(object);
			if (f.DataType is StringType) clrType = typeof(string);
			else if (f.DataType is Int32Type) clrType = typeof(int);
			else if (f.DataType is Int64Type) clrType = typeof(long);
			else if (f.DataType is DoubleType) clrType = typeof(double);
			else if (f.DataType is BooleanType) clrType = typeof(bool);
			else if (f.DataType is Decimal128Type) clrType = typeof(decimal);
			else if (f.DataType is TimestampType) clrType = typeof(DateTime);
			return new PipeColumnInfo(f.Name, clrType, true);
		}).ToList();
		Schema = new Schema(fields, null);
		_logger.LogInformation("XmlStreamReader: Inferred hierarchical schema with {Count} top-level columns.", fields.Count);
	}

	private IArrowType InferArrowTypeFromPath(string path, object? sampleValue)
	{
		if (_autoAppliedTypes != null && _autoAppliedTypes.TryGetValue(path, out var hint))
		{
			return ResolveHintToArrowType(hint);
		}
		var (_, arrowType) = InferTypes(sampleValue, path);
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
		int batchSize = 10000;
		var batch = new List<Dictionary<string, object?>>(batchSize);

		// Only yield _firstNodeDict if we are NOT at the beginning of the stream (e.g. STDIN)
		if (!_isResetSupported && _firstNodeDict != null)
		{
			var dict = _firstNodeDict as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = _firstNodeDict };
			batch.Add(dict);
			_firstNodeDict = null;
		}
		else
		{
			_firstNodeDict = null; // Re-read from beginning in loop
		}

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			var dict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record };
			batch.Add(dict);

			if (batch.Count >= batchSize)
			{
				yield return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!);
				batch.Clear();
			}
		}

		if (batch.Count > 0)
		{
			yield return await Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!);
			batch.Clear();
		}
	}


	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		var batchData = new object?[batchSize][];
		var index = 0;

		// Only yield _firstNodeDict if we are NOT at the beginning of the stream
		if (!_isResetSupported && _firstNodeDict != null)
		{
			batchData[index++] = MapDictToRow(_firstNodeDict);
			_firstNodeDict = null;
		}
		else
		{
			_firstNodeDict = null; 
		}

		await foreach (var record in EnumerateRawRecordsAsync(ct))
		{
			batchData[index++] = MapDictToRow(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record });

			if (index >= batchSize)
			{
				yield return new ReadOnlyMemory<object?[]>(batchData, 0, index);
				batchData = new object?[batchSize][];
				index = 0;
			}
		}

		if (index > 0)
		{
			yield return new ReadOnlyMemory<object?[]>(batchData, 0, index);
		}
	}

	public async Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(int sampleRows, CancellationToken ct = default)
	{
		var sampleData = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
		int rowCount = 0;

		await foreach (var record in EnumerateRawRecordsAsync(ct))
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

	private static IArrowType ClrToArrowType(Type clrType)
	{
		if (clrType == typeof(int)) return Int32Type.Default;
		if (clrType == typeof(long)) return Int64Type.Default;
		if (clrType == typeof(double)) return DoubleType.Default;
		if (clrType == typeof(float)) return FloatType.Default;
		if (clrType == typeof(decimal)) return new Decimal128Type(38, 18);
		if (clrType == typeof(bool)) return BooleanType.Default;
		if (clrType == typeof(DateTime)) return TimestampType.Default;
		if (clrType == typeof(DateTimeOffset)) return TimestampType.Default;
		if (clrType == typeof(Guid)) return StringType.Default;
		return StringType.Default;
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

	private (Type ClrType, IArrowType ArrowType) InferTypes(object? value, string currentPath = "")
	{
		// Check for overrides first
		if (_typeOverrides != null && _typeOverrides.TryGetValue(currentPath, out var overridenType))
		{
			return (overridenType, ClrToArrowType(overridenType));
		}

		if (_autoAppliedTypes != null && _autoAppliedTypes.TryGetValue(currentPath, out var hint))
		{
			return (ResolveHintToClrType(hint) ?? typeof(string), ResolveHintToArrowType(hint));
		}

		return value switch
		{
			bool => (typeof(bool), BooleanType.Default),
			double => (typeof(double), DoubleType.Default),
			long => (typeof(long), Int64Type.Default),
			int => (typeof(int), Int32Type.Default),
			Dictionary<string, object?> d => (typeof(object), InferStructType(d, currentPath)),
			List<object?> l => (typeof(object), InferListType(l, currentPath)),
			_ => (typeof(string), StringType.Default)
		};
	}

	private IArrowType InferStructType(Dictionary<string, object?> dict, string currentPath)
	{
		var fields = new List<Field>();
		foreach (var kvp in dict)
		{
			var childPath = string.IsNullOrEmpty(currentPath) ? kvp.Key : $"{currentPath}.{kvp.Key}";
			var (_, arrowType) = InferTypes(kvp.Value, childPath);
			fields.Add(new Field(kvp.Key, arrowType, true));
		}
		return new StructType(fields);
	}

	private IArrowType InferListType(List<object?> list, string currentPath)
	{
		if (list.Any())
		{
			var first = list.First();
			// For lists, the path context stays the same as the element path
			var (_, itemArrowType) = InferTypes(first, currentPath);
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

	// ── Fragment Support ───────────────────────────────────────────────────

	private class FragmentXmlStream : Stream
	{
		private readonly Stream _inner;
		private bool _firstDeclarationFound = false;
		private int _state = 0; // 0: Searching for <, 1: <, 2: <?, 3: <?x, 4: <?xm, 5: <?xml, 6: Neutralizing

		public FragmentXmlStream(Stream inner) => _inner = inner;

		public override bool CanRead => _inner.CanRead;
		public override bool CanSeek => _inner.CanSeek;
		public override bool CanWrite => false;
		public override long Length => _inner.Length;
		public override long Position { get => _inner.Position; set => _inner.Position = value; }

		public override void Flush() => _inner.Flush();
		public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
		public override void SetLength(long value) => throw new NotSupportedException();
		public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

		public override int Read(byte[] buffer, int offset, int count)
		{
			int bytesRead = _inner.Read(buffer, offset, count);
			if (bytesRead <= 0) return bytesRead;

			for (int i = offset; i < offset + bytesRead; i++)
			{
				byte b = buffer[i];

				if (_state == 6) // Neutralizing mode: replace with spaces until '>'
				{
					buffer[i] = (byte)' ';
					if (b == (byte)'>') _state = 0;
					continue;
				}

				if (b == (byte)'<') _state = 1;
				else if (b == (byte)'?' && _state == 1) _state = 2;
				else if (b == (byte)'x' && _state == 2) _state = 3;
				else if (b == (byte)'m' && _state == 3) _state = 4;
				else if (b == (byte)'l' && _state == 4) 
				{
					if (!_firstDeclarationFound)
					{
						_firstDeclarationFound = true;
						_state = 0;
					}
					else
					{
						_state = 6; // Start neutralizing remaining of declaration
						for (int k = 0; k < 5; k++)
						{
							if (i - k >= offset) buffer[i - k] = (byte)' ';
						}
					}
				}
				else _state = 0;
			}

			return bytesRead;
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing) _inner.Dispose();
			base.Dispose(disposing);
		}
	}
}
