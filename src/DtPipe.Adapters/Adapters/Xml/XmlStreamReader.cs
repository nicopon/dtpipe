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

namespace DtPipe.Adapters.Xml;

public class XmlStreamReader : IStreamReader, IColumnarStreamReader
{
	private readonly string _filePath;
	private readonly XmlReaderOptions _options;
	private readonly ILogger _logger;

	private Stream? _stream;
	private XmlReader? _xmlReader;
	private readonly Stack<string> _pathStack = new();
	private string[]? _targetPathParts;
	private string? _lastPathPart;
	private bool _isRecursiveSearch;

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
			if (_pathStack.Count != _targetPathParts.Length) return false;
			var i = 0;
			foreach (var part in _pathStack.Reverse())
			{
				if (part != _targetPathParts[i++]) return false;
			}
			return true;
		}
	}

	private async Task InferSchemaAsync(CancellationToken ct)
	{
		if (_xmlReader == null) throw new InvalidOperationException("XmlReader is null.");

		await Task.Run(() =>
		{
			while (_xmlReader.Read())
			{
				if (_xmlReader.NodeType == XmlNodeType.Element)
				{
					_pathStack.Push(_xmlReader.LocalName);

					if (IsMatch())
					{
						using var subReader = _xmlReader.ReadSubtree();
						subReader.Read(); 
						var record = ParseElement(subReader);
						_firstNodeDict = record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record };
						break;
					}
				}
				else if (_xmlReader.NodeType == XmlNodeType.EndElement)
				{
					if (_pathStack.Count > 0) _pathStack.Pop();
				}
			}
		}, ct);

		UpdateSchemaFromFirstNode();
	}

	private void UpdateSchemaFromFirstNode()
	{
		if (_firstNodeDict == null)
		{
			Columns = System.Array.Empty<PipeColumnInfo>();
			Schema = new Schema(Enumerable.Empty<Field>(), null);
			return;
		}

		var columns = new List<PipeColumnInfo>();
		var fields = new List<Field>();

		foreach (var kvp in _firstNodeDict)
		{
			var (type, arrowType) = InferTypes(kvp.Value);
			columns.Add(new PipeColumnInfo(kvp.Key, type, true));
			fields.Add(new Field(kvp.Key, arrowType, true));
		}

		Columns = columns;
		Schema = new Schema(fields, null);
		_logger.LogInformation("XmlStreamReader: Inferred schema with {Count} columns.", columns.Count);
	}

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

			while (_xmlReader!.Read())
			{
				if (ct.IsCancellationRequested) break;

				if (_xmlReader.NodeType == XmlNodeType.Element)
				{
					_pathStack.Push(_xmlReader.LocalName);

					if (IsMatch())
					{
						using var subReader = _xmlReader.ReadSubtree();
						subReader.Read(); 
						var record = ParseElement(subReader);
						batch.Add(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record });

						if (batch.Count >= batchSize)
						{
							var recordBatch = Apache.Arrow.Serialization.ArrowSerializer.SerializeAsync(batch, Schema!).GetAwaiter().GetResult();
							writer.TryWrite(recordBatch);
							batch.Clear();
						}
					}
				}
				else if (_xmlReader.NodeType == XmlNodeType.EndElement)
				{
					if (_pathStack.Count > 0) _pathStack.Pop();
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

			while (_xmlReader!.Read())
			{
				if (ct.IsCancellationRequested) break;

				if (_xmlReader.NodeType == XmlNodeType.Element)
				{
					_pathStack.Push(_xmlReader.LocalName);

					if (IsMatch())
					{
						using var subReader = _xmlReader.ReadSubtree();
						subReader.Read(); 
						var record = ParseElement(subReader);
						batchData[index++] = MapDictToRow(record as Dictionary<string, object?> ?? new Dictionary<string, object?> { ["_value"] = record });

						if (index >= batchSize)
						{
							writer.TryWrite(new ReadOnlyMemory<object?[]>(batchData, 0, index));
							batchData = new object?[batchSize][];
							index = 0;
						}
					}
				}
				else if (_xmlReader.NodeType == XmlNodeType.EndElement)
				{
					if (_pathStack.Count > 0) _pathStack.Pop();
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
			row[i] = dict.GetValueOrDefault(colName);
		}

		return row;
	}

	private object? ParseElement(XmlReader reader)
	{
		Dictionary<string, object?>? dict = null;

		if (reader.HasAttributes)
		{
			dict = new Dictionary<string, object?>(reader.AttributeCount);
			for (int i = 0; i < reader.AttributeCount; i++)
			{
				reader.MoveToAttribute(i);
				dict[_options.AttributePrefix + reader.LocalName] = reader.Value;
			}
			reader.MoveToElement();
		}

		if (reader.IsEmptyElement) return dict ?? (object?)new Dictionary<string, object?>();

		string? textValue = null;
		bool hasChildElements = false;

		while (reader.Read())
		{
			switch (reader.NodeType)
			{
				case XmlNodeType.Element:
					hasChildElements = true;
					dict ??= new Dictionary<string, object?>();
					var name = reader.LocalName;
					var value = ParseElement(reader);

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
					textValue = textValue == null ? reader.Value : textValue + reader.Value;
					break;

				case XmlNodeType.EndElement:
					goto Done;
			}
		}

	Done:
		if (!hasChildElements)
		{
			if (dict == null) return textValue ?? "";
			if (textValue != null) dict["_value"] = textValue;
			return dict;
		}

		if (textValue != null && dict != null)
		{
			dict["_value"] = textValue;
		}

		return dict ?? (object?)new Dictionary<string, object?>();
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
