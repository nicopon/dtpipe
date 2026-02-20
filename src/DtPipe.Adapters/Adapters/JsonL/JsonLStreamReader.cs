using System.Text;
using System.Text.Json;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.JsonL;

public class JsonLStreamReader : IStreamReader
{
	private readonly string _filePath;
	private readonly JsonLReaderOptions _options;
	private readonly ILogger _logger;

	private FileStream? _fileStream;
	private StreamReader? _streamReader;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

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

		// We need to read the first line without "consuming" it if it's a file,
		// but for STDIN we can't easily peek a whole line and seek back.
		// However, IStreamReader.ReadBatchesAsync will just continue from where we are.
		// So we'll read the first line, infer, and then in ReadBatchesAsync we'll handle the first line if it's already read.

		_firstLine = await _streamReader.ReadLineAsync(ct);
		if (string.IsNullOrEmpty(_firstLine))
		{
			Columns = Array.Empty<PipeColumnInfo>();
			return;
		}

		try
		{
			using var doc = JsonDocument.Parse(_firstLine);
			var root = doc.RootElement;

			if (root.ValueKind != JsonValueKind.Object)
			{
				throw new InvalidOperationException("First line of JsonL is not a JSON object.");
			}

			var columns = new List<PipeColumnInfo>();
			foreach (var property in root.EnumerateObject())
			{
				var type = property.Value.ValueKind switch
				{
					JsonValueKind.Number => typeof(double),
					JsonValueKind.True or JsonValueKind.False => typeof(bool),
					JsonValueKind.Null => typeof(object),
					_ => typeof(string)
				};
				columns.Add(new PipeColumnInfo(property.Name, type, true));
			}

			Columns = columns;
		}
		catch (JsonException ex)
		{
			throw new InvalidOperationException($"Failed to parse first line of JsonL as JSON: {ex.Message}", ex);
		}
	}

	private string? _firstLine;

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
				row[i] = prop.ValueKind switch
				{
					JsonValueKind.String => prop.GetString(),
					JsonValueKind.Number => prop.GetDouble(),
					JsonValueKind.True => true,
					JsonValueKind.False => false,
					JsonValueKind.Null => null,
					_ => prop.GetRawText()
				};
			}
			else
			{
				row[i] = null;
			}
		}

		return row;
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
