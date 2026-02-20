using System.Text.Json;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.IO;

namespace DtPipe.Adapters.JsonL;

public sealed class JsonLDataWriter : IDataWriter, IRequiresOptions<JsonLWriterOptions>, ISchemaInspector
{
	private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();

	private readonly string _outputPath;
	private readonly JsonLWriterOptions _options;

	private Stream? _outputStream;
	private RecyclableMemoryStream? _memoryStream;
	private IReadOnlyList<PipeColumnInfo>? _columns;
	private int _rowsInBuffer;
	private const int FlushThreshold = 1000;

	public JsonLDataWriter(string outputPath) : this(outputPath, new JsonLWriterOptions())
	{
	}

	public JsonLDataWriter(string outputPath, JsonLWriterOptions options)
	{
		_outputPath = outputPath;
		_options = options;
	}

	public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_outputPath) || _outputPath == "-")
		{
			return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
		}

		if (!File.Exists(_outputPath))
		{
			return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
		}

		// For JsonL, we could try to infer from existing file if appending,
		// but usually we just overwrite or recreate.
		return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, null, new FileInfo(_outputPath).Length, null));
	}

	public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_outputPath) || _outputPath == "-")
		{
			_outputStream = Console.OpenStandardOutput();
		}
		else
		{
			_outputStream = new FileStream(_outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
		}

		_memoryStream = (RecyclableMemoryStream)MemoryStreamManager.GetStream("JsonLDataWriter");
		_columns = columns;

		return ValueTask.CompletedTask;
	}

	public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
	{
		if (_columns is null || _outputStream is null || _memoryStream is null)
			throw new InvalidOperationException("Call InitializeAsync first.");

		var jsonOptions = new JsonWriterOptions
		{
			Indented = _options.Indented,
			SkipValidation = true
		};

		foreach (var row in rows)
		{
			using (var writer = new Utf8JsonWriter((Stream)_memoryStream, jsonOptions))
			{
				writer.WriteStartObject();
				for (int i = 0; i < row.Length; i++)
				{
					var col = _columns[i];
					var val = row[i];

					writer.WritePropertyName(col.Name);
					WriteValue(writer, val);
				}
				writer.WriteEndObject();
				writer.Flush();
			}
			_memoryStream.WriteByte((byte)'\n');
			_rowsInBuffer++;
		}

		if (_rowsInBuffer >= FlushThreshold)
		{
			await FlushBufferToFileAsync(ct);
		}
	}

	private void WriteValue(Utf8JsonWriter writer, object? val)
	{
		if (val == null)
		{
			writer.WriteNullValue();
			return;
		}

		switch (val)
		{
			case string s: writer.WriteStringValue(s); break;
			case bool b: writer.WriteBooleanValue(b); break;
			case int i: writer.WriteNumberValue(i); break;
			case long l: writer.WriteNumberValue(l); break;
			case double d: writer.WriteNumberValue(d); break;
			case decimal dec: writer.WriteNumberValue(dec); break;
			case float f: writer.WriteNumberValue(f); break;
			case DateTime dt: writer.WriteStringValue(dt.ToString("O")); break;
			case DateTimeOffset dto: writer.WriteStringValue(dto.ToString("O")); break;
			default: writer.WriteStringValue(val.ToString()); break;
		}
	}

	private async ValueTask FlushBufferToFileAsync(CancellationToken ct)
	{
		if (_memoryStream is null || _outputStream is null) return;

		_memoryStream.Position = 0;
		await _memoryStream.CopyToAsync(_outputStream, ct);
		await _outputStream.FlushAsync(ct);

		_memoryStream.SetLength(0);
		_rowsInBuffer = 0;
	}

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		if (_rowsInBuffer > 0)
		{
			await FlushBufferToFileAsync(ct);
		}

		if (_outputStream != null)
		{
			await _outputStream.FlushAsync(ct);
		}
	}

	public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		throw new NotSupportedException("Executing raw commands is not supported for JsonL targets.");
	}

	public async ValueTask DisposeAsync()
	{
		await CompleteAsync();
		if (_memoryStream != null) await _memoryStream.DisposeAsync();
		if (_outputStream != null) await _outputStream.DisposeAsync();
	}
}
