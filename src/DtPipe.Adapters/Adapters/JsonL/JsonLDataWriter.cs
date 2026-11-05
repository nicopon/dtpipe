using System.Text.Json;
using System.Collections;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.IO;

namespace DtPipe.Adapters.JsonL;

public sealed class JsonLDataWriter : IRowDataWriter, IRequiresOptions<JsonLWriterOptions>, ISchemaInspector
{
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new();

    private readonly string _outputPath;
    private readonly JsonLWriterOptions _options;

    private Stream? _outputStream;
    private RecyclableMemoryStream? _memoryStream;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private int _rowsInBuffer;
    private long _recordsWritten;
    private const int FlushThreshold = 1000;

    public JsonLDataWriter(string outputPath) : this(outputPath, new JsonLWriterOptions())
    {
    }

    public JsonLDataWriter(string outputPath, JsonLWriterOptions options)
    {
        _outputPath = outputPath;
        _options = options;
    }

    public bool RequiresTargetInspection => false;

    public Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        if (_outputPath == "-")
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
        }

        if (string.IsNullOrEmpty(_outputPath))
        {
            throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

        if (!File.Exists(_outputPath))
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], false, null, null, null));
        }

        try
        {
            using var fs = File.OpenRead(_outputPath);
            using var reader = new StreamReader(fs);
            var firstLine = reader.ReadLine();

            if (string.IsNullOrWhiteSpace(firstLine))
            {
                return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, 0, fs.Length, null));
            }

            // Try to parse the first line as a JSON object to get keys
            var json = JsonDocument.Parse(firstLine);
            var columns = new List<TargetColumnInfo>();

            if (json.RootElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in json.RootElement.EnumerateObject())
                {
                    // Infer type from the first row's value
                    var clrType = prop.Value.ValueKind switch
                    {
                        JsonValueKind.Number => typeof(double),
                        JsonValueKind.True => typeof(bool),
                        JsonValueKind.False => typeof(bool),
                        JsonValueKind.String => typeof(string),
                        _ => typeof(string)
                    };

                    columns.Add(new TargetColumnInfo(
                        prop.Name,
                        prop.Value.ValueKind.ToString().ToUpperInvariant(),
                        clrType,
                        true,
                        false, false, null, null, null));
                }
            }

            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo(columns, true, null, fs.Length, null));
        }
        catch
        {
            return Task.FromResult<TargetSchemaInfo?>(new TargetSchemaInfo([], true, null, new FileInfo(_outputPath).Length, null));
        }
    }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        if (_outputPath == "-")
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
            _recordsWritten++;
            using (var writer = new Utf8JsonWriter((Stream)_memoryStream, jsonOptions))
            {
                writer.WriteStartObject();
                for (int i = 0; i < row.Length; i++)
                {
                    var col = _columns[i];
                    var val = row[i];

                    writer.WritePropertyName(col.Name);
                    WriteValue(writer, val, col.Name);
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

    /// <param name="writer">The JSON being written.</param>
    /// <param name="val">The value, or an element of one.</param>
    /// <param name="column">The column the value belongs to, for a refusal.</param>
    private void WriteValue(Utf8JsonWriter writer, object? val, string column)
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
            case sbyte i8: writer.WriteNumberValue(i8); break;
            case short i16: writer.WriteNumberValue(i16); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case byte u8: writer.WriteNumberValue(u8); break;
            case ushort u16: writer.WriteNumberValue(u16); break;
            case uint u32: writer.WriteNumberValue(u32); break;
            case ulong u64: writer.WriteNumberValue(u64); break;
            // A JSON number has no width; the writer has no overload past 64 bits, so the digits go raw.
            case Int128 i128: writer.WriteRawValue(i128.ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
            case UInt128 u128: writer.WriteRawValue(u128.ToString(System.Globalization.CultureInfo.InvariantCulture)); break;
            case double d:
                if (!double.IsFinite(d)) RefuseNonFinite(column, "double", d.ToString(System.Globalization.CultureInfo.InvariantCulture));
                writer.WriteNumberValue(d);
                break;
            case decimal dec: writer.WriteNumberValue(dec); break;
            case float f:
                if (!float.IsFinite(f)) RefuseNonFinite(column, "float", f.ToString(System.Globalization.CultureInfo.InvariantCulture));
                writer.WriteNumberValue(f);
                break;
            case DateTime dt: writer.WriteStringValue(dt.ToString("O")); break;
            case DateTimeOffset dto: writer.WriteStringValue(dto.ToString("O")); break;
            case DateOnly date: writer.WriteStringValue(date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)); break;
            case TimeOnly time: writer.WriteStringValue(time.ToString("HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture)); break;
            // Before IEnumerable: bytes are one value, written as the base64 text System.Text.Json reads back.
            case byte[] bytes: writer.WriteBase64StringValue(bytes); break;
            case IDictionary dict:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in dict)
                {
                    writer.WritePropertyName(entry.Key.ToString() ?? string.Empty);
                    WriteValue(writer, entry.Value, column);
                }
                writer.WriteEndObject();
                break;
            case IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var item in enumerable)
                {
                    WriteValue(writer, item, column);
                }
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStringValue(val.ToString());
                break;
        }
    }

    // JSON has no number for these, and no spelling a reader would read back as one.
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private void RefuseNonFinite(string column, string type, string spelled)
        => throw new ColumnValueRefusedException(
            $"Record {_recordsWritten}", column, type, $"holds {spelled}, which JSON has no number for",
            "Filter the value out, or read the column as text: declare it 'string', or cast it in the source query.");

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
