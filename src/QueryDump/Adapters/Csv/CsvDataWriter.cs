using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.IO;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Adapters.Csv;

namespace QueryDump.Adapters.Csv;

/// <summary>
/// Writes data to CSV format with streaming.
/// Uses RecyclableMemoryStream for reduced GC pressure.
/// Optimized for DuckDB compatibility.
/// </summary>
public sealed class CsvDataWriter : IDataWriter, IRequiresOptions<CsvOptions>
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
    private readonly CsvOptions _options;
    private readonly FileStream _fileStream;
    private readonly RecyclableMemoryStream _memoryStream;
    private readonly StreamWriter _streamWriter;
    private readonly CsvWriter _csvWriter;
    private IReadOnlyList<ColumnInfo>? _columns;
    private int _rowsInBuffer;
    private const int FlushThreshold = 1000; // Flush every N rows

    public long BytesWritten => _fileStream.Position;

    public CsvDataWriter(string outputPath) : this(outputPath, new CsvOptions())
    {
    }

    public CsvDataWriter(string outputPath, CsvOptions options)
    {
        _outputPath = outputPath;
        _options = options;
        _fileStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 
            bufferSize: 65536, useAsync: true);
        
        // Use RecyclableMemoryStream as intermediate buffer
        _memoryStream = (RecyclableMemoryStream)MemoryStreamManager.GetStream("CsvDataWriter");
        _streamWriter = new StreamWriter(_memoryStream, Encoding.UTF8, bufferSize: 65536, leaveOpen: true);
        
        // DuckDB-compatible CSV configuration
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = options.Header,
            Delimiter = options.Separator.ToString(),
            Quote = options.Quote,
            ShouldQuote = args => ShouldQuoteField(args.Field, options)
        };
        _csvWriter = new CsvWriter(_streamWriter, config);
    }

    public ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
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
        if (_columns is null)
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
        await _csvWriter.FlushAsync();
        await _streamWriter.FlushAsync(ct);
        
        // Copy from memory stream to file
        _memoryStream.Position = 0;
        await _memoryStream.CopyToAsync(_fileStream, ct);
        await _fileStream.FlushAsync(ct);
        
        // Reset memory stream for reuse
        _memoryStream.SetLength(0);
        _rowsInBuffer = 0;
    }

    private string? FormatValue(object? value, ColumnInfo column)
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

    private static bool ShouldQuoteField(string? field, CsvOptions options)
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
        // Flush any remaining data
        if (_rowsInBuffer > 0)
        {
            await FlushBufferToFileAsync(ct);
        }
        await _fileStream.FlushAsync(ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _csvWriter.DisposeAsync();
        await _streamWriter.DisposeAsync();
        await _memoryStream.DisposeAsync();
        await _fileStream.DisposeAsync();
    }
}
