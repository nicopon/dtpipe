using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using QueryDump.Core;

namespace QueryDump.Providers.Csv;

public class CsvStreamReader : IStreamReader
{
    private readonly string _filePath;
    private readonly CsvReaderOptions _options;

    private StreamReader? _streamReader;
    private CsvReader? _csvReader;
    private string[]? _headers;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    public CsvStreamReader(string filePath, CsvReaderOptions options)
    {
        _filePath = filePath;
        _options = options;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        var encoding = Encoding.GetEncoding(_options.Encoding);
        _streamReader = new StreamReader(_filePath, encoding);

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = _options.Delimiter,
            HasHeaderRecord = _options.HasHeader,
            MissingFieldFound = null,
            BadDataFound = null
        };

        _csvReader = new CsvReader(_streamReader, config);

        if (_options.HasHeader)
        {
            await _csvReader.ReadAsync();
            _csvReader.ReadHeader();
            _headers = _csvReader.HeaderRecord ?? Array.Empty<string>();
        }
        else
        {
            // Read first row to determine column count
            if (await _csvReader.ReadAsync())
            {
                var fieldCount = _csvReader.Parser.Count;
                _headers = Enumerable.Range(0, fieldCount).Select(i => $"Column{i}").ToArray();
                // Note: First data row will be yielded in ReadBatchesAsync
            }
            else
            {
                _headers = Array.Empty<string>();
            }
        }

        // All CSV columns are strings
        Columns = _headers.Select(h => new ColumnInfo(h, typeof(string), true)).ToList();
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_csvReader is null || _headers is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var batch = new object?[batchSize][];
        var index = 0;

        // For no-header mode, first row is already read in OpenAsync
        if (!_options.HasHeader && _csvReader.Parser.Row == 1)
        {
            var row = new object?[_headers.Length];
            for (int i = 0; i < _headers.Length; i++)
            {
                row[i] = _csvReader.GetField(i);
            }
            batch[index++] = row;
        }

        while (await _csvReader.ReadAsync())
        {
            ct.ThrowIfCancellationRequested();

            var row = new object?[_headers.Length];
            for (int i = 0; i < _headers.Length; i++)
            {
                row[i] = _csvReader.GetField(i);
            }
            
            batch[index++] = row;
            
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

    public ValueTask DisposeAsync()
    {
        _csvReader?.Dispose();
        _csvReader = null;
        _streamReader?.Dispose();
        _streamReader = null;
        return ValueTask.CompletedTask;
    }
}
