using Parquet;
using Parquet.Schema;
using QueryDump.Core;

namespace QueryDump.Providers.Parquet;

public class ParquetStreamReader : IStreamReader
{
    private readonly string _filePath;

    private ParquetReader? _reader;

    public IReadOnlyList<ColumnInfo>? Columns { get; private set; }

    public ParquetStreamReader(string filePath)
    {
        _filePath = filePath;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        _reader = await ParquetReader.CreateAsync(_filePath);

        var schema = _reader.Schema;
        var columns = new List<ColumnInfo>();

        foreach (var field in schema.Fields)
        {
            if (field is DataField dataField)
            {
                columns.Add(new ColumnInfo(dataField.Name, dataField.ClrType, dataField.IsNullable));
            }
        }

        Columns = columns;
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_reader is null || Columns is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var batch = new object?[batchSize][];
        var index = 0;

        // Read all row groups
        for (int rowGroupIndex = 0; rowGroupIndex < _reader.RowGroupCount; rowGroupIndex++)
        {
            ct.ThrowIfCancellationRequested();

            using var rowGroupReader = _reader.OpenRowGroupReader(rowGroupIndex);
            var rowCount = (int)rowGroupReader.RowCount;

            // Read all columns for this row group
            var columnData = new object?[Columns.Count][];
            for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
            {
                var dataField = _reader.Schema.DataFields[colIndex];
                var dataColumn = await rowGroupReader.ReadColumnAsync(dataField, ct);
                // Convert DataColumn.Data to array
                columnData[colIndex] = dataColumn.Data.Cast<object?>().ToArray();
            }

            // Yield rows
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
            {
                ct.ThrowIfCancellationRequested();

                var row = new object?[Columns.Count];
                for (int colIndex = 0; colIndex < Columns.Count; colIndex++)
                {
                    row[colIndex] = columnData[colIndex]?[rowIndex];
                }
                
                batch[index++] = row;
                
                if (index >= batchSize)
                {
                    yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
                    batch = new object?[batchSize][];
                    index = 0;
                }
            }
        }

        if (index > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
        }
    }

    public ValueTask DisposeAsync()
    {
        _reader?.Dispose();
        _reader = null;
        return ValueTask.CompletedTask;
    }
}
