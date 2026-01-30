using Parquet;
using Parquet.Data;
using Parquet.Schema;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Adapters.Parquet;

namespace DtPipe.Adapters.Parquet;

/// <summary>
/// Writes data to Parquet format with row group streaming.
/// Supports safe dry-run inspection.
/// </summary>
public sealed class ParquetDataWriter(string outputPath) : IDataWriter, IRequiresOptions<ParquetWriterOptions>, ISchemaInspector
{
    private readonly string _outputPath = outputPath;
    private Stream? _outputStream;

    private ParquetSchema? _schema;
    private ParquetWriter? _writer;
    private IReadOnlyList<ColumnInfo>? _columns;
    private DataField[]? _dataFields;

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(_outputPath) || _outputPath == "-")
        {
             return new TargetSchemaInfo([], false, null, null, null);
        }

        if (!File.Exists(_outputPath))
        {
             return new TargetSchemaInfo([], false, null, null, null);
        }

        try 
        {
            using var fileStream = new FileStream(_outputPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var reader = await ParquetReader.CreateAsync(fileStream, cancellationToken: ct);
            
            var schema = reader.Schema;
            var columns = new List<TargetColumnInfo>();

            foreach(var field in schema.DataFields)
            {
                Type clrType = field.ClrNullableIfHasNullsType;

                columns.Add(new TargetColumnInfo(
                    field.Name,
                    clrType.Name, 
                    clrType,
                    field.IsNullable,
                    IsPrimaryKey: false, 
                    IsUnique: false,
                    MaxLength: null,
                    Precision: null,
                    Scale: null
                ));
            }

            long rowCount = 0;
            for(int i = 0; i < reader.RowGroupCount; i++)
            {
                 using var groupReader = reader.OpenRowGroupReader(i);
                 rowCount += groupReader.RowCount;
            }

            return new TargetSchemaInfo(columns, true, rowCount, fileStream.Length, null);
        }
        catch
        {
            // If file is corrupted or unreadable
            return null;
        }
    }

    public async ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(_outputPath) || _outputPath == "-")
        {
             if (!Console.IsOutputRedirected)
             {
                 throw new InvalidOperationException("Refusing to write binary Parquet data to the console. Redirect output with > or use a file path.");
             }
             _outputStream = Console.OpenStandardOutput();
        }
        else
        {
             // Defer file creation until here to avoid overwriting during dry-run
             _outputStream = new FileStream(_outputPath, FileMode.Create, FileAccess.Write, FileShare.None,
                 bufferSize: 65536, useAsync: true);
        }

        _columns = columns;
        _dataFields = BuildDataFields(columns);
        _schema = new ParquetSchema(_dataFields);
        _writer = await ParquetWriter.CreateAsync(_schema, _outputStream, cancellationToken: ct);
        _writer.CompressionMethod = CompressionMethod.Snappy;
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_writer is null || _columns is null || _dataFields is null)
            throw new InvalidOperationException("Call InitializeAsync first.");

        if (rows.Count == 0) return;

        // Create row group for this batch
        using var rowGroup = _writer.CreateRowGroup();

        for (var colIndex = 0; colIndex < _columns.Count; colIndex++)
        {
            var column = _columns[colIndex];
            var dataField = _dataFields[colIndex];
            var dataColumn = CreateDataColumn(dataField, column, rows, colIndex);
            await rowGroup.WriteColumnAsync(dataColumn, ct);
        }
    }

    private static DataField[] BuildDataFields(IReadOnlyList<ColumnInfo> columns)
    {
        var fields = new DataField[columns.Count];

        for (var i = 0; i < columns.Count; i++)
        {
            fields[i] = MapToDataField(columns[i]);
        }

        return fields;
    }

    private static DataField MapToDataField(ColumnInfo col)
    {
        var baseType = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;

        return baseType switch
        {
            Type t when t == typeof(bool) => new DataField<bool?>(col.Name),
            Type t when t == typeof(byte) => new DataField<byte?>(col.Name),
            Type t when t == typeof(sbyte) => new DataField<sbyte?>(col.Name),
            Type t when t == typeof(short) => new DataField<short?>(col.Name),
            Type t when t == typeof(int) => new DataField<int?>(col.Name),
            Type t when t == typeof(long) => new DataField<long?>(col.Name),
            Type t when t == typeof(float) => new DataField<float?>(col.Name),
            Type t when t == typeof(double) => new DataField<double?>(col.Name),
            Type t when t == typeof(decimal) => new DataField<decimal?>(col.Name),
            Type t when t == typeof(DateTime) => new DataField<DateTime?>(col.Name),
            Type t when t == typeof(DateTimeOffset) => new DataField<DateTimeOffset?>(col.Name),
            Type t when t == typeof(TimeSpan) => new DataField<TimeSpan?>(col.Name),
            Type t when t == typeof(Guid) => new DataField<Guid?>(col.Name),
            Type t when t == typeof(byte[]) => new DataField<byte[]>(col.Name),
            _ => new DataField<string?>(col.Name)
        };
    }

    private static DataColumn CreateDataColumn(DataField dataField, ColumnInfo col, IReadOnlyList<object?[]> rows, int colIndex)
    {
        var baseType = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;

        return baseType switch
        {
            Type t when t == typeof(bool) => CreateTypedColumn<bool?>(dataField, rows, colIndex),
            Type t when t == typeof(byte) => CreateTypedColumn<byte?>(dataField, rows, colIndex),
            Type t when t == typeof(sbyte) => CreateTypedColumn<sbyte?>(dataField, rows, colIndex),
            Type t when t == typeof(short) => CreateTypedColumn<short?>(dataField, rows, colIndex),
            Type t when t == typeof(int) => CreateTypedColumn<int?>(dataField, rows, colIndex),
            Type t when t == typeof(long) => CreateTypedColumn<long?>(dataField, rows, colIndex),
            Type t when t == typeof(float) => CreateTypedColumn<float?>(dataField, rows, colIndex),
            Type t when t == typeof(double) => CreateTypedColumn<double?>(dataField, rows, colIndex),
            Type t when t == typeof(decimal) => CreateTypedColumn<decimal?>(dataField, rows, colIndex),
            Type t when t == typeof(DateTime) => CreateTypedColumn<DateTime?>(dataField, rows, colIndex),
            Type t when t == typeof(DateTimeOffset) => CreateTypedColumn<DateTimeOffset?>(dataField, rows, colIndex),
            Type t when t == typeof(TimeSpan) => CreateTypedColumn<TimeSpan?>(dataField, rows, colIndex),
            Type t when t == typeof(Guid) => CreateTypedColumn<Guid?>(dataField, rows, colIndex),
            Type t when t == typeof(byte[]) => CreateByteArrayColumn(dataField, rows, colIndex),
            _ => CreateStringColumn(dataField, rows, colIndex)
        };
    }

    private static DataColumn CreateTypedColumn<T>(DataField dataField, IReadOnlyList<object?[]> rows, int colIndex)
    {
        var values = new T[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            values[i] = rows[i][colIndex] is T val ? val : default!;
        }
        return new DataColumn(dataField, values);
    }

    private static DataColumn CreateByteArrayColumn(DataField dataField, IReadOnlyList<object?[]> rows, int colIndex)
    {
        var values = new byte[]?[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            values[i] = rows[i][colIndex] as byte[];
        }
        return new DataColumn(dataField, values);
    }

    private static DataColumn CreateStringColumn(DataField dataField, IReadOnlyList<object?[]> rows, int colIndex)
    {
        var values = new string?[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            values[i] = rows[i][colIndex]?.ToString();
        }
        return new DataColumn(dataField, values);
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        // Parquet.Net handles flushing on dispose
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer != null)
        {
            _writer.Dispose();
        }
        
        if (_outputStream != null) 
        {
            await _outputStream.DisposeAsync();
        }
    }
}
