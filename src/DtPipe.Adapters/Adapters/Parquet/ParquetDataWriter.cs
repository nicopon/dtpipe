using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Adapters.Shared.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetField = Parquet.Schema.Field;
using System.IO;

namespace DtPipe.Adapters.Parquet;

/// <summary>
/// Writes data to Parquet format with row group streaming.
/// Supports safe dry-run inspection.
/// </summary>
public sealed class ParquetDataWriter(string outputPath) : IColumnarDataWriter, IRequiresOptions<ParquetWriterOptions>, ISchemaInspector, IColumnTypeGate
{
    private string _outputPath = outputPath;
    private Stream? _outputStream;

    private ParquetSchema? _schema;
    private ParquetWriter? _writer;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private ParquetField[]? _dataFields;
    private bool _createdFile;
    private bool _firstBatchChecked;

    public bool RequiresTargetInspection => false;

    public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
    {
        if (_outputPath == "-")
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }

        if (string.IsNullOrEmpty(_outputPath))
        {
            throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

        if (!File.Exists(_outputPath))
        {
            return new TargetSchemaInfo([], false, null, null, null);
        }

        try
        {
            using var fileStream = new FileStream(_outputPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            await using var reader = await ParquetReader.CreateAsync(fileStream, cancellationToken: ct);

            var schema = reader.Schema;
            var columns = new List<TargetColumnInfo>();

            foreach (var field in schema.DataFields)
            {
                Type clrType = ParquetStreamReader.NormalizeClrType(field.ClrNullableIfHasNullsType);

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
            for (int i = 0; i < reader.RowGroupCount; i++)
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

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        if (_outputPath == "-")
        {
            if (!Console.IsOutputRedirected)
            {
                throw new InvalidOperationException("Refusing to write binary Parquet data to the console. Redirect output with > or use a file path.");
            }
            _outputStream = Console.OpenStandardOutput();
        }
        else
        {
            // T64/T66: If it's a directory, append export.parquet
            if (Directory.Exists(_outputPath) || _outputPath.EndsWith("/") || _outputPath.EndsWith("\\"))
            {
                _outputPath = Path.Combine(_outputPath, "export.parquet");
            }
            else if (!Path.HasExtension(_outputPath))
            {
                // File without extension
                _outputPath += ".parquet";
            }

            var directory = Path.GetDirectoryName(_outputPath);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory); // no-op if already exists

            _outputStream = new FileStream(_outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            _createdFile = true;
        }

        _columns = columns;
        _dataFields = BuildDataFields(columns);
        _schema = new ParquetSchema(_dataFields);
        _writer = await ParquetWriter.CreateAsync(_schema, _outputStream, cancellationToken: ct);
        // _writer.CompressionMethod = CompressionMethod.Snappy;
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_writer is null || _dataFields is null)
            throw new InvalidOperationException("Call InitializeAsync first.");

        using (batch)
        {
            // The engine asked before InitializeAsync on the schema it computed; the first batch is
            // what the format receives, so it is asked again, and a refusal leaves no file behind.
            if (!_firstBatchChecked)
            {
                _firstBatchChecked = true;
                var refusals = RefuseColumns(FormatSchema(batch.Schema));
                if (refusals.Count > 0)
                {
                    await DiscardFileAsync();
                    throw new ColumnTypeRefusedException("parquet", refusals,
                        "Parquet cannot write: " + string.Join(", ", refusals.Select(r => $"'{r.Received.Name}' ({r.Received.DataType.Name})")));
                }
            }

            // Parquet carries a LIST natively, so a column declared as one keeps its shape. A
            // composite the schema declared as text — a struct, or a list whose source gave no
            // element type — becomes JSON rather than failing the write: the declared field is
            // what decides, and it was fixed at InitializeAsync.
            using var payload = CompositeCellJson.RenderComposites(
                batch, (index, _) => _dataFields[index] is ListField);

            using var rowGroup = _writer.CreateRowGroup();

            for (int i = 0; i < payload.Batch.ColumnCount; i++)
            {
                var arrowArray = payload.Batch.Column(i);
                await ArrowToParquetConverter.WriteColumnAsync(rowGroup, arrowArray, payload.Batch.Schema.GetFieldByIndex(i), _dataFields[i], ct);
            }
        }
    }

    /// <summary>
    /// Parquet carries every integer width, signed or not, a date, a time of day and an instant under
    /// their own logical type. It has no duration type, so a duration column is refused. The writer has no
    /// form for a column of nulls, a struct, a map, a fixed-size list or a dictionary, and writes a LIST
    /// only of the element types <see cref="ArrowToParquetConverter"/> gives definition levels to.
    /// </summary>
    public IReadOnlyList<ColumnTypeRefusal> RefuseColumns(Schema schema)
        => ColumnTypeRefusals.Collect(schema, f => f.DataType switch
        {
            DurationType => [Int64Type.Default, StringType.Default],
            NullType or StructType or MapType or FixedSizeListType or DictionaryType => [StringType.Default],
            ListType { ValueDataType: not (Int16Type or Int32Type or Int64Type or FloatType or DoubleType or BooleanType or Decimal128Type) }
                => [StringType.Default],
            _ => ColumnTypeRefusals.WideInteger(f)
        });

    // What the format receives, for the first batch's check: a column the writer declared as text, because
    // its source named no shape for it, is rendered as JSON whatever its Arrow type.
    private Schema FormatSchema(Schema batch)
        => new(batch.FieldsList.Where((_, i) => _columns![i].ClrType != typeof(object)), null);

    private async ValueTask DiscardFileAsync()
    {
        if (_writer != null) { await _writer.DisposeAsync(); _writer = null; }
        if (_outputStream != null) { await _outputStream.DisposeAsync(); _outputStream = null; }
        if (_createdFile && File.Exists(_outputPath)) File.Delete(_outputPath);
    }

    private static ParquetField[] BuildDataFields(IReadOnlyList<PipeColumnInfo> columns)
    {
        var fields = new ParquetField[columns.Count];

        for (var i = 0; i < columns.Count; i++)
        {
            fields[i] = MapToField(columns[i]);
        }

        return fields;
    }

    /// <summary>
    /// Parquet field for a column. A collection becomes a real Parquet LIST rather than text:
    /// the format carries lists natively, and writing them as strings would misreport the schema
    /// to every reader. "list"/"element" are the names the Parquet spec uses for the container and
    /// its items — the same ones DuckDB emits.
    /// </summary>
    private static ParquetField MapToField(PipeColumnInfo col)
    {
        var baseType = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;

        if (baseType != typeof(byte[]) && baseType != typeof(string) &&
            TryGetElementType(baseType, out var elementType))
        {
            var item = MapToDataField(new PipeColumnInfo("element", elementType, true, Precision: col.Precision, Scale: col.Scale));
            return new ListField(col.Name, item, "list");
        }

        return MapToDataField(col);
    }

    private static bool TryGetElementType(Type type, out Type elementType)
    {
        elementType = typeof(object);
        if (type.IsArray) { elementType = type.GetElementType()!; return true; }
        var enumerable = type.GetInterfaces().FirstOrDefault(
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (enumerable is null) return false;
        elementType = enumerable.GetGenericArguments()[0];
        return true;
    }

    private static DataField MapToDataField(PipeColumnInfo col)
    {
        var baseType = Nullable.GetUnderlyingType(col.ClrType) ?? col.ClrType;

        return baseType switch
        {
            Type t when t == typeof(bool) => new DataField<bool?>(col.Name),
            Type t when t == typeof(byte) => new DataField<byte?>(col.Name),
            Type t when t == typeof(sbyte) => new DataField<sbyte?>(col.Name),
            Type t when t == typeof(short) => new DataField<short?>(col.Name),
            Type t when t == typeof(ushort) => new DataField<ushort?>(col.Name),
            Type t when t == typeof(uint) => new DataField<uint?>(col.Name),
            Type t when t == typeof(ulong) => new DataField<ulong?>(col.Name),
            Type t when t == typeof(int) => new DataField<int?>(col.Name),
            Type t when t == typeof(long) => new DataField<long?>(col.Name),
            Type t when t == typeof(float) => new DataField<float?>(col.Name),
            Type t when t == typeof(double) => new DataField<double?>(col.Name),
            // A decimal the source declared a width for keeps it: DataField<decimal?> would write
            // Parquet.Net's default (38, 18), turning a DECIMAL(10,2) column into one the target
            // no longer describes.
            Type t when t == typeof(decimal) => col is { Precision: int p, Scale: int s } && p >= 1 && p <= 38 && s >= 0 && s <= p
                ? new DecimalDataField(col.Name, p, s, isNullable: true)
                : new DataField<decimal?>(col.Name),
            Type t when t == typeof(DateTime) => new DataField<DateTime?>(col.Name),
            // An instant: a UTC timestamp, which Parquet marks isAdjustedToUTC.
            Type t when t == typeof(DateTimeOffset) => new DateTimeDataField(col.Name, DateTimeFormat.Timestamp, isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true),
            Type t when t == typeof(DateOnly) => new DataField<DateOnly?>(col.Name),
            // A time of day, in microseconds since midnight: TIME(MICROS), not adjusted to UTC, which
            // Parquet.Net otherwise marks it and a reader then takes for a time with a zone.
            Type t when t == typeof(TimeOnly) => new TimeDataField(col.Name, TimeUnitPrecision.Micros, isNullable: true) { IsAdjustedToUtc = false },
            Type t when t == typeof(Guid) => new DataField<Guid?>(col.Name),
            Type t when t == typeof(byte[]) => new DataField<byte[]>(col.Name),
            _ => new DataField<string?>(col.Name)
        };
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        // Parquet.Net handles flushing on dispose
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        throw new NotSupportedException("Executing raw commands is not supported for Parquet targets.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_writer != null)
        {
            await _writer.DisposeAsync();
        }

        if (_outputStream != null)
        {
            await _outputStream.DisposeAsync();
        }
    }
}
