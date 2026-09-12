using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Reflection;
using DtPipe.Adapters.Common;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

/// <summary>
/// Columnar stream reader for PostgreSQL. Produces Apache Arrow RecordBatches directly
/// from PostgreSQL binary COPY TO STDOUT stream (no boxing, direct binary copying).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed partial class PostgreSqlReader : IColumnarStreamReader, IBatchSizeConfigurable
{
    public int BatchSize { get; set; } = DtPipe.Core.Models.PipelineOptions.DefaultBatchSize;
    public long MaxBatchBytes { get; set; }

    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _timeout;
    private NpgsqlConnection? _connection;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public Schema? Schema { get; private set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    public PostgreSqlReader(string connectionString, string query, int timeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connectionString = connectionString;
        _query = query;
        _timeout = timeout;
    }

    private static void ValidateQueryIsSafeSelect(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();
        if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "VALUES")
            throw new InvalidOperationException($"Only SELECT queries are allowed. Detected: {firstWord}");
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Enforce readonly session
        var csb = new NpgsqlConnectionStringBuilder(_connectionString);
        if (!string.IsNullOrEmpty(csb.Options))
            csb.Options += " -c default_transaction_read_only=on";
        else
            csb.Options = "-c default_transaction_read_only=on";

        _connection = new NpgsqlConnection(csb.ConnectionString);
        await _connection.OpenAsync(ct);

        // Introspect the query schema using SchemaOnly first before launching binary copy
        using (var schemaCommand = new NpgsqlCommand(_query, _connection))
        {
            if (_timeout > 0) schemaCommand.CommandTimeout = _timeout;
            using var schemaReader = await schemaCommand.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct);
            var dbColumns = await schemaReader.GetColumnSchemaAsync(ct);

            // Build PipeColumnInfo from DB schema (CLR types) — authoritative for DtPipe pipeline
            Columns = dbColumns.Select(c => new PipeColumnInfo(
                c.ColumnName,
                ResolveClrType(c),
                c.AllowDBNull ?? true,
                IsCaseSensitive: c.ColumnName != c.ColumnName.ToLowerInvariant(),
                Precision: (int?)c.NumericPrecision,
                Scale: (int?)c.NumericScale
            )).ToList();
        }

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);
    }

    /// <summary>
    /// CLR type for a column. Npgsql reports every array column as the abstract
    /// <see cref="System.Array"/>, which carries no element type, so the schema factory has nothing
    /// to map. The element is recovered from the provider type name ("integer[]" → int[]) using the
    /// same table the writer maps with. Resolving it here rather than in the shared mapper keeps
    /// Postgres' spelling in the Postgres adapter.
    /// </summary>
    private static Type ResolveClrType(System.Data.Common.DbColumn column)
    {
        var declared = column.DataType ?? typeof(object);
        if (declared != typeof(System.Array)) return declared;

        var name = column.DataTypeName;
        if (string.IsNullOrEmpty(name) || !name.EndsWith("[]", StringComparison.Ordinal))
            return typeof(string[]);

        var element = PostgreSqlTypeConverter.Instance.MapFromProviderType(name[..^2]);
        return element.MakeArrayType();
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Columns == null || Schema == null || _connection == null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var copySql = $"COPY ({_query}) TO STDOUT (FORMAT BINARY)";
        await using var exporter = await _connection.BeginBinaryExportAsync(copySql, ct);

        int targetBatchSize = BatchSize;
        long maxBatchBytes = MaxBatchBytes;

        // Fixed part of a row's size, from the Arrow schema; variable-width columns add their
        // payload per value through the AddBytes callback below (see the dual bound in the inner loop).
        int fixedBytesPerRow = 0;
        for (int i = 0; i < Columns.Count; i++)
            fixedBytesPerRow += Apache.Arrow.Ado.ArrowByteSize.FixedWidth(Schema.FieldsList[i].DataType);

        while (true)
        {
            long batchBytes = 0;
            void AddBytes(long n) => batchBytes += n;

            var readers = new (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build)[Columns.Count];
            for (int i = 0; i < Columns.Count; i++)
            {
                var npgsqlType = PostgreSqlTypeConverter.Instance.MapToNpgsqlDbType(Columns[i].ClrType);
                readers[i] = BuildBinaryColumnReader(npgsqlType, Schema.FieldsList[i].DataType, AddBytes);
            }

            int rowsInBuffer = 0;
            bool streamEnded = false;
            while (rowsInBuffer < targetBatchSize)
            {
                if (await exporter.StartRowAsync(ct) == -1)
                {
                    streamEnded = true;
                    break;
                }

                for (int i = 0; i < Columns.Count; i++)
                {
                    readers[i].Read(exporter);
                }
                rowsInBuffer++;
                batchBytes += fixedBytesPerRow;

                // Dual bound: flush on row count or on the soft byte cap, whichever hits first.
                if (maxBatchBytes > 0 && batchBytes >= maxBatchBytes)
                    break;
            }

            if (rowsInBuffer == 0)
                break;

            var arrays = new IArrowArray[Columns.Count];
            for (int i = 0; i < Columns.Count; i++)
            {
                arrays[i] = readers[i].Build();
            }

            yield return new RecordBatch(Schema, arrays, rowsInBuffer);

            if (streamEnded)
                break;
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
            {
                var rowCount = recordBatch.Length;
                var colCount = recordBatch.Schema.FieldsList.Count;
                var rows = new object?[rowCount][];

                for (int i = 0; i < rowCount; i++)
                {
                    var row = new object?[colCount];
                    for (int j = 0; j < colCount; j++)
                    {
                        var array = recordBatch.Column(j);
                        row[j] = ArrowTypeMapper.GetValueForField(array, recordBatch.Schema.FieldsList[j], i);
                    }
                    rows[i] = row;
                }

                yield return new ReadOnlyMemory<object?[]>(rows);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection != null) await _connection.DisposeAsync();
    }

    #region Binary Column Readers

    private static (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build) BuildBinaryColumnReader(NpgsqlTypes.NpgsqlDbType npgsqlType, IArrowType arrowType, Action<long> addBytes)
    {
        // The array built here must match the field the schema declares. The fallback case below
        // builds a StringArray from Read<object>().ToString(), so without this an array column
        // declared as a list silently produced a column of whitespace and exit 0.
        if (arrowType is ListType listType)
            return BuildBinaryListReader(listType);

        return npgsqlType switch
        {
            NpgsqlTypes.NpgsqlDbType.Boolean => BuildTyped(
                new BooleanArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<bool>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Smallint => BuildTyped(
                new Int16Array.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<short>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Integer => BuildTyped(
                new Int32Array.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<int>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Bigint => BuildTyped(
                new Int64Array.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<long>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Real => BuildTyped(
                new FloatArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<float>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Double => BuildTyped(
                new DoubleArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<double>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Numeric => BuildTyped(
                new Decimal128Array.Builder((Decimal128Type)arrowType),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<decimal>()); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Text or NpgsqlTypes.NpgsqlDbType.Varchar
                or NpgsqlTypes.NpgsqlDbType.Char or NpgsqlTypes.NpgsqlDbType.Name => BuildTyped(
                new StringArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else { var s = e.Read<string>(npgsqlType); b.Append(s); addBytes(System.Text.Encoding.UTF8.GetByteCount(s)); } },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Bytea => BuildTyped(
                new BinaryArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else { var v = e.Read<byte[]>(); b.Append((System.Collections.Generic.IEnumerable<byte>)v); addBytes(v.Length); } },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Uuid => BuildTyped(
                new FixedSizeBinaryArrayBuilder(16),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(ArrowTypeMapper.ToArrowUuidBytes(e.Read<Guid>())); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Date => arrowType is Date64Type 
                ? BuildTyped(new Date64Array.Builder(), (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<DateOnly>().ToDateTime(TimeOnly.MinValue)); }, b => b.Build())
                : BuildTyped(new Date32Array.Builder(), (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<DateOnly>().ToDateTime(TimeOnly.MinValue)); }, b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Timestamp => BuildTyped(
                new TimestampArray.Builder((TimestampType)arrowType),
                // Npgsql's binary export returns Kind=Unspecified; a raw Append would resolve it
                // against the local zone.
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(Apache.Arrow.Serialization.Mapping.TemporalNormalization.ToOffset(e.Read<DateTime>())); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.TimestampTz => BuildTyped(
                new TimestampArray.Builder((TimestampType)arrowType),
                // timestamptz too: the binary export returns the UTC wall clock with no Kind, so
                // it needs the same normalization as a zone-less column.
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(Apache.Arrow.Serialization.Mapping.TemporalNormalization.ToOffset(e.Read<DateTime>())); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Interval => BuildTyped(
                new DurationArray.Builder((DurationType)arrowType),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<TimeSpan>().Ticks / TimeSpan.TicksPerMillisecond); },
                b => b.Build()),

            _ => BuildTyped(
                new StringArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else { var s = e.Read<object>(npgsqlType)?.ToString(); b.Append(s); addBytes(s?.Length ?? 0); } },
                b => b.Build())
        };
    }

    /// <summary>
    /// Reads a Postgres array column from the binary COPY stream into an Arrow list. Npgsql
    /// materializes the array itself (int[], string[]…), so the element type never has to be known
    /// at compile time here; the shared list builder walks it. Element conversion stays with
    /// <see cref="ArrowTypeMapper"/>, which owns the CLR↔Arrow map.
    /// </summary>
    private static (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build) BuildBinaryListReader(ListType listType)
    {
        var builder = ArrowTypeMapper.CreateBuilder(listType);

        // Npgsql needs the concrete array type to decode the binary payload; Read<object>() yields
        // nothing usable. Nullable element types throughout, because a Postgres array may hold
        // NULLs and reading those into int[] throws. Chosen once per column, not per row.
        Func<NpgsqlBinaryExporter, object?> readArray = listType.ValueDataType switch
        {
            BooleanType => e => e.Read<bool?[]>(),
            Int16Type => e => e.Read<short?[]>(),
            Int32Type => e => e.Read<int?[]>(),
            Int64Type => e => e.Read<long?[]>(),
            FloatType => e => e.Read<float?[]>(),
            DoubleType => e => e.Read<double?[]>(),
            Decimal128Type => e => e.Read<decimal?[]>(),
            StringType => e => e.Read<string?[]>(),
            TimestampType => e => e.Read<DateTime?[]>(),
            Date32Type => e => e.Read<DateOnly?[]>(),
            _ => throw new NotSupportedException(
                $"Postgres array of {listType.ValueDataType.Name} is not supported. " +
                "Cast the column in your query, for example with array_to_json(col)::text.")
        };

        var append = ArrowTypeMapper.ResolveAppender(builder);

        return (
            e =>
            {
                if (e.IsNull) { e.Skip(); append(null); }
                else append(readArray(e));
            },
            () => ArrowTypeMapper.BuildArray(builder));
    }

    private static (Action<NpgsqlBinaryExporter>, Func<IArrowArray>) BuildTyped<TBuilder>(
        TBuilder builder, Action<NpgsqlBinaryExporter, TBuilder> reader, Func<TBuilder, IArrowArray> buildFunc)
    {
        return (exporter => reader(exporter, builder), () => buildFunc(builder));
    }

    #endregion
}
