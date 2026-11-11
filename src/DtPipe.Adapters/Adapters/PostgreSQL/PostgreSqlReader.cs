using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
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

    public PostgreSqlReader(string connectionString, string query, int timeout = 0)
    {
        ValidateQueryIsSafeSelect(query);
        _connectionString = connectionString;
        _query = query;
        _timeout = timeout;
    }

    private static void ValidateQueryIsSafeSelect(string query)
        => SqlQueryGuard.RequireReadOnlyStatement(query, "VALUES");

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
            var dbColumns = (await schemaReader.GetColumnSchemaAsync(ct)).Cast<Npgsql.Schema.NpgsqlDbColumn>().ToList();

            // Every column is judged before the schema is built: a type the schema factory cannot
            // map would otherwise fail on its own, without the column, and hide the others.
            var plans = dbColumns.Select(Plan).ToList();
            var refusals = dbColumns.Where((_, i) => plans[i] is null).Select(Refusal).ToList();
            if (refusals.Count > 0)
                throw new ColumnReadRefusedException(PostgreSqlMetadata.ComponentName, refusals);

            Columns = dbColumns.Select((c, i) => new PipeColumnInfo(
                c.ColumnName,
                plans[i]!.Value.ClrType,
                c.AllowDBNull ?? true,
                IsCaseSensitive: c.ColumnName != c.ColumnName.ToLowerInvariant(),
                Precision: (int?)c.NumericPrecision,
                Scale: (int?)c.NumericScale
            )).ToList();

            _plans = plans.Select(p => p!.Value).ToArray();
            _sourceTypes = dbColumns.Select(c => c.DataTypeName).ToArray();
        }

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);
    }

    /// <summary>
    /// How a column is read: the CLR type the pipeline sees, the builder that reads it (a scalar's
    /// reported type) and the type the exporter decodes with (null for an untyped read).
    /// </summary>
    private readonly record struct ColumnPlan(Type ClrType, NpgsqlTypes.NpgsqlDbType Builder, NpgsqlTypes.NpgsqlDbType? DecodeAs);

    private ColumnPlan[] _plans = [];
    private string[] _sourceTypes = [];

    // The scalar types read natively, by the type Npgsql reports. Closed: a type absent here is
    // refused, never read through a fallback that turns its binary form into text.
    private static readonly Dictionary<NpgsqlTypes.NpgsqlDbType, Type> NativeScalars = new()
    {
        [NpgsqlTypes.NpgsqlDbType.Boolean] = typeof(bool),
        [NpgsqlTypes.NpgsqlDbType.Smallint] = typeof(short),
        [NpgsqlTypes.NpgsqlDbType.Integer] = typeof(int),
        [NpgsqlTypes.NpgsqlDbType.Bigint] = typeof(long),
        [NpgsqlTypes.NpgsqlDbType.Real] = typeof(float),
        [NpgsqlTypes.NpgsqlDbType.Double] = typeof(double),
        [NpgsqlTypes.NpgsqlDbType.Numeric] = typeof(decimal),
        [NpgsqlTypes.NpgsqlDbType.Bytea] = typeof(byte[]),
        [NpgsqlTypes.NpgsqlDbType.Uuid] = typeof(Guid),
        [NpgsqlTypes.NpgsqlDbType.Date] = typeof(DateOnly),
        [NpgsqlTypes.NpgsqlDbType.Timestamp] = typeof(DateTime),
        // Npgsql reports timestamptz as DateTime, which would make the column a zone-less Arrow
        // timestamp: the instant would then be written and read back as a wall clock, which the
        // server resolves against the session's zone.
        [NpgsqlTypes.NpgsqlDbType.TimestampTz] = typeof(DateTimeOffset),
        [NpgsqlTypes.NpgsqlDbType.Time] = typeof(TimeOnly),
    };

    // The types whose text Npgsql decodes itself, read as strings under their own type: read as
    // Text, a jsonb keeps the format-version byte of its binary form.
    private static readonly HashSet<NpgsqlTypes.NpgsqlDbType> TextScalars =
    [
        NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Varchar, NpgsqlTypes.NpgsqlDbType.Char,
        NpgsqlTypes.NpgsqlDbType.Name, NpgsqlTypes.NpgsqlDbType.Citext, NpgsqlTypes.NpgsqlDbType.Xml,
        NpgsqlTypes.NpgsqlDbType.Json, NpgsqlTypes.NpgsqlDbType.Jsonb, NpgsqlTypes.NpgsqlDbType.JsonPath,
        NpgsqlTypes.NpgsqlDbType.LTree, NpgsqlTypes.NpgsqlDbType.LQuery, NpgsqlTypes.NpgsqlDbType.LTxtQuery,
        NpgsqlTypes.NpgsqlDbType.Bit, NpgsqlTypes.NpgsqlDbType.Varbit,
    ];

    // The element types BuildBinaryListReader has an array read for.
    private static readonly HashSet<Type> ListElements =
    [
        typeof(bool), typeof(short), typeof(int), typeof(long), typeof(float), typeof(double),
        typeof(decimal), typeof(string), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly),
    ];

    /// <summary>
    /// The plan for a column, or null when the reader does not read its type. A domain arrives as its
    /// base type; an array is judged by its element, never by the spelling of its name.
    /// </summary>
    private static ColumnPlan? Plan(Npgsql.Schema.NpgsqlDbColumn column)
    {
        if (column.PostgresType is Npgsql.PostgresTypes.PostgresArrayType array)
        {
            var elementType = column.NpgsqlDbType is { } t ? t & ~NpgsqlTypes.NpgsqlDbType.Array : (NpgsqlTypes.NpgsqlDbType?)null;
            return Scalar(array.Element, elementType) is { } element && ListElements.Contains(element.ClrType)
                ? new ColumnPlan(element.ClrType.MakeArrayType(), NpgsqlTypes.NpgsqlDbType.Array, column.NpgsqlDbType)
                : null;
        }
        return Scalar(column.PostgresType, column.NpgsqlDbType);
    }

    private static ColumnPlan? Scalar(Npgsql.PostgresTypes.PostgresType? type, NpgsqlTypes.NpgsqlDbType? reported)
    {
        // An enum's binary form is its label.
        if (type is Npgsql.PostgresTypes.PostgresEnumType)
            return new ColumnPlan(typeof(string), NpgsqlTypes.NpgsqlDbType.Text, NpgsqlTypes.NpgsqlDbType.Text);
        if (type is not Npgsql.PostgresTypes.PostgresBaseType || reported is not { } r)
            return null;
        if (TextScalars.Contains(r))
            return new ColumnPlan(typeof(string), NpgsqlTypes.NpgsqlDbType.Text, r);
        return NativeScalars.TryGetValue(r, out var clr) ? new ColumnPlan(clr, r, r) : null;
    }

    /// <summary>
    /// The refusal of a column, with the cast that reads it: its text, or for money the number, whose
    /// scale the binary form does not carry.
    /// </summary>
    private static ColumnReadRefusal Refusal(Npgsql.Schema.NpgsqlDbColumn column)
    {
        var isArray = column.PostgresType is Npgsql.PostgresTypes.PostgresArrayType;
        var element = isArray ? ((Npgsql.PostgresTypes.PostgresArrayType)column.PostgresType).Element : column.PostgresType;
        var target = element.Name == "money" ? "numeric" : "text";
        var name = QuoteIfNeeded(column.ColumnName);
        return new ColumnReadRefusal(column.ColumnName, column.DataTypeName, $"{name}::{target}{(isArray ? "[]" : "")} AS {name}");
    }

    private static readonly DtPipe.Adapters.Shared.Dialects.PostgreSqlDialect Dialect = new();

    private static string QuoteIfNeeded(string name)
        => Dialect.NeedsQuoting(name) ? "\"" + name.Replace("\"", "\"\"") + "\"" : name;

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Columns == null || Schema == null || _connection == null)
            throw new InvalidOperationException("Call OpenAsync first.");

        var copySql = $"COPY ({_query}) TO STDOUT (FORMAT BINARY)";
        await using var exporter = await _connection.BeginBinaryExportAsync(copySql, ct);

        int targetBatchSize = BatchSize;
        long maxBatchBytes = MaxBatchBytes;
        long record = 0;

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
                readers[i] = BuildBinaryColumnReader(_plans[i], Schema.FieldsList[i].DataType, AddBytes);

            int rowsInBuffer = 0;
            bool streamEnded = false;
            while (rowsInBuffer < targetBatchSize)
            {
                var startRowVt = exporter.StartRowAsync(ct);
                int startRowResult = startRowVt.IsCompletedSuccessfully ? startRowVt.Result : await startRowVt;
                if (startRowResult == -1)
                {
                    streamEnded = true;
                    break;
                }

                ReadRow(exporter, readers, ++record);
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

    /// <summary>
    /// One row off the export. A value that cannot be read is refused by its column: a driver's message
    /// names neither the column nor a way out, and can quote the value.
    /// </summary>
    private void ReadRow(NpgsqlBinaryExporter exporter, (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build)[] readers, long record)
    {
        int i = 0;
        try
        {
            for (; i < readers.Length; i++)
                readers[i].Read(exporter);
        }
        catch (PostgresTemporal.UnreadableValue fault)
        {
            throw RefuseValue(i, record, fault.Message);
        }
        catch (Exception fault) when (Apache.Arrow.Serialization.ArrowValueException.IsValueFault(fault))
        {
            var type = Apache.Arrow.Serialization.Mapping.ArrowTypeMap.Spell(Schema!.FieldsList[i].DataType);
            throw RefuseValue(i, record, $"holds a value that does not fit {type}");
        }
    }

    private ColumnValueRefusedException RefuseValue(int column, long record, string problem)
    {
        var name = Columns![column].Name;
        var quoted = QuoteIfNeeded(name);
        return new ColumnValueRefusedException(
            $"Record {record}", name, _sourceTypes[column], problem, $"Select it as: {quoted}::text AS {quoted}");
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        batchSize = Math.Max(1, batchSize);
        // The record batch is as large as the reader's BatchSize; the contract bounds a batch by batchSize, one array each.
        await foreach (var recordBatch in ReadRecordBatchesAsync(ct))
        {
            using (recordBatch)
            {
                var colCount = recordBatch.Schema.FieldsList.Count;
                for (var start = 0; start < recordBatch.Length; start += batchSize)
                {
                    var rowCount = Math.Min(batchSize, recordBatch.Length - start);
                    var rows = new object?[rowCount][];
                    for (int i = 0; i < rowCount; i++)
                    {
                        var row = new object?[colCount];
                        for (int j = 0; j < colCount; j++)
                        {
                            var array = recordBatch.Column(j);
                            row[j] = ArrowTypeMapper.GetValueForField(array, recordBatch.Schema.FieldsList[j], start + i);
                        }
                        rows[i] = row;
                    }

                    yield return new ReadOnlyMemory<object?[]>(rows);
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection != null) await _connection.DisposeAsync();
    }

    #region Binary Column Readers

    private static (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build) BuildBinaryColumnReader(ColumnPlan plan, IArrowType arrowType, Action<long> addBytes)
    {
        if (arrowType is ListType listType)
            return BuildBinaryListReader(listType, plan.DecodeAs);

        var textReadType = plan.DecodeAs ?? NpgsqlTypes.NpgsqlDbType.Text;

        return plan.Builder switch
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
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(DecimalWidth.ForBuilder(e.Read<decimal>(), ((Decimal128Type)arrowType).Scale)); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Text => BuildTyped(
                new StringArray.Builder(),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else { var s = e.Read<string>(textReadType); b.Append(s); addBytes(System.Text.Encoding.UTF8.GetByteCount(s)); } },
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
                ? BuildTyped(new Date64Array.Builder(), (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(PostgresTemporal.Date(e.Read<int>(NpgsqlTypes.NpgsqlDbType.Date)).ToDateTime(TimeOnly.MinValue)); }, b => b.Build())
                : BuildTyped(new Date32Array.Builder(), (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(PostgresTemporal.Date(e.Read<int>(NpgsqlTypes.NpgsqlDbType.Date)).ToDateTime(TimeOnly.MinValue)); }, b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Timestamp => BuildTyped(
                new TimestampArray.Builder((TimestampType)arrowType),
                // Kind=Unspecified, as PostgresTemporal builds it; a raw Append would resolve it
                // against the local zone.
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(Apache.Arrow.Serialization.Mapping.TemporalNormalization.ToOffset(PostgresTemporal.Timestamp(e.Read<long>(NpgsqlTypes.NpgsqlDbType.Timestamp)))); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.TimestampTz => BuildTyped(
                new TimestampArray.Builder((TimestampType)arrowType),
                // timestamptz too: its binary form is the UTC wall clock, so it needs the same
                // normalization as a zone-less column.
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(Apache.Arrow.Serialization.Mapping.TemporalNormalization.ToOffset(PostgresTemporal.Timestamp(e.Read<long>(NpgsqlTypes.NpgsqlDbType.TimestampTz)))); },
                b => b.Build()),

            NpgsqlTypes.NpgsqlDbType.Time => BuildTyped(
                new Time64Array.Builder((Time64Type)arrowType),
                (e, b) => { if (e.IsNull) { e.Skip(); b.AppendNull(); } else b.Append(e.Read<TimeOnly>()); },
                b => b.Build()),

            // OpenAsync refuses every type it has no plan for, so no column reaches here unplanned.
            _ => throw new InvalidOperationException($"No reader for the planned type {plan.Builder}.")
        };
    }

    /// <summary>
    /// Reads a Postgres array column from the binary COPY stream into an Arrow list. Npgsql
    /// materializes the array itself (int[], string[]…), so the element type never has to be known
    /// at compile time here; the shared list builder walks it. Element conversion stays with
    /// <see cref="ArrowTypeMapper"/>, which owns the CLR↔Arrow map.
    /// </summary>
    private static (Action<NpgsqlBinaryExporter> Read, Func<IArrowArray> Build) BuildBinaryListReader(ListType listType, NpgsqlTypes.NpgsqlDbType? reportedType)
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
            // Under the column's own array type, jsonb[] elements drop their version byte; an
            // array type Npgsql does not name (an enum[]) keeps the untyped read.
            StringType => reportedType is { } arrayType
                ? e => e.Read<string?[]>(arrayType)
                : e => e.Read<string?[]>(),
            // Off the binary form, as a scalar is: infinity has no element of its own among dates.
            TimestampType => e => PostgresTemporal.Timestamps(e.Read<long?[]>(reportedType!.Value)),
            Date32Type => e => PostgresTemporal.Dates(e.Read<int?[]>(reportedType!.Value)),
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
