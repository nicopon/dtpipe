using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using DtPipe.Core.Infrastructure.Arrow;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter
{
    private async Task WriteBatchDirectAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
    {
        var copySql = BuildCopySql(_quotedTargetTableName);
        _logger.LogDebug("Executing PostgreSQL Binary Import: {Sql}", copySql);
        await using var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct);

        await WriteRowsToCopyAsync(writer, rows, ct);
        await writer.CompleteAsync(ct);
    }

    private async Task WriteRecordBatchDirectAsync(RecordBatch batch, CancellationToken ct)
    {
        var copySql = BuildCopySql(_quotedTargetTableName);
        await using var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct);
        await WriteColumnarToCopyAsync(writer, batch, ct);
        await writer.CompleteAsync(ct);
    }

    private async Task WriteColumnarToCopyAsync(NpgsqlBinaryImporter writer, RecordBatch batch, CancellationToken ct)
    {
        for (int row = 0; row < batch.Length; row++)
        {
            await writer.StartRowAsync(ct);
            for (int j = 0; j < _targetToSourceMap!.Length; j++)
            {
                var array = batch.Column(_targetToSourceMap[j]);
                if (array.IsNull(row))
                    writer.WriteNull();  // sync, no allocation
                else
                    _arrowColumnWriters![j](array, row, writer);  // sync, zero-boxing
            }
        }
    }

    private async Task WriteRowsToCopyAsync(NpgsqlBinaryImporter writer, IReadOnlyList<object?[]> rows, CancellationToken ct)
    {
        foreach (var row in rows)
        {
            await writer.StartRowAsync(ct);
            for (int j = 0; j < _targetToSourceMap!.Length; j++)
            {
                int sourceIdx = _targetToSourceMap[j];
                var val = _converters![j](row[sourceIdx]);

                if (val is null || val == DBNull.Value)
                {
                    await writer.WriteNullAsync(ct);
                    continue;
                }

                if (val is DateTime dt)
                {
                    var dbType = _columnTypes![j];
                    if (dbType == NpgsqlTypes.NpgsqlDbType.Timestamp && dt.Kind != DateTimeKind.Unspecified)
                        val = DateTime.SpecifyKind(dt, DateTimeKind.Unspecified);
                    else if (dbType == NpgsqlTypes.NpgsqlDbType.TimestampTz && dt.Kind == DateTimeKind.Unspecified)
                        val = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                }

                await writer.WriteAsync(val, _columnTypes![j], ct);
            }
        }
    }

    private string BuildCopySql(string tableName)
    {
        var sb = new StringBuilder();
        sb.Append($"COPY {tableName} (");
        for (int i = 0; i < _targetNames!.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(_dialect.Quote(_targetNames[i]));
        }
        sb.Append(") FROM STDIN (FORMAT BINARY)");
        return sb.ToString();
    }

    private static Action<IArrowArray, int, NpgsqlBinaryImporter> BuildNpgsqlColumnWriter(
        NpgsqlTypes.NpgsqlDbType npgsqlType) => npgsqlType switch
        {
            NpgsqlTypes.NpgsqlDbType.Boolean =>
                static (arr, row, w) => w.Write(((BooleanArray)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Boolean),

            NpgsqlTypes.NpgsqlDbType.Smallint =>
                static (arr, row, w) => w.Write(((Int16Array)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Smallint),

            NpgsqlTypes.NpgsqlDbType.Integer =>
                static (arr, row, w) => w.Write(((Int32Array)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Integer),

            NpgsqlTypes.NpgsqlDbType.Bigint =>
                static (arr, row, w) => w.Write(((Int64Array)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Bigint),

            NpgsqlTypes.NpgsqlDbType.Real =>
                static (arr, row, w) => w.Write(((FloatArray)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Real),

            NpgsqlTypes.NpgsqlDbType.Double =>
                static (arr, row, w) => w.Write(((DoubleArray)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Double),

            NpgsqlTypes.NpgsqlDbType.Numeric =>
                static (arr, row, w) => w.Write(((Decimal128Array)arr).GetValue(row)!.Value, NpgsqlTypes.NpgsqlDbType.Numeric),

            NpgsqlTypes.NpgsqlDbType.Text or NpgsqlTypes.NpgsqlDbType.Varchar
                or NpgsqlTypes.NpgsqlDbType.Char or NpgsqlTypes.NpgsqlDbType.Name =>
                (arr, row, w) => w.Write(((StringArray)arr).GetString(row)!, npgsqlType),

            NpgsqlTypes.NpgsqlDbType.Bytea =>
                static (arr, row, w) => w.Write(arr switch
                {
                    BinaryArray b => b.GetBytes(row).ToArray(),
                    FixedSizeBinaryArray f => f.GetBytes(row).ToArray(),
                    _ => System.Array.Empty<byte>()
                }, NpgsqlTypes.NpgsqlDbType.Bytea),

            // DtPipe UUID convention: Binary(16) = RFC 4122 big-endian bytes
            // UUID: arrow.uuid extension uses FixedSizeBinaryType(16)
            NpgsqlTypes.NpgsqlDbType.Uuid =>
                static (arr, row, w) =>
                    w.Write(ArrowTypeMapper.FromArrowUuidBytes(((FixedSizeBinaryArray)arr).GetBytes(row)),
                        NpgsqlTypes.NpgsqlDbType.Uuid),

            NpgsqlTypes.NpgsqlDbType.Date =>
                static (arr, row, w) =>
                {
                    var dt = arr switch
                    {
                        Date32Array d32 => d32.GetDateTime(row),
                        Date64Array d64 => d64.GetDateTime(row),
                        _ => ArrowTypeMapper.GetValue(arr, row) as DateTime? ?? default
                    };
                    w.Write(dt, NpgsqlTypes.NpgsqlDbType.Date);
                }
            ,

            // Fix T19/T52: also handle Date64Array/Date32Array for Timestamp columns
            NpgsqlTypes.NpgsqlDbType.Timestamp =>
                static (arr, row, w) =>
                {
                    var dt = arr switch
                    {
                        TimestampArray ts => ts.GetTimestamp(row)?.DateTime ?? default,
                        Date64Array d64 => d64.GetDateTime(row) ?? default,
                        Date32Array d32 => d32.GetDateTime(row) ?? default,
                        _ => ArrowTypeMapper.GetValue(arr, row) as DateTime? ?? default
                    };
                    w.Write(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), NpgsqlTypes.NpgsqlDbType.Timestamp);
                }
            ,

            NpgsqlTypes.NpgsqlDbType.TimestampTz =>
                static (arr, row, w) =>
                {
                    var dt = arr switch
                    {
                        TimestampArray ts => ts.GetTimestamp(row)?.DateTime ?? default,
                        Date64Array d64 => d64.GetDateTime(row) ?? default,
                        Date32Array d32 => d32.GetDateTime(row) ?? default,
                        _ => ArrowTypeMapper.GetValue(arr, row) as DateTime? ?? default
                    };
                    var utc = dt.Kind == DateTimeKind.Unspecified
                        ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
                        : dt;
                    w.Write(utc, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                }
            ,

            // Fallback: box through object (covers InternalChar, Oid, Money, etc.)
            _ => (arr, row, w) =>
            {
                var val = ArrowTypeMapper.GetValue(arr, row);
                if (val is null) w.WriteNull();
                else w.Write(val, npgsqlType);
            }
        };
}
