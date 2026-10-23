using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public sealed partial class PostgreSqlDataWriter
{
    private async Task WriteBatchViaStagingAsync(IReadOnlyList<object?[]> rows, CancellationToken ct)
    {
        var stagingTable = $"tmp_batch_{Guid.NewGuid():N}";
        await ExecuteNonQueryAsync($"CREATE TEMP TABLE {stagingTable} (LIKE {_quotedTargetTableName} INCLUDING DEFAULTS)", ct);

        try
        {
            var copySql = BuildCopySql(stagingTable);
            await using (var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct))
            {
                await WriteRowsToCopyAsync(writer, rows, ct);
                await writer.CompleteAsync(ct);
            }

            await MergeStagingBatchAsync(stagingTable, ct);
        }
        finally
        {
            try
            {
                await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {stagingTable}", ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to drop staging table {TableName}", stagingTable);
            }
        }
    }

    private async Task WriteRecordBatchViaStagingAsync(RecordBatch batch, CancellationToken ct)
    {
        var stagingTable = $"tmp_batch_{Guid.NewGuid():N}";
        await ExecuteNonQueryAsync($"CREATE TEMP TABLE {stagingTable} (LIKE {_quotedTargetTableName} INCLUDING DEFAULTS)", ct);

        try
        {
            var copySql = BuildCopySql(stagingTable);
            await using (var writer = await ((NpgsqlConnection)_connection!).BeginBinaryImportAsync(copySql, ct))
            {
                await WriteColumnarToCopyAsync(writer, batch, ct);
                await writer.CompleteAsync(ct);
            }

            await MergeStagingBatchAsync(stagingTable, ct);
        }
        finally
        {
            try { await ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {stagingTable}", ct); } catch { }
        }
    }

    private async Task MergeStagingBatchAsync(string stagingTable, CancellationToken ct)
    {
        // F9: SQL generation is dialect-owned (shared ANSI ON CONFLICT builder).
        var mode = _options.Strategy switch
        {
            PostgreSqlWriteStrategy.Ignore => MergeMode.Ignore,
            PostgreSqlWriteStrategy.Upsert => MergeMode.Upsert,
            _ => MergeMode.Insert,
        };

        var spec = new MergeSpec(
            QuotedTargetTable: _quotedTargetTableName,
            SourceTable: _dialect.Quote(stagingTable),
            KeyColumns: _keyColumns,
            Columns: _targetNames!.Select(n => new PipeColumnInfo(n, typeof(object), false)).ToList(),
            Mode: mode);

        await ExecuteNonQueryAsync(_dialect.BuildStagingMerge(spec), ct);
    }
}
