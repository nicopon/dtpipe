using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Processors.Sql;

/// <summary>
/// Shared helpers used by both <see cref="DataFusion.DataFusionProcessor"/> and
/// <see cref="DuckDB.DuckDBSqlProcessor"/> to avoid code duplication.
/// </summary>
internal static class SqlProcessorHelpers
{
    internal static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
    {
        var rows = new object?[batch.Length][];
        for (int r = 0; r < batch.Length; r++)
        {
            rows[r] = new object?[batch.ColumnCount];
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                var col = batch.Column(c);
                rows[r][c] = col == null ? null : ArrowTypeMapper.GetValueForField(col, batch.Schema.GetFieldByIndex(c), r);
            }
        }
        return rows;
    }

    internal static void ValidateAliases(string? mainChannelAlias, string[] refChannelAliases)
    {
        var aliases = new List<string>();
        if (!string.IsNullOrEmpty(mainChannelAlias)) aliases.Add(mainChannelAlias);
        aliases.AddRange(refChannelAliases);

        var groups = aliases.GroupBy(a => a.ToLowerInvariant())
                            .Where(g => g.Count() > 1)
                            .ToList();

        if (groups.Any())
        {
            var duplicates = string.Join(", ", groups.Select(g => $"'{string.Join("' vs '", g)}'"));
            throw new InvalidOperationException($"Case ambiguity detected in stream aliases: {duplicates}");
        }
    }

    internal static void ValidateSchema(string alias, Apache.Arrow.Schema schema)
    {
        var groups = schema.FieldsList.GroupBy(f => f.Name.ToLowerInvariant())
                                     .Where(g => g.Count() > 1)
                                     .ToList();

        if (groups.Any())
        {
            var duplicates = string.Join(", ", groups.Select(g => $"'{string.Join("' vs '", g)}'"));
            throw new InvalidOperationException($"Case ambiguity detected in columns for stream '{alias}': {duplicates}");
        }
    }
}
