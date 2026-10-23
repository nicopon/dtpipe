using DtPipe.Core.Models;
using MySqlConnector;

namespace DtPipe.Adapters.MySql;

public sealed partial class MySqlDataWriter
{
    /// <summary>
    /// Resolves the database and table names for information_schema lookups. An empty database
    /// means "the connection's default", which information_schema spells as DATABASE().
    /// </summary>
    private (string Schema, string Table) SplitTableName()
    {
        var raw = (_options.Table ?? string.Empty).Trim();
        var parts = raw.Split('.', 2);
        return parts.Length == 2
            ? (parts[0].Trim('`'), parts[1].Trim('`'))
            : (string.Empty, raw.Trim('`'));
    }

    protected override async Task<TargetSchemaInfo?> InspectTargetInternalAsync(CancellationToken ct = default)
    {
        var (schema, table) = SplitTableName();

        // A separate connection, as the other SQL writers do: introspection must not disturb the
        // state (or open transaction) of the connection the write path is using.
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        var columns = new List<TargetColumnInfo>();
        var pkColumns = new List<string>();
        var uniqueColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var indexes = await LoadUniqueIndexesAsync(connection, schema, table, ct);
        if (indexes.TryGetValue("PRIMARY", out var primary)) pkColumns.AddRange(primary);
        foreach (var index in indexes.Where(i => i.Key != "PRIMARY"))
            foreach (var col in index.Value)
                uniqueColumns.Add(col);

        var pkSet = new HashSet<string>(pkColumns, StringComparer.OrdinalIgnoreCase);

        await using (var cmd = connection.CreateCommand())
        {
            // COLUMN_TYPE, not DATA_TYPE: only the former keeps the width and the unsigned flag,
            // and both change which CLR type the column really holds (tinyint(1) → bool,
            // char(36) → Guid, int unsigned → uint).
            cmd.CommandText = @"
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH,
                       NUMERIC_PRECISION, NUMERIC_SCALE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = IFNULL(NULLIF(@schema, ''), DATABASE()) AND TABLE_NAME = @table
                ORDER BY ORDINAL_POSITION";
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var columnType = reader.GetString(1);
                var nullable = reader.GetString(2) == "YES";
                var maxLength = ReadLengthOrNull(reader, 3);
                var precision = ReadLengthOrNull(reader, 4);
                var scale = ReadLengthOrNull(reader, 5);

                columns.Add(new TargetColumnInfo(
                    name,
                    columnType,
                    _typeMapper.MapFromProviderType(columnType),
                    nullable,
                    pkSet.Contains(name),
                    uniqueColumns.Contains(name),
                    maxLength,
                    precision,
                    scale,
                    // MySQL matches column identifiers case-insensitively; nothing needs quoting
                    // to preserve case.
                    IsCaseSensitive: false));
            }
        }

        if (columns.Count == 0) return new TargetSchemaInfo([], false, null, null, null);

        long? rowCount = null;
        long? sizeBytes = null;
        await using (var cmd = connection.CreateCommand())
        {
            // TABLE_ROWS is InnoDB's sampled estimate, not a count — flagged as such below rather
            // than paying a full COUNT(*) on every inspection of a table that may be very large.
            cmd.CommandText = @"
                SELECT TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = IFNULL(NULLIF(@schema, ''), DATABASE()) AND TABLE_NAME = @table";
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", table);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                if (!reader.IsDBNull(0)) rowCount = Convert.ToInt64(reader.GetValue(0));
                if (!reader.IsDBNull(1)) sizeBytes = Convert.ToInt64(reader.GetValue(1));
            }
        }

        return new TargetSchemaInfo(
            columns,
            true,
            rowCount,
            sizeBytes,
            pkColumns.Count > 0 ? pkColumns : null,
            uniqueColumns.Count > 0 ? uniqueColumns.ToList() : null,
            IsRowCountEstimate: true);
    }

    /// <summary>
    /// Reads an information_schema length/precision column into <c>int?</c>.
    /// <para>
    /// These columns are BIGINT UNSIGNED, and MySQL means it: CHARACTER_MAXIMUM_LENGTH is
    /// 4 294 967 295 for LONGTEXT, which overflows the <c>int?</c> that
    /// <see cref="TargetColumnInfo.MaxLength" /> declares. A value past int range is reported as
    /// null — "no meaningful bound" — rather than truncated to a number that would later be
    /// emitted as a nonsense VARCHAR width.
    /// </para>
    /// </summary>
    private static int? ReadLengthOrNull(MySqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return null;
        var value = Convert.ToUInt64(reader.GetValue(ordinal));
        return value > int.MaxValue ? null : (int)value;
    }

    private async Task<Dictionary<string, List<string>>> LoadUniqueIndexesAsync(CancellationToken ct)
    {
        var (schema, table) = SplitTableName();
        await using var connection = new MySqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        return await LoadUniqueIndexesAsync(connection, schema, table, ct);
    }

    /// <summary>
    /// Every unique index on the table, keyed by index name, columns in declaration order.
    /// PRIMARY is one of them — MySQL models the primary key as an index named PRIMARY, which is
    /// also why the upsert has no conflict target to name.
    /// </summary>
    private static async Task<Dictionary<string, List<string>>> LoadUniqueIndexesAsync(
        MySqlConnection connection, string schema, string table, CancellationToken ct)
    {
        var result = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT INDEX_NAME, COLUMN_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = IFNULL(NULLIF(@schema, ''), DATABASE())
              AND TABLE_NAME = @table
              AND NON_UNIQUE = 0
            ORDER BY INDEX_NAME, SEQ_IN_INDEX";
        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var indexName = reader.GetString(0);
            if (!result.TryGetValue(indexName, out var cols))
            {
                cols = new List<string>();
                result[indexName] = cols;
            }
            cols.Add(reader.GetString(1));
        }

        return result;
    }
}
