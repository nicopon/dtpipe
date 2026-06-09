using DtPipe.Core.Security;
using DuckDB.NET.Data;

namespace DtPipe.Adapters.DuckDB;

internal static class DuckInitSqlHelper
{
    internal static async Task RunAsync(DuckDBConnection conn, string? initSql, IStringContentResolver? resolver, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(initSql)) return;

        var sql = await (resolver ?? DefaultStringContentResolver.Instance).ResolveAsync(initSql, ct);
        if (string.IsNullOrWhiteSpace(sql)) return;

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
