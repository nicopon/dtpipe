using System.Data.Common;

namespace DtPipe.Adapters.Shared.Infrastructure.DuckDb;

/// <summary>
/// The memory ceiling and spill directory every DuckDB instance of the process starts with.
/// Applied by the <c>duck:</c> reader, the <c>duck:</c> writer and the <c>--sql</c> processor
/// right after their connection opens and before <c>--duck-init</c>, so a user's own
/// <c>SET</c> still wins.
/// </summary>
/// <remarks>
/// <para>
/// DuckDB spills to disk on its own once it nears <c>memory_limit</c>. What it cannot see is the
/// memory it does not account for, so its own default would leave the process short of room.
/// </para>
/// <para>
/// Each instance gets its own spill directory. Instances sharing one delete each other's live
/// spill files when one of them closes, which makes the other's query fail. DuckDB
/// creates the leaf at the first spill and removes it on close, so nothing is left behind.
/// </para>
/// <para>
/// Both settings are locked by <c>enable_external_access=false</c>: a caller that locks the
/// instance must apply these first.
/// </para>
/// </remarks>
public static class DuckDbResourceSettings
{
    /// <summary>
    /// Share of the memory the .NET runtime reports as available (the physical memory, or the
    /// share of a container's limit it grants itself) that DuckDB's buffer manager may use. The
    /// remainder is headroom for what DuckDB does not account for: the .NET heap, the Arrow
    /// batches handed to a scan, and the output buffers.
    /// </summary>
    public const double MemoryFraction = 0.6;

    private const string SpillRootName = "dtpipe";

    /// <summary>
    /// Sets the memory ceiling and the spill directory on <paramref name="connection"/>. A setting
    /// whose input is unknown is left to DuckDB's own default.
    /// </summary>
    public static async Task ApplyAsync(DbConnection connection, CancellationToken ct)
    {
        var sql = BuildSql(
            GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
            CreateSpillDirectory(Path.GetTempPath()));
        if (sql.Length == 0) return;

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The settings for a process with <paramref name="availableBytes"/> of memory (zero or
    /// negative when unknown) and a spill directory, or none when that directory is unusable.
    /// </summary>
    public static string BuildSql(long availableBytes, string? spillDirectory)
    {
        var sql = new System.Text.StringBuilder();

        if (availableBytes > 0)
        {
            var limitMiB = (long)(availableBytes * MemoryFraction / (1024 * 1024));
            if (limitMiB > 0)
                sql.Append("SET memory_limit='").Append(limitMiB).Append("MiB'; ");
        }

        if (!string.IsNullOrEmpty(spillDirectory))
            sql.Append("SET temp_directory='").Append(Quote(spillDirectory)).Append("'; ");

        return sql.ToString();
    }

    /// <summary>
    /// A fresh spill directory path under <paramref name="tempRoot"/>, or null when the directory
    /// that holds it cannot be written. The path itself is not created: DuckDB does that at the
    /// first spill, and an unwritable path would only fail then, in the middle of a query.
    /// </summary>
    public static string? CreateSpillDirectory(string tempRoot)
    {
        try
        {
            var root = Path.Combine(tempRoot, SpillRootName);
            Directory.CreateDirectory(root);

            var probe = Path.Combine(root, $".probe-{Guid.NewGuid():N}");
            using (new FileStream(probe, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                       bufferSize: 1, FileOptions.DeleteOnClose))
            {
            }

            return Path.Combine(root, $"duckdb-{Environment.ProcessId}-{Guid.NewGuid():N}");
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
        {
            return null;
        }
    }

    private static string Quote(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}
