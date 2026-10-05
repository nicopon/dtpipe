using System.Data.Common;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.DuckDB;

public class DuckDbResourceSettingsTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dtpipe-settings-tests-" + Guid.NewGuid().ToString("N"));

    public DuckDbResourceSettingsTests() => Directory.CreateDirectory(_root);

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public void BuildSql_SetsTheLimitToTheFractionOfAvailableMemory()
    {
        var sql = DuckDbResourceSettings.BuildSql(availableBytes: 1024L * 1024 * 1024, spillDirectory: null);

        var expectedMiB = (long)(1024 * DuckDbResourceSettings.MemoryFraction);
        Assert.Equal($"SET memory_limit='{expectedMiB}MiB'; ", sql);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void BuildSql_LeavesTheLimitToDuckDb_WhenAvailableMemoryIsUnknown(long availableBytes)
    {
        var sql = DuckDbResourceSettings.BuildSql(availableBytes, spillDirectory: "/spill");

        Assert.DoesNotContain("memory_limit", sql);
        Assert.Contains("temp_directory='/spill'", sql);
    }

    [Fact]
    public void BuildSql_LeavesTheDirectoryToDuckDb_WhenNoneIsUsable()
    {
        var sql = DuckDbResourceSettings.BuildSql(availableBytes: 1024L * 1024 * 1024, spillDirectory: null);

        Assert.DoesNotContain("temp_directory", sql);
    }

    [Fact]
    public void BuildSql_DoublesAQuoteInThePath()
    {
        var sql = DuckDbResourceSettings.BuildSql(availableBytes: 0, spillDirectory: "/home/o'brien/tmp");

        Assert.Equal("SET temp_directory='/home/o''brien/tmp'; ", sql);
    }

    [Fact]
    public void CreateSpillDirectory_GivesEachCallItsOwnPath_WithoutCreatingIt()
    {
        var first = DuckDbResourceSettings.CreateSpillDirectory(_root);
        var second = DuckDbResourceSettings.CreateSpillDirectory(_root);

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.NotEqual(first, second);
        Assert.Equal(Path.Combine(_root, "dtpipe"), Path.GetDirectoryName(first));
        Assert.False(Directory.Exists(first), "DuckDB creates the leaf at the first spill and removes it on close");
        Assert.Empty(Directory.GetFileSystemEntries(Path.Combine(_root, "dtpipe")));
    }

    [Fact]
    public void CreateSpillDirectory_ReturnsNull_WhenTheRootCannotHoldADirectory()
    {
        var file = Path.Combine(_root, "a-file");
        File.WriteAllText(file, "");

        Assert.Null(DuckDbResourceSettings.CreateSpillDirectory(file));
    }

    [Fact]
    public async Task ApplyAsync_GivesEveryInstanceItsOwnSpillDirectory_AndTheSameLimitAsTheFraction()
    {
        await using var first = new DuckDBConnection("Data Source=:memory:");
        await using var second = new DuckDBConnection("Data Source=:memory:");
        await first.OpenAsync();
        await second.OpenAsync();

        await DuckDbResourceSettings.ApplyAsync(first, CancellationToken.None);
        await DuckDbResourceSettings.ApplyAsync(second, CancellationToken.None);

        var firstDir = await ScalarAsync(first, "SELECT current_setting('temp_directory')");
        var secondDir = await ScalarAsync(second, "SELECT current_setting('temp_directory')");
        Assert.NotEqual(firstDir, secondDir);
        Assert.Contains(Path.Combine("dtpipe", "duckdb-"), firstDir);

        // The limit DuckDB reports must be what the fraction of the same figure produces.
        var limitMiB = (long)(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes * DuckDbResourceSettings.MemoryFraction / (1024 * 1024));
        await using var reference = new DuckDBConnection("Data Source=:memory:");
        await reference.OpenAsync();
        await ScalarAsync(reference, $"SET memory_limit='{limitMiB}MiB'");
        Assert.Equal(
            await ScalarAsync(reference, "SELECT current_setting('memory_limit')"),
            await ScalarAsync(first, "SELECT current_setting('memory_limit')"));
    }

    private static async Task<string> ScalarAsync(DbConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(await cmd.ExecuteScalarAsync()) ?? "";
    }
}
