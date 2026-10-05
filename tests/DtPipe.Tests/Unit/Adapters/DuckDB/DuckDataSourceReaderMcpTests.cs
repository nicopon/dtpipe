using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Security;
using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.DuckDB;

/// <summary>
/// An MCP session hands the reader to a model, so a <c>SELECT</c> the read-only guard accepts must not
/// become a way to read the host's files. DuckDB's own switch is what refuses it; these tests open a
/// real in-memory instance rather than assert the SQL the reader sends.
/// </summary>
public class DuckDataSourceReaderMcpTests : IDisposable
{
    private sealed class Session(bool isMcp) : IMcpSecurityContext
    {
        public bool IsMcpSession { get; set; } = isMcp;
    }

    private readonly string _csv = Path.Combine(Path.GetTempPath(), "dtpipe-mcp-" + Guid.NewGuid().ToString("N") + ".csv");

    public DuckDataSourceReaderMcpTests() => File.WriteAllText(_csv, "id\n1\n2\n");

    public void Dispose() => File.Delete(_csv);

    private static DuckDataSourceReader Reader(string query, bool isMcp, string? initSql = null) => new(
        DuckDbConnectionHelper.InMemoryConnectionString,
        query,
        new DuckDbReaderOptions { InitSql = initSql },
        mcpSecurityContext: new Session(isMcp));

    private static async Task<List<object?[]>> ReadAsync(DuckDataSourceReader reader)
    {
        await reader.OpenAsync();
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            using (batch)
                for (var i = 0; i < batch.Length; i++)
                    rows.Add(Enumerable.Range(0, batch.ColumnCount)
                        .Select(c => ArrowTypeMapper.GetValue(batch.Column(c), i)).ToArray());
        }
        return rows;
    }

    [Fact]
    public async Task McpSession_StillReadsAnOrdinaryQuery()
    {
        await using var reader = Reader("SELECT 42 AS answer", isMcp: true);

        var rows = await ReadAsync(reader);

        Assert.Equal(42, Convert.ToInt32(Assert.Single(rows)[0]));
    }

    [Fact]
    public async Task McpSession_RefusesToReadALocalFile()
    {
        await using var reader = Reader($"SELECT * FROM read_csv('{_csv.Replace("'", "''")}')", isMcp: true);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(reader));

        Assert.Contains("Permission Error", ex.Message);
        Assert.Contains("Cannot access file", ex.Message);
    }

    [Fact]
    public async Task OrdinarySession_ReadsTheSameFile()
    {
        await using var reader = Reader($"SELECT * FROM read_csv('{_csv.Replace("'", "''")}')", isMcp: false);

        Assert.Equal(2, (await ReadAsync(reader)).Count);
    }

    /// <summary>The init SQL of a tool-driven read runs after the lock, so it cannot lift it.</summary>
    [Fact]
    public async Task McpSession_InitSqlCannotLiftTheLock()
    {
        await using var reader = Reader("SELECT 1 AS x", isMcp: true, initSql: "SET enable_external_access = true");

        var ex = await Assert.ThrowsAsync<DuckDBException>(() => ReadAsync(reader));

        Assert.Contains("Cannot enable external access", ex.Message);
    }

    /// <summary>
    /// The spill directory is locked together with external access, so it has to be set first. A
    /// query run afterwards must still report the per-instance directory.
    /// </summary>
    [Fact]
    public async Task McpSession_KeepsTheSpillDirectorySetBeforeTheLock()
    {
        await using var reader = Reader("SELECT current_setting('temp_directory') AS dir", isMcp: true);

        var dir = Convert.ToString(Assert.Single(await ReadAsync(reader))[0]);

        Assert.Contains(Path.Combine("dtpipe", "duckdb-"), dir);
    }
}
