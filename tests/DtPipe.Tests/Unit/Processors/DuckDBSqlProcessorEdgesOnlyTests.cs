using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Processors.DuckDB;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// The SQL processor under <c>--edges-only</c>: it reads the channels its branch declares, through its own in-memory
/// database, and reaches no file, database or extension. What the coordinator's origin groups rely on: a branch reads
/// no alias it does not declare in <c>from</c> or <c>ref</c>, whatever runs beside it in the process.
/// </summary>
public class DuckDBSqlProcessorEdgesOnlyTests
{
    private static readonly Schema IdSchema = new([new Field("id", Int64Type.Default, nullable: false)], null);

    /// <summary>A registry holding one completed channel of <paramref name="rows"/> ids per alias.</summary>
    private static IMemoryChannelRegistry Registry(params (string Alias, int Rows)[] channels)
    {
        var mock = new Mock<IMemoryChannelRegistry>();
        foreach (var (alias, rows) in channels)
        {
            var channel = Channel.CreateUnbounded<RecordBatch>();
            var ids = new Int64Array.Builder();
            for (var i = 0; i < rows; i++) ids.Append(i);
            channel.Writer.TryWrite(new RecordBatch(IdSchema, [ids.Build()], rows));
            channel.Writer.Complete();
            mock.Setup(r => r.WaitForArrowChannelSchemaAsync(alias, It.IsAny<CancellationToken>())).ReturnsAsync(IdSchema);
            mock.Setup(r => r.GetArrowChannel(alias)).Returns((channel, IdSchema));
        }
        return mock.Object;
    }

    private static DuckDBSqlProcessor Processor(
        IMemoryChannelRegistry registry, string query, string main, string[]? refs = null, string? initSql = null, bool edgesOnly = true) =>
        new(registry, query, main, main, refs ?? [], refs ?? [], NullLogger<DuckDBSqlProcessor>.Instance, initSql, edgesOnly: edgesOnly);

    private static async Task<long> ScalarAsync(DuckDBSqlProcessor processor)
    {
        await processor.OpenAsync(TestContext.Current.CancellationToken);
        long value = 0;
        await foreach (var batch in processor.ReadRecordBatchesAsync(TestContext.Current.CancellationToken))
            using (batch)
                if (batch.Length > 0) value = ((Int64Array)batch.Column(0)).GetValue(0)!.Value;
        await processor.DisposeAsync();
        return value;
    }

    [Fact]
    public async Task It_Reads_Its_Streaming_Channel_And_Its_References()
    {
        var registry = Registry(("main", 5), ("lookup", 3));

        var count = await ScalarAsync(Processor(registry,
            "SELECT count(*) + (SELECT count(*) FROM lookup) * 100 AS n FROM main", "main", ["lookup"]));

        Assert.Equal(305, count);
    }

    [Theory]
    [InlineData("SELECT count(*) AS n FROM read_csv('/etc/hosts')")]
    [InlineData("SELECT count(*) AS n FROM 'x.parquet'")]
    [InlineData("SELECT count(*) AS n FROM sqlite_scan('stolen.db', 't')")]
    public async Task It_Refuses_A_Query_That_Reaches_A_File(string query)
    {
        var processor = Processor(Registry(("main", 1)), query, "main");

        var refusal = await Assert.ThrowsAnyAsync<Exception>(async () => await ScalarAsync(processor));
        Assert.Contains("disabled by configuration", refusal.ToString());
    }

    [Fact]
    public async Task It_Refuses_An_Init_Script_Before_Opening_Anything()
    {
        await using var processor = Processor(Registry(("main", 1)), "SELECT count(*) AS n FROM main", "main",
            initSql: "ATTACH 'stolen.db' AS stolen");

        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => processor.OpenAsync(TestContext.Current.CancellationToken));
        Assert.Contains("--duck-init is refused under --edges-only", refusal.Message);
    }

    [Fact]
    public async Task Its_Threads_Are_The_Processors_The_Runtime_Reports()
    {
        var threads = await ScalarAsync(Processor(Registry(("main", 1)), "SELECT current_setting('threads')::BIGINT AS n FROM main", "main"));

        Assert.Equal(Environment.ProcessorCount, threads);
    }

    [Fact]
    public async Task It_Cannot_Turn_A_Setting_Back()
    {
        var processor = Processor(Registry(("main", 1)), "SET enable_external_access = true", "main");

        var refusal = await Assert.ThrowsAnyAsync<Exception>(async () => await ScalarAsync(processor));
        Assert.Contains("locked", refusal.ToString());
    }

    [Fact]
    public async Task A_Branch_Reads_No_Alias_It_Does_Not_Declare_Even_Beside_A_Processor_That_Does()
    {
        // Both channels exist in the process, and a processor beside this one has registered 'secret' in its database.
        var registry = Registry(("main", 2), ("secret", 7));
        await using var declared = Processor(registry, "SELECT count(*) AS n FROM secret", "secret", edgesOnly: false);
        await declared.OpenAsync(TestContext.Current.CancellationToken);

        await using var undeclared = Processor(registry, "SELECT count(*) AS n FROM main, secret", "main", edgesOnly: false);

        var refusal = await Assert.ThrowsAnyAsync<Exception>(() => undeclared.OpenAsync(TestContext.Current.CancellationToken));
        Assert.Contains("secret", refusal.ToString());
    }

    /// <summary>
    /// What the processor relies on: DuckDB spills to its temporary directory with external access off and the
    /// configuration locked. The sort holds more than the memory limit, as the same sort with no temporary directory shows.
    /// </summary>
    [Fact]
    public async Task DuckDB_Still_Spills_To_Its_Temporary_Directory_Without_External_Access()
    {
        var temp = Directory.CreateTempSubdirectory("dtpipe-spill-");
        try
        {
            Assert.Equal(6_000_000, await SortAsync(temp.FullName));
            var unspilled = await Assert.ThrowsAnyAsync<Exception>(() => SortAsync(""));
            Assert.Contains("Out of Memory", unspilled.Message);
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    private static async Task<long> SortAsync(string temporaryDirectory)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new DuckDBConnection("DataSource=:memory:");
        await connection.OpenAsync(ct);
        await using var cmd = connection.CreateCommand();
        foreach (var setting in new[]
                 {
                     $"SET temp_directory = '{temporaryDirectory}'", "SET memory_limit = '64MB'", "SET threads = 2",
                     "SET preserve_insertion_order = false", "SET enable_external_access = false", "SET lock_configuration = true",
                 })
        {
            cmd.CommandText = setting;
            await cmd.ExecuteNonQueryAsync(ct);
        }

        cmd.CommandText = "SELECT range AS k, random() AS r FROM range(6000000) ORDER BY r";
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        long rows = 0;
        while (await reader.ReadAsync(ct)) rows++;
        return rows;
    }
}
