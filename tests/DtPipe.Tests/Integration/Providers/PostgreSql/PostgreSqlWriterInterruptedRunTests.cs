using DtPipe.Adapters.PostgreSQL;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// The PostgreSQL writer commits each batch on its own and wraps the run in no transaction: a
/// batch refused after earlier ones were written leaves those earlier batches in the table, with
/// their exact values, and the refused batch writes nothing. The exception that surfaces is what
/// ends the run with exit code 1.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlWriterInterruptedRunTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
{
    private const string Table = "writer_interrupted_run_test";

    [Fact]
    public async Task An_Append_Run_Refused_At_Its_Last_Batch_Leaves_The_Batches_Already_Written()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(int), true) };
        using var first = TestBatchBuilder.FromRows(columns, [1, null], [2, null]);
        using var second = TestBatchBuilder.FromRows(columns, [3, null], [4, null]);
        using var last = TestBatchBuilder.FromRows(columns, [5, 5], [6, 6]);

        await using var writer = new PostgreSqlDataWriter(connectionString,
            new PostgreSqlWriterOptions { Table = Table, Strategy = PostgreSqlWriteStrategy.Append },
            NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(first);
        await writer.WriteRecordBatchAsync(second);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteRecordBatchAsync(last));

        Assert.Contains("Column 'n' is bytea and the source sends int32", ex.Message);
        Assert.Equal(["1 | ", "2 | ", "3 | ", "4 | "], await ReadBackAsync(connectionString));
    }

    private static async Task CreateTargetAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {Table}; CREATE TABLE {Table} (id INT, n BYTEA)";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> ReadBackAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT id || ' | ' || coalesce(n::text, '') FROM {Table} ORDER BY id";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(0));
        return rows;
    }
}
