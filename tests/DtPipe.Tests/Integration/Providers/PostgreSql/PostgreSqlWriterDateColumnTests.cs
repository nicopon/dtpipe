using DtPipe.Adapters.PostgreSQL;
using DtPipe.Core.Models;
using DtPipe.Tests.Fixtures;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Tests.Integration;

/// <summary>
/// The PostgreSQL writer appending to a <c>date</c> column writes a date source and refuses a source
/// of another type, a timestamp included: the column named, the cast to ask for given, and none of
/// the rows of the batch written.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlWriterDateColumnTests(GlobalDatabaseFixture fixture)
{
    private const string Table = "writer_date_column_test";

    [Fact]
    public async Task A_Timestamp_Source_Is_Refused_Naming_The_Column_And_The_Cast()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(DateTime), true) };
        using var batch = TestBatchBuilder.FromRows(columns,
            [1, new DateTime(2024, 1, 2, 13, 45, 0)],
            [2, new DateTime(1999, 12, 31, 23, 59, 59)]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteRecordBatchAsync(batch));

        Assert.Contains("Column 'n' is date and the source sends timestamp", ex.Message);
        Assert.Contains("CAST(n AS DATE)", ex.Message);
        Assert.Empty(await ReadBackAsync(connectionString));
    }

    [Fact]
    public async Task A_Date_Source_Is_Written()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(DateOnly), true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, new DateOnly(2024, 1, 2)], [2, null]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(batch);

        Assert.Equal(["1 | 2024-01-02", "2 | "], await ReadBackAsync(connectionString));
    }

    private static PostgreSqlDataWriter NewWriter(string connectionString)
        => new(connectionString,
            new PostgreSqlWriterOptions { Table = Table, Strategy = PostgreSqlWriteStrategy.Append },
            NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);

    private static async Task CreateTargetAsync(string connectionString)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {Table}; CREATE TABLE {Table} (id INT, n DATE)";
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
