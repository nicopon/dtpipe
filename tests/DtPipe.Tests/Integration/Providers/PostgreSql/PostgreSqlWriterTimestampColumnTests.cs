using DtPipe.Adapters.PostgreSQL;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// The PostgreSQL writer appending to a <c>timestamp</c> or <c>timestamptz</c> column writes a
/// temporal source and refuses a source of another type: the column named, the cast to ask for given,
/// and none of the rows of the batch written.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlWriterTimestampColumnTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
{
    private const string Table = "writer_timestamp_column_test";

    public static TheoryData<string, Type, object, string, string> Refused => new()
    {
        { "timestamp", typeof(string), "2024-01-02 10:00:00", "utf8", "CAST(n AS TIMESTAMP)" },
        { "timestamp", typeof(TimeOnly), new TimeOnly(10, 0, 0), "time64", "CAST(n AS TIMESTAMP)" },
        { "timestamp", typeof(int), 5, "int32", "CAST(n AS TIMESTAMP)" },
        { "timestamptz", typeof(string), "2024-01-02", "utf8", "CAST(n AS TIMESTAMPTZ)" },
    };

    [Theory]
    [MemberData(nameof(Refused))]
    public async Task A_Non_Temporal_Source_Is_Refused_Naming_The_Column_And_The_Cast(
        string columnType, Type sourceType, object value, string arrowName, string cast)
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString, columnType);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", sourceType, true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, value], [2, value]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteRecordBatchAsync(batch));

        Assert.Contains($"Column 'n' is {columnType} and the source sends {arrowName}", ex.Message);
        Assert.Contains(cast, ex.Message);
        Assert.Empty(await ReadBackAsync(connectionString, "n::text"));
    }

    [Theory]
    [InlineData("timestamp", "to_char(n, 'YYYY-MM-DD HH24:MI:SS')", "2024-01-02 13:45:00")]
    [InlineData("timestamptz", "to_char(n AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')", "2024-01-02 13:45:00")]
    public async Task A_Timestamp_Source_Is_Written(string columnType, string select, string expected)
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString, columnType);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(DateTime), true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, new DateTime(2024, 1, 2, 13, 45, 0)], [2, null]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(batch);

        Assert.Equal([$"1 | {expected}", "2 | "], await ReadBackAsync(connectionString, select));
    }

    [Fact]
    public async Task A_Date_Source_Is_Written_Into_A_Timestamp_Column()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString, "timestamp");

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(DateOnly), true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, new DateOnly(2024, 1, 2)]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(batch);

        Assert.Equal(["1 | 2024-01-02 00:00:00"], await ReadBackAsync(connectionString, "to_char(n, 'YYYY-MM-DD HH24:MI:SS')"));
    }

    private static PostgreSqlDataWriter NewWriter(string connectionString)
        => new(connectionString,
            new PostgreSqlWriterOptions { Table = Table, Strategy = PostgreSqlWriteStrategy.Append },
            NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);

    private static async Task CreateTargetAsync(string connectionString, string columnType)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {Table}; CREATE TABLE {Table} (id INT, n {columnType})";
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> ReadBackAsync(string connectionString, string select)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT id || ' | ' || coalesce({select}, '') FROM {Table} ORDER BY id";
        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) rows.Add(reader.GetString(0));
        return rows;
    }
}
