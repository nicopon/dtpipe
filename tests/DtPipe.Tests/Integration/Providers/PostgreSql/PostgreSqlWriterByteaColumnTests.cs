using DtPipe.Adapters.PostgreSQL;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// The PostgreSQL writer appending to a <c>bytea</c> column writes a binary source and refuses a
/// source that is not binary: the column named, the way to convert it given, and none of the rows of
/// the batch written.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlWriterByteaColumnTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
{
    private const string Table = "writer_bytea_column_test";

    public static TheoryData<Type, object, string> Refused => new()
    {
        { typeof(int), 5, "int32" },
        { typeof(bool), true, "bool" },
    };

    [Theory]
    [MemberData(nameof(Refused))]
    public async Task A_Non_Binary_Source_Is_Refused_Naming_The_Column_And_The_Way_To_Convert_It(
        Type sourceType, object value, string arrowName)
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", sourceType, true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, value], [2, value]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.WriteRecordBatchAsync(batch));

        Assert.Contains($"Column 'n' is bytea and the source sends {arrowName}", ex.Message);
        Assert.Contains("convert it in the query to a binary type", ex.Message);
        Assert.DoesNotContain("CAST(", ex.Message);
        Assert.Empty(await ReadBackAsync(connectionString));
    }

    [Fact]
    public async Task A_Binary_Source_Is_Written()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(byte[]), true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, new byte[] { 0x61, 0x62, 0x63 }], [2, null]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(batch);

        Assert.Equal(["1 | \\x616263", "2 | "], await ReadBackAsync(connectionString));
    }

    [Fact]
    public async Task A_Text_Source_Is_Written_As_Its_Utf8_Bytes()
    {
        if (!DockerHelper.IsAvailable() || fixture.PostgresConnectionString is not { } connectionString) return;
        await CreateTargetAsync(connectionString);

        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false), new("n", typeof(string), true) };
        using var batch = TestBatchBuilder.FromRows(columns, [1, "abc"]);

        await using var writer = NewWriter(connectionString);
        await writer.InitializeAsync(columns);
        await writer.WriteRecordBatchAsync(batch);

        Assert.Equal(["1 | \\x616263"], await ReadBackAsync(connectionString));
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
