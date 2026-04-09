using DtPipe.Adapters.PostgreSQL;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;
using FluentAssertions;
using Apache.Arrow;

namespace DtPipe.Tests.Integration;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class AdoBinaryIntegrationTests : IAsyncLifetime
{
    private readonly DtPipe.Tests.Fixtures.GlobalDatabaseFixture _fixture;
    private string? _connectionString;

    public AdoBinaryIntegrationTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    public async ValueTask InitializeAsync()
    {
        _connectionString = _fixture.PostgresConnectionString;
        if (_connectionString == null) return;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS binary_test; CREATE TABLE binary_test (id INT, data BYTEA)";
        await cmd.ExecuteNonQueryAsync();

        cmd.CommandText = "INSERT INTO binary_test (id, data) VALUES (1, @d1), (2, NULL)";
        cmd.Parameters.AddWithValue("d1", new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
        await cmd.ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task PostgreSqlColumnarReader_ReadsBinaryDataCorrectly()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        // Arrange
        var reader = new PostgreSqlColumnarReader(_connectionString, "SELECT id, data FROM binary_test ORDER BY id");

        // Act
        await reader.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            batches.Add(batch);
        }

        // Assert
        batches.Should().HaveCount(1);
        var firstBatch = batches[0];
        firstBatch.Length.Should().Be(2);

        var binaryCol = firstBatch.Column(1) as BinaryArray;
        binaryCol.Should().NotBeNull();
        
        // Row 1: Valid data
        binaryCol!.IsNull(0).Should().BeFalse();
        binaryCol.GetBytes(0).ToArray().Should().Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });

        // Row 2: Null
        binaryCol.IsNull(1).Should().BeTrue();
    }
}
