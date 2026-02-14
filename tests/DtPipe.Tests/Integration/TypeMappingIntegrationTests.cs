using DtPipe.Adapters.Sqlite;
using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Microsoft.Data.Sqlite;
using DuckDB.NET.Data;

namespace DtPipe.Tests.Integration;

public class TypeMappingIntegrationTests
{
    private readonly List<PipeColumnInfo> _fullTypeSchema = new()
    {
        new PipeColumnInfo("COL_INT", typeof(int), false),
        new PipeColumnInfo("COL_LONG", typeof(long), false),
        new PipeColumnInfo("COL_SHORT", typeof(short), false),
        new PipeColumnInfo("COL_BYTE", typeof(byte), false),
        new PipeColumnInfo("COL_BOOL", typeof(bool), false),
        new PipeColumnInfo("COL_FLOAT", typeof(float), false),
        new PipeColumnInfo("COL_DOUBLE", typeof(double), false),
        new PipeColumnInfo("COL_DECIMAL", typeof(decimal), false),
        new PipeColumnInfo("COL_STRING", typeof(string), true),
        new PipeColumnInfo("COL_DATETIME", typeof(DateTime), false),
        new PipeColumnInfo("COL_GUID", typeof(Guid), false),
        new PipeColumnInfo("COL_BLOB", typeof(byte[]), true)
    };

    private object?[] GetSampleRow() => new object?[]
    {
        123,
        9876543210L,
        (short)32000,
        (byte)255,
        true,
        1.23f,
        4.5678,
        1234.56m,
        "Hello World",
        new DateTime(2025, 1, 1, 12, 0, 0),
        Guid.Parse("f47ac10b-58cc-4372-a567-0e02b2c3d479"),
        new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }
    };

    [Fact]
    public async Task Sqlite_TypeMapping_RoundTrip()
    {
        var dbPath = $"type_test_sqlite_{Guid.NewGuid()}.db";
        var connectionString = $"Data Source={dbPath}";
        try
        {
            var options = new SqliteWriterOptions { Table = "TYPE_TEST", Strategy = SqliteWriteStrategy.Recreate };
            var writer = new SqliteDataWriter(connectionString, options, NullLogger<SqliteDataWriter>.Instance, SqliteTypeConverter.Instance);

            await writer.InitializeAsync(_fullTypeSchema, CancellationToken.None);

            var row = GetSampleRow();
            await writer.WriteBatchAsync(new List<object?[]> { row }, CancellationToken.None);
            await writer.CompleteAsync(CancellationToken.None);

            // Verify
            using var conn = new SqliteConnection(connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM TYPE_TEST";
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());

            Assert.Equal(123, reader.GetInt32(0));
            Assert.Equal(9876543210L, reader.GetInt64(1));
            Assert.Equal(32000, reader.GetInt16(2));
            Assert.Equal(255, reader.GetByte(3));
            Assert.Equal(1, reader.GetInt32(4)); // SQLite bool is 1
            Assert.Equal(1.23, reader.GetDouble(5), 2);
            Assert.Equal(4.5678, reader.GetDouble(6), 4);
            Assert.Equal(1234.56m, reader.GetDecimal(7));
            Assert.Equal("Hello World", reader.GetString(8));
            // SQLite DateTime is stored as string TEXT
            Assert.Equal("2025-01-01 12:00:00", reader.GetString(9));
            Assert.Equal("f47ac10b-58cc-4372-a567-0e02b2c3d479", reader.GetString(10), ignoreCase: true);

            var blob = (byte[])reader.GetValue(11);
            Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, blob);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Fact]
    public async Task DuckDb_TypeMapping_RoundTrip()
    {
        var dbPath = $"type_test_duck_{Guid.NewGuid()}.db";
        var connectionString = $"Data Source={dbPath}";
        try
        {
            var options = new DuckDbWriterOptions { Table = "TYPE_TEST", Strategy = DuckDbWriteStrategy.Recreate };
            var writer = new DuckDbDataWriter(connectionString, options, NullLogger<DuckDbDataWriter>.Instance, DuckDbTypeConverter.Instance);

            await writer.InitializeAsync(_fullTypeSchema, CancellationToken.None);

            var row = GetSampleRow();
            await writer.WriteBatchAsync(new List<object?[]> { row }, CancellationToken.None);
            await writer.CompleteAsync(CancellationToken.None);

            // Verify
            using var conn = new DuckDBConnection(connectionString);
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM TYPE_TEST";
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());

            Assert.Equal(123, reader.GetInt32(0));
            Assert.Equal(9876543210L, reader.GetInt64(1));
            Assert.Equal(32000, reader.GetInt16(2));
            Assert.Equal((byte)255, reader.GetByte(3));
            Assert.True(reader.GetBoolean(4));
            Assert.Equal(1.23f, reader.GetFloat(5), 2);
            Assert.Equal(4.5678, reader.GetDouble(6), 4);
            Assert.Equal(1234.56m, reader.GetDecimal(7));
            Assert.Equal("Hello World", reader.GetString(8));
            Assert.Equal(new DateTime(2025, 1, 1, 12, 0, 0), reader.GetDateTime(9));
            Assert.Equal(Guid.Parse("f47ac10b-58cc-4372-a567-0e02b2c3d479"), reader.GetFieldValue<Guid>(10));

            var blobValue = reader.GetValue(11);
            byte[] actualBlob;
            if (blobValue is System.IO.Stream stream)
            {
                using var ms = new System.IO.MemoryStream();
                stream.CopyTo(ms);
                actualBlob = ms.ToArray();
            }
            else
            {
                actualBlob = (byte[])blobValue;
            }
            Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, actualBlob);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }
}
