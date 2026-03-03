using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Columnar.Fake;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using FluentAssertions;
using System.IO;

namespace DtPipe.Tests.Integration;

public class DecimalPersistenceTests : IAsyncDisposable
{
    private readonly string _duckDbPath;
    private readonly string _parquetPath;

    public DecimalPersistenceTests()
    {
        _duckDbPath = Path.Combine(Path.GetTempPath(), $"decimal_test_{Guid.NewGuid():N}.duckdb");
        _parquetPath = Path.Combine(Path.GetTempPath(), $"decimal_test_{Guid.NewGuid():N}.parquet");
    }

    public async ValueTask DisposeAsync()
    {
        try { if (File.Exists(_duckDbPath)) File.Delete(_duckDbPath); } catch { }
        try { if (File.Exists(_parquetPath)) File.Delete(_parquetPath); } catch { }
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task Decimal128_Persistence_DuckDB_Columnar_RoundTrip()
    {
        // 1. Setup Input Schema (without Decimal, it will be added as virtual)
        var columns = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false)
        };

        // 2. Generate Fake Data (using our refactored FakeDataTransformer)
        var transformer = new FakeDataTransformer(new FakeOptions
        {
            Seed = 42,
            Fake = new List<string> { "PreciseValue:finance.amount" }
        });

        var outputColumns = await transformer.InitializeAsync(columns);

        // Input batch with just Id
        var inputBatch = new RecordBatch(
            new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build(),
            new IArrowArray[] { new Int32Array.Builder().AppendRange(new[] { 1, 2, 3, 4, 5 }).Build() },
            5);

        var outputBatch = await transformer.TransformBatchAsync(inputBatch);
        outputBatch.Should().NotBeNull();

        // 3. Verify Batch has Decimal128Array
        var decimalCol = outputBatch.Column(1);
        decimalCol.Should().BeOfType<Decimal128Array>("FakeDataTransformer should produce Decimal128Array for decimal type via ArrowTypeMapper");

        var d128Array = (Decimal128Array)decimalCol;
        decimal expectedValue = (decimal)ArrowTypeMapper.GetValue(d128Array, 0)!;
        expectedValue.Should().NotBe(0);

        // 4. Write to DuckDB (Columnar path)
        var writerOptions = new DuckDbWriterOptions
        {
            Table = "DecimalTest",
            Strategy = DuckDbWriteStrategy.Recreate
        };
        var connectionString = $"Data Source={_duckDbPath}";

        await using (var writer = new DuckDbDataWriter(
            connectionString,
            writerOptions,
            NullLogger<DuckDbDataWriter>.Instance,
            DuckDbTypeConverter.Instance))
        {
            await writer.InitializeAsync(outputColumns);
            await writer.WriteRecordBatchAsync(outputBatch);
            await writer.CompleteAsync();
        }

        // 5. Verify in DuckDB
        await using (var conn = new DuckDBConnection(connectionString))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT PreciseValue FROM DecimalTest ORDER BY Id";
            using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            var dbValue = reader.GetDecimal(0);

            dbValue.Should().Be(expectedValue, "Value in DuckDB should match exactly the value in Arrow (lossless persistence)");
        }
#pragma warning restore CS8602 // Dereference of a possibly null reference.
    }

    [Fact]
    public async Task Decimal128_Persistence_Parquet_Columnar_Success()
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        // 1. Setup Input Schema (without Decimal, it will be added as virtual)
        var columns = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false)
        };

        // 2. Generate Fake Data
        var transformer = new FakeDataTransformer(new FakeOptions
        {
            Seed = 123,
            Fake = new List<string> { "PreciseValue:finance.amount" }
        });
        var outputColumns = await transformer.InitializeAsync(columns);

        var inputBatch = new RecordBatch(
            new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build(),
            new IArrowArray[] { new Int32Array.Builder().AppendRange(new[] { 1, 2, 3, 4, 5 }).Build() },
            5);
        var outputBatch = await transformer.TransformBatchAsync(inputBatch);
        outputBatch.Should().NotBeNull();

        // 3. Write to Parquet (via ArrowToParquetConverter)
        await using (var writer = new ParquetDataWriter(_parquetPath))
        {
            await writer.InitializeAsync(outputColumns);
            await writer.WriteRecordBatchAsync(outputBatch);
            await writer.CompleteAsync();
        }

        // 4. Check file existence and non-zero size
        new FileInfo(_parquetPath).Length.Should().BeGreaterThan(0);
    }
}
