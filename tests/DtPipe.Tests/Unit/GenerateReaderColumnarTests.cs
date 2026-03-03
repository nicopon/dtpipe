using Apache.Arrow;
using DtPipe.Adapters.Generate;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit;

public class GenerateReaderColumnarTests
{
    [Fact]
    public async Task ReadRecordBatchesAsync_YieldsCorrectData()
    {
        // Setup
        var options = new GenerateReaderOptions { RowCount = 100 };
        var reader = new GenerateReader("generate:100", "", options);
        await reader.OpenAsync();

        // Act
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            batches.Add(batch);
        }

        // Assert
        batches.Should().NotBeEmpty();
        long totalRows = batches.Sum(b => (long)b.Length);
        totalRows.Should().Be(100);

        var firstBatch = batches[0];
        firstBatch.Schema.GetFieldByIndex(0).Name.Should().Be("GenerateIndex");

        var array = firstBatch.Column(0) as Int64Array;
        array.Should().NotBeNull();
        array!.GetValue(0).Should().Be(0);
        array.GetValue(1).Should().Be(1);
    }
}
