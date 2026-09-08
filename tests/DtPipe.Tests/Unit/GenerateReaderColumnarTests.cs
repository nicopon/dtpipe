using Apache.Arrow;
using DtPipe.Adapters.Generate;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit;

public class GenerateReaderColumnarTests
{
    /// <summary>
    /// This reader takes a row count and nothing else. Dropping what it could not parse fell back
    /// to the default 100: a recorded session wrote 'generate:id', meaning "an id column", and got
    /// a hundred rows of something it never asked for with nothing said.
    /// </summary>
    [Theory]
    [InlineData("id")]
    [InlineData("abc")]
    [InlineData("count=abc")]
    [InlineData("rate=soon")]
    public void A_Config_That_Is_Not_A_Row_Count_Is_Refused(string config)
    {
        var act = () => new GenerateReader(config, "", new GenerateReaderOptions());

        act.Should().Throw<InvalidOperationException>().WithMessage("*is not a row count*");
    }

    [Theory]
    [InlineData("250", 250)]
    [InlineData("2k", 2000)]
    [InlineData("count=7;rate=3", 7)]
    public void A_Config_That_Is_A_Row_Count_Binds(string config, long expected)
    {
        var options = new GenerateReaderOptions();
        _ = new GenerateReader(config, "", options);

        options.RowCount.Should().Be(expected);
    }

    [Fact]
    public async Task ReadRecordBatchesAsync_YieldsCorrectData()
    {
        // Setup
        var options = new GenerateReaderOptions { RowCount = 100 };
        // Selector-stripped form, as the descriptor receives it at runtime.
        var reader = new GenerateReader("100", "", options);
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
