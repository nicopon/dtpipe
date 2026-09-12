using DtPipe.Adapters.Csv;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Writers;

/// <summary>
/// Zero rows is a result, not a failure: a filter or a SQL predicate that matches nothing still
/// has a schema to declare. These hold the two ends of that — what the writer leaves on disk, and
/// that the reader accepts it back.
/// </summary>
public class CsvEmptyResultTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"empty_{Guid.NewGuid()}.csv");

    public void Dispose()
    {
        if (File.Exists(_path)) File.Delete(_path);
    }

    private static List<PipeColumnInfo> Columns() => new()
    {
        new("id", typeof(string), true),
        new("something", typeof(int), true)
    };

    [Fact]
    public async Task NoRowsWritten_StillLeavesTheHeader()
    {
        await using (var writer = new CsvDataWriter(_path))
        {
            await writer.InitializeAsync(Columns());
            await writer.CompleteAsync();
        }

        (await File.ReadAllTextAsync(_path)).TrimEnd().Should().Be("id,something");
    }

    /// <summary>
    /// The writer used to drop the buffered header along with the rows it was waiting for, and the
    /// 0-byte file that resulted is one this very reader refuses. dtpipe must read back what it wrote.
    /// </summary>
    [Fact]
    public async Task AnEmptyResult_IsReadableAgain()
    {
        await using (var writer = new CsvDataWriter(_path))
        {
            await writer.InitializeAsync(Columns());
            await writer.CompleteAsync();
        }

        var reader = new CsvStreamReader(_path, new CsvReaderOptions());
        await reader.OpenAsync();

        reader.Columns!.Select(c => c.Name).Should().Equal("id", "something");
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(100))
            for (int i = 0; i < batch.Length; i++) rows.Add(batch.Span[i]);
        rows.Should().BeEmpty();
    }

    [Fact]
    public async Task HeaderWithNoDataRows_IsZeroRowsNotAnError()
    {
        await File.WriteAllTextAsync(_path, "id,something\n");

        var reader = new CsvStreamReader(_path, new CsvReaderOptions());
        await reader.OpenAsync();

        reader.Columns.Should().HaveCount(2);
    }

    /// <summary>
    /// Zero bytes carries no header, so there are no column names and no schema to hand on. That
    /// stays a refusal — but one that says which of the two empty files it is looking at.
    /// </summary>
    [Fact]
    public async Task ZeroBytes_IsRefusedAndSaysWhy()
    {
        await File.WriteAllTextAsync(_path, string.Empty);

        var reader = new CsvStreamReader(_path, new CsvReaderOptions());
        var act = async () => await reader.OpenAsync();

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*no header line*").WithMessage("*declares no columns*");
    }
}
