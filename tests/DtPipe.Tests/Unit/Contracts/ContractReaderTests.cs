using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Contracts;
using Xunit;

namespace DtPipe.Tests.Unit.Contracts;

/// <summary>
/// The source side of <c>--from-contract</c>: a reader that publishes a schema and no rows.
/// </summary>
public class ContractReaderTests
{
    private static DataContract Sample()
    {
        var schema = new Schema(
        [
            new Field("Id", Int32Type.Default, nullable: false),
            new Field("Label", StringType.Default, nullable: true),
        ], null);
        return DataContract.FromSchema(schema, "batch", "VerbScanOnly", "main");
    }

    [Fact]
    public async Task Publishes_The_Contract_Schema()
    {
        var reader = new ContractStreamReader(Sample());
        await reader.OpenAsync();

        Assert.NotNull(reader.Columns);
        Assert.Equal(["Id", "Label"], reader.Columns!.Select(c => c.Name).ToArray());
        Assert.Equal([typeof(int), typeof(string)], reader.Columns.Select(c => c.ClrType).ToArray());
        Assert.Equal([false, true], reader.Columns.Select(c => c.IsNullable).ToArray());
    }

    /// <summary>
    /// No rows, on both surfaces.
    /// </summary>
    /// <remarks>
    /// A reader that yielded one empty batch instead would put a real batch through the engine and
    /// the contract check would start reporting on data it invented. Ending the stream is what
    /// keeps the check a statement about the SHAPE.
    /// </remarks>
    [Fact]
    public async Task Yields_No_Batches()
    {
        var reader = new ContractStreamReader(Sample());
        await reader.OpenAsync();

        Assert.Empty(await reader.ReadRecordBatchesAsync().ToListAsync());
        Assert.Empty(await reader.ReadBatchesAsync(1024).ToListAsync());
    }

    /// <summary>
    /// The factory never claims a path by looking at it — the router hands it over from the flag.
    /// </summary>
    /// <remarks>
    /// <c>CanHandle</c> returning true for anything would let a contract path into the
    /// <c>{component}[+{variant}]:</c> grammar, where <c>C:\contracts\orders.json</c> reads as the
    /// component <c>C</c>.
    /// </remarks>
    [Theory]
    [InlineData("contracts/orders.json")]
    [InlineData(@"C:\contracts\orders.json")]
    [InlineData("contract:orders.json")]
    public void Never_Claims_A_String(string candidate)
        => Assert.False(new ContractReaderFactory("ignored").CanHandle(candidate));

    [Fact]
    public void Missing_File_Names_The_Flag_That_Produces_One()
    {
        var factory = new ContractReaderFactory(Path.Combine(Path.GetTempPath(), $"absent-{Guid.NewGuid():N}.json"));
        var ex = Assert.Throws<InvalidOperationException>(() => factory.Create(new DtPipe.Core.Options.OptionsRegistry()));
        Assert.Contains("--contract-save", ex.Message);
    }
}

internal static class AsyncEnumerableTestExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var items = new List<T>();
        await foreach (var item in source) items.Add(item);
        return items;
    }
}
