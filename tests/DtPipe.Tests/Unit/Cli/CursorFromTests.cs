using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// <c>--cursor-from</c> is a value of the run it is given to: every <c>${{cursor://…}}</c> of that run resolves to it,
/// a job read by <c>--job</c> included, and the next run without it reads the default again.
/// </summary>
public sealed class CursorFromTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-cursor-from-" + Guid.NewGuid().ToString("N"));

    public CursorFromTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task A_Job_File_Starts_From_The_Value_Given_For_Its_Run()
    {
        var db = Path.Combine(_dir, "shop.db");
        var output = Path.Combine(_dir, "orders.csv");
        var job = Path.Combine(_dir, "fragment.yaml");
        Assert.Equal(0, await Program.Main(["-i", "generate:7", "--rename", "GenerateIndex:OrderId",
            "-o", $"sqlite:{db}", "--table", "orders", "--no-stats"]));
        File.WriteAllText(job, $$$"""
            orders:
              input: sqlite:{{{db}}}
              output: csv:{{{output}}}
              provider-options:
                sqlite:
                  query: SELECT * FROM orders WHERE OrderId > ${{cursor://|-1}} ORDER BY OrderId
            """);

        Assert.Equal(0, await Program.Main(["--job", job, "--cursor-from", "4", "--no-stats"]));
        Assert.Equal(2, File.ReadAllLines(output).Length - 1);

        Assert.Equal(0, await Program.Main(["--job", job, "--no-stats"]));
        Assert.Equal(7, File.ReadAllLines(output).Length - 1);
    }
}
