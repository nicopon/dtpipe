namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A three-fragment chain (A produces, B relays untouched, C consumes), plus the witness CSV a
/// single unsplit run of the same source produces. A and C are the producer/consumer pair
/// <see cref="SplitFixture"/> builds; the middle fragment is a plain hand-written
/// <c>arrow:- -&gt; arrow:-</c> passthrough between them. <c>extraArgs</c> are the stages A runs and
/// the witness runs too; C only writes <see cref="SplitCsvPath"/>, so the two CSV files differ by
/// path alone. A chain exercises three fragments, a middle one dying mid-flow, and the other two
/// reported as consequences.
/// </summary>
internal sealed class ChainFixture : IDisposable
{
    public required string FragmentAJobPath { get; init; }
    public required string FragmentBJobPath { get; init; }
    public required string FragmentCJobPath { get; init; }
    public required string WitnessCsvPath { get; init; }
    public required string SplitCsvPath { get; init; }
    public required int RowCount { get; init; }
    private DirectoryInfo Dir { get; init; } = null!;

    public static ChainFixture Create(int rowCount, params string[] extraArgs)
    {
        var dir = Directory.CreateTempSubdirectory("pnode-chain-");
        var witnessCsv = Path.Combine(dir.FullName, "witness.csv");
        var splitCsv = Path.Combine(dir.FullName, "split.csv");
        var fragmentAPath = Path.Combine(dir.FullName, "frag-a.yaml");
        var fragmentCPath = Path.Combine(dir.FullName, "frag-c.yaml");

        string[] producerArgs = ["--input", $"generate:{rowCount}", .. extraArgs];
        DtPipeCli.Run([.. producerArgs, "--output", $"csv:{witnessCsv}"]);
        DtPipeCli.Run([.. producerArgs, "--output", "arrow:-", "--export-job", fragmentAPath]);
        DtPipeCli.Run("--input", "arrow:-", "--output", $"csv:{splitCsv}", "--export-job", fragmentCPath);

        var fragmentBPath = Path.Combine(dir.FullName, "frag-b.yaml");
        File.WriteAllText(fragmentBPath, "main:\n  input: arrow:-\n  output: arrow:-\n  batch-size: 32768\n  sampling-rate: 1\n");

        return new ChainFixture
        {
            FragmentAJobPath = fragmentAPath,
            FragmentBJobPath = fragmentBPath,
            FragmentCJobPath = fragmentCPath,
            WitnessCsvPath = witnessCsv,
            SplitCsvPath = splitCsv,
            RowCount = rowCount,
            Dir = dir,
        };
    }

    public void Dispose() => Dir.Delete(recursive: true);
}
