namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A three-fragment chain (A produces, B relays untouched, C consumes), plus the witness CSV a
/// single unsplit run of the same source produces. <c>dtpipe split</c> only cuts a job into a
/// producer/consumer pair, so the middle fragment is a plain hand-written <c>arrow:- -&gt; arrow:-</c>
/// passthrough rather than a second cut - there is no transformer stage to cut it at either side of.
/// Used instead of a join: <see cref="PipelineNode.StartAsync"/> refuses more than one inbound edge
/// per node, so a fragment with two producers feeding it is not runnable yet. A chain still exercises
/// three fragments, a middle one dying mid-flow, and the other two reported as consequences.
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
        var jobPath = Path.Combine(dir.FullName, "full.yaml");

        string[] readerArgs = ["--input", $"generate:{rowCount}", .. extraArgs, "--output", $"csv:{witnessCsv}"];
        DtPipeCli.Run(readerArgs);
        DtPipeCli.Run([.. readerArgs, "--export-job", jobPath]);

        var outPrefix = Path.Combine(dir.FullName, "frag");
        DtPipeCli.Run("split", jobPath, "--at", "0", "--out", outPrefix, "--acknowledge");

        var fragmentAPath = $"{outPrefix}-producer.yaml";
        var fragmentCPath = $"{outPrefix}-consumer.yaml";
        File.WriteAllText(fragmentCPath, File.ReadAllText(fragmentCPath).Replace(witnessCsv, splitCsv));

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
