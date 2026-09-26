namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// Two producers merged by a consumer with two inbound edges - isolates J2's own plumbing (a
/// second, named-pipe edge alongside the primary stdin one) from any join semantics, which
/// <see cref="JoinFixture"/> covers instead. The consumer job is hand-built via <c>--export-job</c>
/// rather than derived from <c>dtpipe split</c>, which only ever cuts a single linear job at one
/// point and never produces a two-producer topology.
/// </summary>
internal sealed class MergeFixture : IDisposable
{
    public required string ProducerAJobPath { get; init; }
    public required string ProducerBJobPath { get; init; }
    public required string ConsumerJobPath { get; init; }
    public required string WitnessCsvPath { get; init; }
    public required string SplitCsvPath { get; init; }
    private DirectoryInfo Dir { get; init; } = null!;

    public static MergeFixture Create(int rowCountA, int rowCountB)
    {
        var dir = Directory.CreateTempSubdirectory("pnode-merge-");
        var witnessCsv = Path.Combine(dir.FullName, "witness.csv");
        var splitCsv = Path.Combine(dir.FullName, "split.csv");

        var producerAJobPath = Path.Combine(dir.FullName, "producer-a.yaml");
        DtPipeCli.Run("--input", $"generate:{rowCountA}", "--output", "arrow:-", "--export-job", producerAJobPath);

        var producerBJobPath = Path.Combine(dir.FullName, "producer-b.yaml");
        DtPipeCli.Run("--input", $"generate:{rowCountB}", "--output", "arrow:-", "--export-job", producerBJobPath);

        DtPipeCli.Run(
            "--input", $"generate:{rowCountA}", "--alias", "a",
            "--input", $"generate:{rowCountB}", "--alias", "b",
            "--from", "a,b", "--merge", "--output", $"csv:{witnessCsv}", "--no-stats");

        var consumerJobPath = Path.Combine(dir.FullName, "consumer.yaml");
        DtPipeCli.Run(
            "--input", "arrow:-", "--alias", "a",
            "--input", "arrow:-", "--alias", "b",
            "--from", "a,b", "--merge", "--output", $"csv:{splitCsv}",
            "--export-job", consumerJobPath);

        return new MergeFixture
        {
            ProducerAJobPath = producerAJobPath,
            ProducerBJobPath = producerBJobPath,
            ConsumerJobPath = consumerJobPath,
            WitnessCsvPath = witnessCsv,
            SplitCsvPath = splitCsv,
            Dir = dir,
        };
    }

    public void Dispose() => Dir.Delete(recursive: true);
}
