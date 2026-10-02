namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A job cut into a producer/consumer pair by the real <c>dtpipe split</c>, plus the witness CSV a
/// single unsplit run of the same job produces — the comparison every round-trip guard checks
/// against. <see cref="ConsumerJobPath"/> is rewritten to write <see cref="SplitCsvPath"/> rather
/// than <see cref="WitnessCsvPath"/>, which the exported job would otherwise reuse.
/// </summary>
internal sealed class SplitFixture : IDisposable
{
    public required string ProducerJobPath { get; init; }
    public required string ConsumerJobPath { get; init; }
    public required string WitnessCsvPath { get; init; }
    public required string SplitCsvPath { get; init; }
    public required int RowCount { get; init; }
    private DirectoryInfo Dir { get; init; } = null!;

    public static SplitFixture Create(int rowCount, int cutAt = 0, params string[] extraArgs)
    {
        var dir = Directory.CreateTempSubdirectory("pnode-guard-");
        var witnessCsv = Path.Combine(dir.FullName, "witness.csv");
        var splitCsv = Path.Combine(dir.FullName, "split.csv");
        var jobPath = Path.Combine(dir.FullName, "full.yaml");

        var generateInput = $"generate:{rowCount}";
        string[] readerArgs = ["--input", generateInput, .. extraArgs, "--output", $"csv:{witnessCsv}"];
        DtPipeCli.Run(readerArgs);
        DtPipeCli.Run([.. readerArgs, "--export-job", jobPath]);

        var outPrefix = Path.Combine(dir.FullName, "frag");
        DtPipeCli.Run("split", jobPath, "--at", cutAt.ToString(), "--out", outPrefix, "--acknowledge");

        var producerPath = $"{outPrefix}-producer.yaml";
        var consumerPath = $"{outPrefix}-consumer.yaml";
        File.WriteAllText(consumerPath, File.ReadAllText(consumerPath).Replace(witnessCsv, splitCsv));

        return new SplitFixture
        {
            ProducerJobPath = producerPath,
            ConsumerJobPath = consumerPath,
            WitnessCsvPath = witnessCsv,
            SplitCsvPath = splitCsv,
            RowCount = rowCount,
            Dir = dir,
        };
    }

    public void Dispose() => Dir.Delete(recursive: true);
}
