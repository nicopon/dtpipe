namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A producer/consumer pair of fragments meeting on <c>arrow:-</c>, plus the witness CSV a single
/// unsplit run of the same job produces — the comparison every round-trip guard checks against.
/// <c>extraArgs</c> are the stages the producer runs and the witness runs too; the consumer only
/// writes <see cref="SplitCsvPath"/>, so the two CSV files differ by path alone.
/// </summary>
internal sealed class SplitFixture : IDisposable
{
    public required string ProducerJobPath { get; init; }
    public required string ConsumerJobPath { get; init; }
    public required string WitnessCsvPath { get; init; }
    public required string SplitCsvPath { get; init; }
    public required int RowCount { get; init; }
    private DirectoryInfo Dir { get; init; } = null!;

    public static SplitFixture Create(int rowCount, params string[] extraArgs)
    {
        var dir = Directory.CreateTempSubdirectory("pnode-guard-");
        var witnessCsv = Path.Combine(dir.FullName, "witness.csv");
        var splitCsv = Path.Combine(dir.FullName, "split.csv");
        var producerPath = Path.Combine(dir.FullName, "frag-producer.yaml");
        var consumerPath = Path.Combine(dir.FullName, "frag-consumer.yaml");

        string[] producerArgs = ["--input", $"generate:{rowCount}", .. extraArgs];
        DtPipeCli.Run([.. producerArgs, "--output", $"csv:{witnessCsv}"]);
        DtPipeCli.Run([.. producerArgs, "--output", "arrow:-", "--export-job", producerPath]);
        DtPipeCli.Run("--input", "arrow:-", "--output", $"csv:{splitCsv}", "--export-job", consumerPath);

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
