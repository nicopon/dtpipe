namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// Two producers joined on a shared key by a consumer with two inbound edges - the join C4's own
/// design motivated (<c>--from X --ref Y --sql</c>) but that could not run distributed until J2
/// lifted <see cref="PipelineNode"/>'s one-inbound-edge cap. Both producers emit <c>generate:</c>
/// rows (a deterministic <c>GenerateIndex</c> column, no database needed), joined 1:1 on it so the
/// witness needs no fixture data of its own. The consumer job is hand-built via
/// <c>--export-job</c> rather than derived from <c>dtpipe split</c>, which only ever cuts a single
/// linear job at one point and never produces a two-producer join.
/// </summary>
internal sealed class JoinFixture : IDisposable
{
    private const string JoinSql =
        "SELECT o.GenerateIndex AS id, c.GenerateIndex AS customer_id " +
        "FROM orders o JOIN customers c ON o.GenerateIndex = c.GenerateIndex";

    public required string ProducerOrdersJobPath { get; init; }
    public required string ProducerCustomersJobPath { get; init; }
    public required string ConsumerJobPath { get; init; }
    public required string WitnessCsvPath { get; init; }
    public required string SplitCsvPath { get; init; }
    public required int RowCount { get; init; }
    private DirectoryInfo Dir { get; init; } = null!;

    public static JoinFixture Create(int rowCount, params string[] producerExtraArgs)
    {
        var dir = Directory.CreateTempSubdirectory("pnode-join-");
        var witnessCsv = Path.Combine(dir.FullName, "witness.csv");
        var splitCsv = Path.Combine(dir.FullName, "split.csv");

        var producerOrdersJobPath = Path.Combine(dir.FullName, "producer-orders.yaml");
        DtPipeCli.Run([
            "--input", $"generate:{rowCount}", .. producerExtraArgs, "--output", "arrow:-",
            "--export-job", producerOrdersJobPath]);

        var producerCustomersJobPath = Path.Combine(dir.FullName, "producer-customers.yaml");
        DtPipeCli.Run([
            "--input", $"generate:{rowCount}", .. producerExtraArgs, "--output", "arrow:-",
            "--export-job", producerCustomersJobPath]);

        DtPipeCli.Run([
            "--input", $"generate:{rowCount}", .. producerExtraArgs, "--alias", "orders",
            "--input", $"generate:{rowCount}", .. producerExtraArgs, "--alias", "customers",
            "--from", "orders", "--ref", "customers", "--sql", JoinSql, "--alias", "joined",
            "--output", $"csv:{witnessCsv}", "--no-stats"]);

        var consumerJobPath = Path.Combine(dir.FullName, "consumer.yaml");
        DtPipeCli.Run(
            "--input", "arrow:-", "--alias", "orders",
            "--input", "arrow:-", "--alias", "customers",
            "--from", "orders", "--ref", "customers", "--sql", JoinSql, "--alias", "joined",
            "--output", $"csv:{splitCsv}",
            "--export-job", consumerJobPath);

        return new JoinFixture
        {
            ProducerOrdersJobPath = producerOrdersJobPath,
            ProducerCustomersJobPath = producerCustomersJobPath,
            ConsumerJobPath = consumerJobPath,
            WitnessCsvPath = witnessCsv,
            SplitCsvPath = splitCsv,
            RowCount = rowCount,
            Dir = dir,
        };
    }

    public void Dispose() => Dir.Delete(recursive: true);
}
