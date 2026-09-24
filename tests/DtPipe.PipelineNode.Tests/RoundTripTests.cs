using DtPipe.PipelineNode.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

public class RoundTripTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task TwoNodesThroughAHub_MatchTheUnsplitWitness()
    {
        // Large enough that the Arrow IPC stream spans multiple 64 KB reads, so the row counter's
        // header-split-across-chunks path is actually exercised (500 rows fit in a single read).
        using var fixture = SplitFixture.Create(rowCount: 200_000);
        await using var host = await NodeTestHost.StartAsync();

        await using var producer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ProducerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
        });
        await using var consumer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ConsumerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Inbound)],
        });

        var transferId = await host.TransferInitiator.InitTransferAsync(
            producer.ClientId, consumer.ClientId, batchSize: 8, timeoutMs: 20_000);

        await Task.WhenAll(
            producer.WireAsync("main", transferId).WaitAsync(Bound),
            consumer.WireAsync("main", transferId).WaitAsync(Bound));

        var exitCodes = await Task.WhenAll(
            producer.RunToCompletionAsync().WaitAsync(Bound),
            consumer.RunToCompletionAsync().WaitAsync(Bound));

        Assert.Equal(0, exitCodes[0]);
        Assert.Equal(0, exitCodes[1]);
        Assert.Equal(fixture.RowCount, producer.RowCounts["main"]);
        Assert.Equal(fixture.RowCount, consumer.RowCounts["main"]);

        var witnessRows = File.ReadAllLines(fixture.WitnessCsvPath).Order(StringComparer.Ordinal);
        var splitRows = File.ReadAllLines(fixture.SplitCsvPath).Order(StringComparer.Ordinal);
        Assert.Equal(witnessRows, splitRows);
    }
}
