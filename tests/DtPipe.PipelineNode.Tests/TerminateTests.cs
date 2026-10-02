using System.Diagnostics;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

public class TerminateTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Neither node nor child process is touched here — only the transfer itself is declared failed,
    /// the gesture a coordinator makes once it decides a peer is gone. What this guard owns: that
    /// alone must be enough for the node to turn the failure into a local stream rupture, and for the
    /// consumer's own dtpipe child — reading a now-truncated arrow: stream — to exit 1, the same way
    /// it does for any other truncated source. A control run with the terminate call removed confirmed
    /// the two guards below (round trip, this one) don't pass by coincidence.
    /// </summary>
    [Fact]
    public async Task TerminatingALiveTransfer_ConsumerExitsOne()
    {
        using var fixture = SplitFixture.Create(rowCount: 1_000_000);
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

        var sw = Stopwatch.StartNew();
        await host.TransferTerminator.TerminateAsync(transferId).WaitAsync(Bound);

        var consumerExit = await consumer.RunToCompletionAsync().WaitAsync(Bound);
        sw.Stop();
        _ = await producer.RunToCompletionAsync().WaitAsync(Bound); // drains the producer's own relay task; not this guard's assertion

        Assert.Equal(1, consumerExit);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3), $"terminate-to-exit took {sw.ElapsedMilliseconds}ms");
        Assert.True(
            consumer.RowCounts["main"] < fixture.RowCount,
            $"expected a mid-transfer truncation, got {consumer.RowCounts["main"]} of {fixture.RowCount} rows");
    }
}
