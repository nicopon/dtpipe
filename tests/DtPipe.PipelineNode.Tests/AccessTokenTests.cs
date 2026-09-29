using DtPipe.PipelineNode.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A hub in production mode identifies every client by a bearer token, so a
/// <see cref="PipelineNode"/> can only reach it through <see cref="PipelineNodeOptions.AccessTokenProvider"/>.
/// </summary>
public class AccessTokenTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task TwoNodesWithTokenProviders_RoundTripThroughAJwtHub()
    {
        using var fixture = SplitFixture.Create(rowCount: 5_000);
        var jwt = new TestJwt();
        await using var host = await NodeTestHost.StartAsync(jwt);

        var producerCalls = 0;
        await using var producer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            AccessTokenProvider = () =>
            {
                Interlocked.Increment(ref producerCalls);
                return Task.FromResult(jwt.Mint("producer", "test", TimeSpan.FromHours(1)));
            },
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ProducerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
        });
        await using var consumer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            AccessTokenProvider = () => Task.FromResult(jwt.Mint("consumer", "test", TimeSpan.FromHours(1))),
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

        Assert.Equal([0, 0], exitCodes);
        Assert.Equal(fixture.RowCount, consumer.RowCounts["main"]);
        Assert.True(producerCalls > 0, "the hub was reached without asking the provider for a token");
    }

    /// <summary>The negative control: the same hub refuses a node that presents no token, so the
    /// round trip above is carried by the provider and not by an open hub.</summary>
    [Fact]
    public async Task ANodeWithoutATokenProvider_IsRefusedByAJwtHub()
    {
        using var fixture = SplitFixture.Create(rowCount: 10);
        await using var host = await NodeTestHost.StartAsync(new TestJwt());

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var node = await PipelineNode.StartAsync(new PipelineNodeOptions
            {
                HubUrl = host.Url,
                DtPipeExecutable = DtPipeExecutableLocator.Path,
                FragmentJobPath = fixture.ProducerJobPath,
                Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
            });
        }).WaitAsync(TimeSpan.FromSeconds(60));
    }

    /// <summary>A token the hub cannot verify is refused as well: the hub checks the signature, it
    /// does not take the provider's word for the node's group.</summary>
    [Fact]
    public async Task ANodeWithATokenSignedByAnotherKey_IsRefusedByAJwtHub()
    {
        using var fixture = SplitFixture.Create(rowCount: 10);
        await using var host = await NodeTestHost.StartAsync(new TestJwt());
        var stranger = new TestJwt();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var node = await PipelineNode.StartAsync(new PipelineNodeOptions
            {
                HubUrl = host.Url,
                AccessTokenProvider = () => Task.FromResult(stranger.Mint("producer", "test", TimeSpan.FromHours(1))),
                DtPipeExecutable = DtPipeExecutableLocator.Path,
                FragmentJobPath = fixture.ProducerJobPath,
                Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
            });
        }).WaitAsync(TimeSpan.FromSeconds(60));
    }
}
