using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public sealed class NodeLogForwarderTests
{
    [Fact]
    public void A_Forwarded_Line_Names_The_Category_Of_The_Logger_That_Wrote_It()
    {
        var forwarder = new NodeLogForwarder();
        var factory = forwarder.For(NullLoggerFactory.Instance, "n", fragment: null);

        factory.CreateLogger("TransportR.Client.Session").LogWarning("session lost");
        factory.CreateLogger("DtPipe.Other").LogInformation("hello");

        Assert.True(forwarder.Reader.TryRead(out var transport));
        Assert.Equal(("Warning", "TransportR.Client.Session"), (transport!.Level, transport.Category));
        Assert.True(LogCategories.IsTransport(transport.Category));
        Assert.True(forwarder.Reader.TryRead(out var other));
        Assert.Equal("DtPipe.Other", other!.Category);
        Assert.False(LogCategories.IsTransport(other.Category));
    }
}
