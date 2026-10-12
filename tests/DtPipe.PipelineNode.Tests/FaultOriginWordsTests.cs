using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Xunit;
using CoordinatorOrigin = DtPipe.Coordinator.FaultOrigin;
using NodeOrigin = DtPipe.PipelineNode.FaultOrigin;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// <c>DtPipe.PipelineNode</c> and <c>DtPipe.Coordinator</c> each declare where a fault began, and neither references
/// the other. This is the one place that sees both with the words a report carries between them.
/// </summary>
public sealed class FaultOriginWordsTests
{
    private static readonly string[] Words = [ExitOrigins.Local, ExitOrigins.Remote];

    [Fact]
    public void The_Node_And_The_Coordinator_Declare_The_Origins_The_Wire_Has_Words_For()
    {
        Assert.Equal(Words.Order(StringComparer.Ordinal), Enum.GetNames<NodeOrigin>().Order(StringComparer.Ordinal));
        Assert.Equal(Words.Order(StringComparer.Ordinal), Enum.GetNames<CoordinatorOrigin>().Order(StringComparer.Ordinal));
    }

    [Fact]
    public void A_Replica_Reports_Each_Origin_As_Its_Name_And_A_Run_With_None_As_Local()
    {
        foreach (var origin in Enum.GetValues<NodeOrigin>())
            Assert.Equal(origin.ToString(), ReplicaRuns.OriginWord(origin));
        Assert.Equal(ExitOrigins.Local, ReplicaRuns.OriginWord(null));
    }
}
