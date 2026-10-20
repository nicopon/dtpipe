using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>A host that cannot connect, or whose announcement is refused, tries again at a growing and bounded interval.</summary>
public sealed class ConnectRetryTests
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(5, 15)]
    [InlineData(500, 15)]
    public void The_Wait_Doubles_From_One_Second_And_Stops_At_The_Cap(int failures, int seconds) =>
        Assert.Equal(TimeSpan.FromSeconds(seconds), NodeAgent.RetryDelay(failures));
}
