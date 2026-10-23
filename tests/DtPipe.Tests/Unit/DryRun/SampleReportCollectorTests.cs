using DtPipe.DryRun;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A single-branch job runs on the linear path, whose report carries no alias. The run files it under
/// the name the job gives that branch, so a caller looking it up by that name finds it. Only a run
/// that armed a collection receives anything.
/// </summary>
public class SampleReportCollectorTests
{
    private static SampleReport Report(string? alias) =>
        new(new SampleRun([], 0, 0), [], [], null, null, BranchAlias: alias);

    [Fact]
    public void ALinearReport_IsFiledUnderTheBranchTheJobNames()
    {
        var collector = new SampleReportCollector();
        using var scope = collector.Begin();
        collector.LinearBranch = "people";

        collector.Publish(null, Report(null));

        scope.Run.Reports.Keys.Should().Equal("people");
    }

    [Fact]
    public void ADagReport_KeepsItsOwnAlias()
    {
        var collector = new SampleReportCollector();
        using var scope = collector.Begin();
        collector.LinearBranch = "people";

        collector.Publish("orders", Report("orders"));

        scope.Run.Reports.Keys.Should().Equal("orders");
    }

    [Fact]
    public void ANewRun_CannotInheritTheLinearBranchOfAnother()
    {
        var collector = new SampleReportCollector();
        using (collector.Begin()) collector.LinearBranch = "people";

        using var next = collector.Begin();
        collector.Publish(null, Report(null));

        next.Run.Reports.Keys.Should().Equal("main");
    }

    [Fact]
    public void WithoutARun_NothingIsCollected()
    {
        var collector = new SampleReportCollector();

        collector.Publish("a", Report("a"));
        collector.RecordCheckpoint("a", "key");
        collector.LinearBranch = "a";

        collector.LinearBranch.Should().BeNull();
        using var scope = collector.Begin();
        scope.Run.Reports.Should().BeEmpty();
        scope.Run.Checkpoints.Should().BeEmpty();
    }
}
