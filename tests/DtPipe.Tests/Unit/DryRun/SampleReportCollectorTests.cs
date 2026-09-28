using DtPipe.DryRun;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A single-branch job runs on the linear path, whose report carries no alias. The collector files
/// it under the name the job gives that branch, so a caller looking it up by that name finds it.
/// </summary>
public class SampleReportCollectorTests
{
	private static SampleReport Report(string? alias) =>
		new(new SampleRun([], 0, 0), [], [], null, null, BranchAlias: alias);

	[Fact]
	public void ALinearReport_IsFiledUnderTheBranchTheJobNames()
	{
		var collector = new SampleReportCollector { Enabled = true, LinearBranch = "people" };

		collector.Publish(null, Report(null));

		collector.Reports.Keys.Should().Equal("people");
	}

	[Fact]
	public void ADagReport_KeepsItsOwnAlias()
	{
		var collector = new SampleReportCollector { Enabled = true, LinearBranch = "people" };

		collector.Publish("orders", Report("orders"));

		collector.Reports.Keys.Should().Equal("orders");
	}

	[Fact]
	public void Clear_ForgetsTheLinearBranch_SoTheNextRunCannotInheritIt()
	{
		var collector = new SampleReportCollector { Enabled = true, LinearBranch = "people" };

		collector.Clear();
		collector.Publish(null, Report(null));

		collector.Reports.Keys.Should().Equal("main");
	}
}
