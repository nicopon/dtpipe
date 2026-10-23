using DtPipe.Cli.DryRun;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A trace is laid out row-major, which only lines up while the pipeline is 1:1. Where a step
/// changes the row count the header says so, rather than leaving a blank cell that reads as
/// "this row was dropped" — the reading that made a windowed pipeline look like a total loss.
/// </summary>
public class CardinalityNoteTests
{
    [Fact]
    public void A_Step_That_Expands_Is_Marked()
        => DryRunRenderer.CardinalityNote([10, 30], stepIndex: 0)
            .Should().Contain("10").And.Contain("30").And.Contain("rows");

    [Fact]
    public void A_Step_That_Aggregates_Is_Marked()
        => DryRunRenderer.CardinalityNote([10, 3], stepIndex: 0)
            .Should().Contain("10").And.Contain("3").And.Contain("rows");

    [Fact]
    public void A_One_To_One_Step_Is_Not_Marked()
        => DryRunRenderer.CardinalityNote([10, 10], stepIndex: 0)
            .Should().BeEmpty("nothing changed, so there is nothing to explain");

    [Fact]
    public void Missing_Totals_Render_As_Before()
        => DryRunRenderer.CardinalityNote(null, stepIndex: 0).Should().BeEmpty();

    [Fact]
    public void A_Step_Beyond_The_Known_Totals_Is_Not_Marked()
        => DryRunRenderer.CardinalityNote([10, 30], stepIndex: 5).Should().BeEmpty();
}
