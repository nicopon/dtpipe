using DtPipe.Adapters.Shared.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// Recreate drops the table and rebuilds it from the source, so the warning is the only trace of
/// what the old table had that the new one does not.
/// </summary>
public class RecreateDivergenceTests
{
    [Fact]
    public void A_Table_That_Matches_The_Source_Is_Not_Worth_A_Warning()
        => Assert.Null(RecreateDivergence.Describe("t", new[] { "a", "b" }, new[] { "a", "b" }, null, false));

    [Fact]
    public void Names_Are_Compared_Without_Regard_To_Case()
        => Assert.Null(RecreateDivergence.Describe("t", new[] { "ID", "NAME" }, new[] { "id", "name" }, null, false));

    [Fact]
    public void A_Column_The_Source_Lacks_Is_Named_As_Dropped()
    {
        var message = RecreateDivergence.Describe("t", new[] { "a", "extra" }, new[] { "a" }, null, false);

        Assert.Contains("extra are not in the source and are dropped", message);
    }

    [Fact]
    public void A_New_Column_Is_Named()
    {
        var message = RecreateDivergence.Describe("t", new[] { "a" }, new[] { "a", "b" }, null, false);

        Assert.Contains("b are new", message);
    }

    [Fact]
    public void The_Old_Primary_Key_Is_Named_When_The_Source_Names_None()
    {
        var message = RecreateDivergence.Describe("t", new[] { "id", "a" }, new[] { "id", "a" }, new[] { "id" }, false);

        Assert.Contains("primary key (id) is not recreated; pass --key", message);
    }

    [Fact]
    public void A_Key_The_Source_Names_Is_Not_Reported_As_Lost()
        => Assert.Null(RecreateDivergence.Describe("t", new[] { "id" }, new[] { "id" }, new[] { "id" }, true));
}
