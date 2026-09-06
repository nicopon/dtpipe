using DtPipe.Cli.Agent.Tui;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// When a live band follows its tail and when it holds still. The clock is a parameter, so expiry
/// is asserted outright instead of being waited for.
/// </summary>
public class FollowGateTests
{
    private const long Hold = FollowGate.HoldMs;

    [Fact]
    public void An_Unread_Band_Always_Follows()
    {
        var gate = new FollowGate();
        Assert.True(gate.ShouldFollow(0));
        Assert.True(gate.ShouldFollow(1_000_000));
    }

    [Fact]
    public void Taking_The_Focus_Holds_The_Tail_Until_The_Hold_Lapses()
    {
        var gate = new FollowGate();
        gate.Focused(1000);

        Assert.False(gate.ShouldFollow(1000));
        Assert.False(gate.ShouldFollow(1000 + Hold - 1));
        Assert.True(gate.ShouldFollow(1000 + Hold));
    }

    [Fact]
    public void Every_Move_Through_The_Band_Renews_The_Hold()
    {
        var gate = new FollowGate();
        gate.Focused(1000);

        gate.Interacted(1000 + Hold - 1);          // just before it would have lapsed
        Assert.False(gate.ShouldFollow(1000 + Hold));
        Assert.False(gate.ShouldFollow(2000 + Hold - 2));
        Assert.True(gate.ShouldFollow(1000 + Hold - 1 + Hold));
    }

    [Fact]
    public void Losing_The_Focus_Resumes_At_Once()
    {
        var gate = new FollowGate();
        gate.Focused(1000);
        Assert.False(gate.ShouldFollow(1000));

        gate.Blurred();
        Assert.True(gate.ShouldFollow(1000));
    }

    /// <summary>
    /// The panel re-pins the tail by moving the selection and the viewport, which is a move through
    /// the band in every respect but intent. Were that to renew the hold, a band would freeze
    /// permanently the first time it was focused and then left.
    /// </summary>
    [Fact]
    public void A_Move_While_Unfocused_Renews_Nothing()
    {
        var gate = new FollowGate();
        gate.Interacted(1000);
        Assert.True(gate.ShouldFollow(1000));

        gate.Focused(1000);
        gate.Blurred();
        gate.Interacted(2000);
        Assert.True(gate.ShouldFollow(2000));
    }
}
