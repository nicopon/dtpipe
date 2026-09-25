using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// <see cref="RunOrchestrator.DetermineOutcome"/> as a pure function - every branch of the outcome
/// rule, without spawning a single process. <see cref="CoordinatorDrivenTests"/> in the pipeline
/// node's own test project covers the same rule end to end, against real fragments.
/// </summary>
public class RunOrchestratorTests
{
    private static readonly IReadOnlyDictionary<string, long> NoRows = new Dictionary<string, long>();

    private static FragmentExitReport Ok(string fragment, string alias, long rows) =>
        new(fragment, 0, null, null, new Dictionary<string, long> { [alias] = rows });

    private static FragmentExitReport Failed(string fragment, FaultOrigin origin, string alias = "out") =>
        new(fragment, 1, origin, $"{fragment} failed", new Dictionary<string, long> { [alias] = 0 });

    [Fact]
    public void EveryFragmentZero_MatchingCounts_Succeeds()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 100),
            ["B"] = Ok("B", "in", 100),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(reports, edges);

        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        Assert.Null(result.Cause);
    }

    /// <summary>
    /// A fragment reporting 0 attests its own process, not that its peer received what it sent: a
    /// transfer can be acknowledged into a receive buffer before the receiving fragment has actually
    /// drained all of it, so a consumer can exit 0 having only forwarded part of what its producer
    /// sent - no fault, no non-zero exit code, on either side.
    /// </summary>
    [Fact]
    public void EveryFragmentZero_MismatchedCounts_FailsNamingTheEdge()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 1_000_000),
            ["B"] = Ok("B", "in", 200_000),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(reports, edges);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Contains("A->B", result.Cause);
        Assert.Empty(result.Consequences);
    }

    /// <summary>
    /// The cause is the fragment with a local-origin failure, not whichever non-zero fragment the
    /// reports dictionary happens to enumerate first - built with the local failure inserted last to
    /// rule that out. Dropping the origin filter (leaving only the "first non-zero" fallback) turns
    /// this red with cause "A", the insertion order, instead of "B".
    /// </summary>
    [Fact]
    public void MixedOrigins_CauseIsTheLocalOne_RegardlessOfInsertionOrder()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote),
            ["C"] = Failed("C", FaultOrigin.Remote),
            ["B"] = Failed("B", FaultOrigin.Local),
        };
        var edges = Array.Empty<RunEdge>();

        var result = RunOrchestrator.DetermineOutcome(reports, edges);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        Assert.Equal(["A", "C"], result.Consequences.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void NoLocalOrigin_FallsBackToTheFirstNonZeroReport()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote),
            ["B"] = Failed("B", FaultOrigin.Remote),
        };

        var result = RunOrchestrator.DetermineOutcome(reports, Array.Empty<RunEdge>());

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("A", result.Cause);
    }

    /// <summary>
    /// A fragment that never reports a count for its side of an edge - it died before ever being
    /// wired to it - must not read as an agreement. A chain of <c>&amp;&amp;</c> lookups that
    /// short-circuits past the missing key would do exactly that.
    /// </summary>
    [Fact]
    public void AnEdgeWithAMissingCount_FailsClosed()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 100),
            ["B"] = new FragmentExitReport("B", 0, null, null, NoRows),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(reports, edges);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Contains("A->B", result.Cause);
    }

    [Fact]
    public void EdgeCounts_AreReported_EvenWhenTheCauseIsAFragmentFailure()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 500),
            ["B"] = Failed("B", FaultOrigin.Local, "in"),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(reports, edges);

        var edge = Assert.Single(result.EdgeCounts);
        Assert.Equal(500, edge.Sent);
        Assert.Equal(0, edge.Received);
        Assert.False(edge.Agrees);
        Assert.Contains("A.out -> B.in", result.Describe());
    }
}
