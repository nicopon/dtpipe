using DtPipe.Coordinator;
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
    private static readonly IReadOnlySet<string> NoneCancelled = new HashSet<string>();

    private static FragmentExitReport Ok(string fragment, string alias, long rows) =>
        new(fragment, 0, null, null, new Dictionary<string, long> { [alias] = rows });

    private static FragmentExitReport Failed(string fragment, FaultOrigin origin, string alias = "out", int exitCode = 1) =>
        new(fragment, exitCode, origin, $"{fragment} failed", new Dictionary<string, long> { [alias] = 0 });

    private static RunResult Determine(
        IReadOnlyDictionary<string, FragmentExitReport> reports, IReadOnlyList<RunEdge> edges,
        IReadOnlySet<string>? cancelled = null, bool requesterCancelled = false) =>
        RunOrchestrator.DetermineOutcome(reports, edges, reports.Keys.ToList(), cancelled ?? NoneCancelled, requesterCancelled);

    [Fact]
    public void EveryFragmentZero_MatchingCounts_Succeeds()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 100),
            ["B"] = Ok("B", "in", 100),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = Determine(reports, edges);

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

        var result = Determine(reports, edges);

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

        var result = Determine(reports, edges);

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

        var result = Determine(reports, Array.Empty<RunEdge>());

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

        var result = Determine(reports, edges);

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

        var result = Determine(reports, edges);

        var edge = Assert.Single(result.EdgeCounts);
        Assert.Equal(500, edge.Sent);
        Assert.Equal(0, edge.Received);
        Assert.False(edge.Agrees);
        Assert.Contains("A.out -> B.in", result.Describe());
    }

    /// <summary>
    /// A fragment absent from <c>reports</c> - it never reported <c>Exited</c> at all - is the pair
    /// muet case: named as the cause even though every fragment that did report is at 0. A run is
    /// never read as a success just because nothing present has failed.
    /// </summary>
    [Fact]
    public void AFragmentThatNeverReported_IsTheCause_NeverReadAsSuccess()
    {
        var reports = new Dictionary<string, FragmentExitReport> { ["A"] = Ok("A", "out", 100) };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        Assert.DoesNotContain("B", result.Reports.Keys);
        Assert.True(result.CauseIsUnresponsive);
        Assert.Contains("(unresponsive)", result.Describe());
    }

    /// <summary>
    /// The row-count-mismatch form of <c>Cause</c> is a message, not a fragment name, and must never
    /// be labelled "(unresponsive)" in <see cref="RunResult.Describe"/> - the deterrent case for
    /// annotating any cause absent from <c>Reports</c>, which this message always is.
    /// </summary>
    [Fact]
    public void AnEdgeMismatchCause_IsNeverLabelledUnresponsive()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Ok("A", "out", 1_000_000),
            ["B"] = Ok("B", "in", 200_000),
        };
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = Determine(reports, edges);

        Assert.False(result.CauseIsUnresponsive);
        Assert.DoesNotContain("(unresponsive)", result.Describe());
    }

    [Fact]
    public void RequesterCancelled_ReportsCancelled_RegardlessOfWhatReported()
    {
        var reports = new Dictionary<string, FragmentExitReport> { ["A"] = Failed("A", FaultOrigin.Remote) };

        var result = RunOrchestrator.DetermineOutcome(reports, [], ["A", "B"], NoneCancelled, requesterCancelled: true);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Null(result.Cause);
    }

    /// <summary>
    /// A first uncommanded stop that is itself a 130 (the product's own user-cancellation exit code,
    /// root <c>CLAUDE.md</c>) reports the whole run as cancelled, not failed - no distinction is drawn
    /// between a requester's own cancel and a fragment's child exiting 130 on its own.
    /// </summary>
    [Fact]
    public void AnUncommandedLocalFaultAt130_ReportsCancelled()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Local, exitCode: 130),
            ["B"] = Failed("B", FaultOrigin.Remote),
        };

        var result = Determine(reports, []);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Equal("A", result.Cause);
        Assert.Equal(["B"], result.Consequences);
    }

    /// <summary>
    /// A fragment the coordinator itself told to <c>Cancel</c> reports a plain non-zero, local-origin
    /// exit - its own child was killed on command - indistinguishable on the wire from an organic
    /// failure. Excluding it from cause and consequence is what keeps a coordinator-driven abort from
    /// misreporting its own collateral as the reason the run failed.
    /// </summary>
    [Fact]
    public void ACoordinatorCancelledFragment_IsNeverTheCauseNorAConsequence()
    {
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote), // the organic failure driving the abort
            ["B"] = Failed("B", FaultOrigin.Local),  // killed by the coordinator's own Cancel
        };

        var result = Determine(reports, [], cancelled: new HashSet<string> { "B" });

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("A", result.Cause);
        Assert.DoesNotContain("B", result.Consequences);
    }
}
