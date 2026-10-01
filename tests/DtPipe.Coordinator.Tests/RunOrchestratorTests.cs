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
    /// <summary>
    /// A transfer the hub could not open is the cause, as a message: the fragments it cancelled are
    /// neither silent nor a mismatch, and the verdict is never labelled "(unresponsive)".
    /// </summary>
    [Fact]
    public void AWiringFailure_IsTheCause_NeitherUnresponsiveNorAMismatch()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };
        var cancelled = new HashSet<string> { "A", "B" };
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Local),
            ["B"] = Failed("B", FaultOrigin.Local),
        };

        var result = RunOrchestrator.DetermineOutcome(
            reports, edges, ["A", "B"], cancelled, requesterCancelled: false,
            wiringFailure: "transfer A -> B could not be opened by the hub");

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("transfer A -> B could not be opened by the hub", result.Cause);
        Assert.False(result.CauseIsUnresponsive);
        Assert.DoesNotContain("(unresponsive)", result.Describe());
    }

    [Fact]
    public void AWiringFailure_YieldsToAFragmentThatWentSilent()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };
        var reports = new Dictionary<string, FragmentExitReport> { ["A"] = Ok("A", "out", 0) };

        var result = RunOrchestrator.DetermineOutcome(
            reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false,
            wiringFailure: "transfer A -> B could not be opened by the hub");

        Assert.Equal("B", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
    }

    [Fact]
    public void AWiringFailure_NeverOutranksTheRequestersCancellation()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        var result = RunOrchestrator.DetermineOutcome(
            new Dictionary<string, FragmentExitReport>(), edges, ["A", "B"], NoneCancelled, requesterCancelled: true,
            wiringFailure: "transfer A -> B could not be opened by the hub");

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
    }

    /// <summary>The cause stays the first fragment whose own process failed, even when a transfer also failed to open.</summary>
    [Fact]
    public void AWiringFailure_YieldsToAFragmentsLocalFault()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Local),
            ["B"] = Failed("B", FaultOrigin.Remote),
        };

        var result = RunOrchestrator.DetermineOutcome(
            reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false,
            wiringFailure: "transfer A -> B could not be opened by the hub");

        Assert.Equal("A", result.Cause);
        Assert.Equal(["B"], result.Consequences);
    }

    /// <summary>
    /// The grace after a remote failure ran out on B, which never reported on its own: it is the cause, even though it
    /// reported (a remote exit) once the teardown told it to cancel, and A - which failed remotely because of it - is its
    /// consequence. Without B in the unresponsive set, the first remote report in plan order (A) would be named instead.
    /// </summary>
    [Fact]
    public void AFragmentThatDidNotReportWithinTheGrace_IsTheCause_NotTheFragmentThatFailedBecauseOfIt()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote),
            ["B"] = Failed("B", FaultOrigin.Remote),
        };

        var withoutTheSet = RunOrchestrator.DetermineOutcome(reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false);
        var result = RunOrchestrator.DetermineOutcome(
            reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false, unresponsive: new HashSet<string> { "B" });

        Assert.Equal("A", withoutTheSet.Cause);
        Assert.False(withoutTheSet.CauseIsUnresponsive);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("B", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
        Assert.Equal(["A"], result.Consequences);
    }

    /// <summary>An empty set changes nothing: a run that never needed the grace is judged as before.</summary>
    [Fact]
    public void NoUnresponsiveFragment_LeavesTheOutcomeRuleUntouched()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote),
            ["B"] = Failed("B", FaultOrigin.Remote),
        };

        var plain = RunOrchestrator.DetermineOutcome(reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false);
        var empty = RunOrchestrator.DetermineOutcome(
            reports, edges, ["A", "B"], NoneCancelled, requesterCancelled: false, unresponsive: new HashSet<string>());

        Assert.Equal(plain.Cause, empty.Cause);
        Assert.Equal(plain.Consequences, empty.Consequences);
        Assert.Equal(plain.CauseIsUnresponsive, empty.CauseIsUnresponsive);
    }

    private static IReadOnlySet<string> Set(params string[] names) => new HashSet<string>(names);

    /// <summary>
    /// A chain A -> B -> C whose middle is stuck: A failed because of B, and C is silent only because B is. B alone is
    /// named, even with the victim listed first in the plan.
    /// </summary>
    [Fact]
    public void TheStuckMiddleOfAChain_IsNamed_NotTheFragmentThatWaitsOnIt()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in"), new RunEdge("B", "out", "C", "in") };

        var owing = RunOrchestrator.PeersOwingAReport(edges, failedRemotely: Set("A"), reported: Set("A"));
        var reports = new Dictionary<string, FragmentExitReport>
        {
            ["A"] = Failed("A", FaultOrigin.Remote),
            ["B"] = Failed("B", FaultOrigin.Remote),
            ["C"] = Failed("C", FaultOrigin.Remote),
        };
        var result = RunOrchestrator.DetermineOutcome(reports, edges, ["A", "C", "B"], NoneCancelled, requesterCancelled: false, unresponsive: owing);

        Assert.Equal(Set("B"), owing);
        Assert.Equal("B", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
    }

    /// <summary>A branch the failure never touched is not named, however long its fragments take.</summary>
    [Fact]
    public void AnIndependentBranchStillRunning_IsNeverNamed()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in"), new RunEdge("Y", "out", "Z", "in") };

        var owing = RunOrchestrator.PeersOwingAReport(edges, failedRemotely: Set("A"), reported: Set("A"));

        Assert.Equal(Set("B"), owing);
    }

    /// <summary>The failed fragment may be either end of its edge: its silent peer is named on both sides.</summary>
    [Fact]
    public void ThePeerOfAFailedFragment_IsNamedWhicheverEndItIs()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        Assert.Equal(Set("A"), RunOrchestrator.PeersOwingAReport(edges, failedRemotely: Set("B"), reported: Set("B")));
        Assert.Equal(Set("B"), RunOrchestrator.PeersOwingAReport(edges, failedRemotely: Set("A"), reported: Set("A")));
    }

    /// <summary>Nothing is owed when every peer of a failed fragment has reported: the ordinary cause rule applies.</summary>
    [Fact]
    public void WhenEveryPeerHasReported_NoFragmentIsOwing()
    {
        var edges = new[] { new RunEdge("A", "out", "B", "in") };

        Assert.Empty(RunOrchestrator.PeersOwingAReport(edges, failedRemotely: Set("A"), reported: Set("A", "B")));
    }
}
