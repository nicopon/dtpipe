using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// <see cref="RunOrchestrator.DetermineOutcome"/> reading the category each end reports of its edge's failure, on reports written by
/// hand. When every fault is a consequence of a transfer, an edge an end of which failed end-to-end encryption is the cause, as an edge,
/// and a sender that had no acknowledgement names its receiver; nothing else changes, and reports without a category give the verdict
/// the rule gives without them, to the character.
/// </summary>
public class EdgeFailureVerdictTests
{
    private static readonly IReadOnlySet<string> NoneCancelled = new HashSet<string>();

    private static FragmentExitReport Remote(string fragment, params (string Alias, string Word)[] failures) =>
        new(fragment, 1, FaultOrigin.Remote, $"{fragment} failed", failures.ToDictionary(f => f.Alias, _ => 0L),
            EdgeFailures: failures.Length == 0 ? null : failures.ToDictionary(f => f.Alias, f => f.Word));

    private static FragmentExitReport WithoutCategories(FragmentExitReport report) => report with { EdgeFailures = null };

    private static RunResult Determine(IReadOnlyList<RunEdge> edges, params FragmentExitReport[] reports) =>
        RunOrchestrator.DetermineOutcome(
            reports.ToDictionary(r => r.Fragment), edges, reports.Select(r => r.Fragment).ToList(), NoneCancelled, requesterCancelled: false);

    private static readonly RunEdge[] Pair = [new("A", "out", "B", "in")];
    private static readonly RunEdge[] Chain = [new("A", "out", "B", "in"), new("B", "out", "C", "in")];

    private const string Sealing = TransferFailures.EndToEndEncryptionFailed;
    private const string Abandoned = TransferFailures.AbandonedByPeer;

    [Theory]
    [InlineData("the producer")]
    [InlineData("the consumer")]
    [InlineData("the producer, the consumer reporting no category")]
    public void AnEdgeAnEndOfWhichFailedTheEncryption_IsTheCause_AndEveryFailedFragmentAConsequence(string sealingEnd)
    {
        var (producer, consumer) = sealingEnd switch
        {
            "the producer" => (Remote("A", ("out", Sealing)), Remote("B", ("in", Abandoned))),
            "the consumer" => (Remote("A", ("out", Abandoned)), Remote("B", ("in", Sealing))),
            _ => (Remote("A", ("out", Sealing)), Remote("B")),
        };

        var result = Determine(Pair, producer, consumer);

        Assert.Equal((RunOutcome.Failed, "A -> B", CauseKinds.Edge, false), (result.Outcome, result.Cause, result.CauseKind, result.CauseIsUnresponsive));
        Assert.Equal(["A", "B"], result.Consequences);
    }

    /// <summary>
    /// The chain A -> B -> C whose middle alone seals: both of B's edges fail, and the first edge of the run is the cause, where the
    /// reports without their categories name A, the healthy fragment that happens to come first.
    /// </summary>
    [Fact]
    public void AChainWhoseMiddleAloneSeals_NamesItsFirstEdge_WhereTheSameReportsWithoutCategoriesNameAFragment()
    {
        FragmentExitReport[] reports =
        [
            Remote("A", ("out", Abandoned)),
            Remote("B", ("in", Sealing), ("out", Sealing)),
            Remote("C", ("in", Abandoned)),
        ];

        var result = Determine(Chain, reports);
        var without = Determine(Chain, [.. reports.Select(WithoutCategories)]);

        Assert.Equal(("A -> B", CauseKinds.Edge), (result.Cause, result.CauseKind));
        Assert.Equal(["A", "B", "C"], result.Consequences);
        Assert.Equal((RunOutcome.Failed, "A", CauseKinds.Fragment, false), (without.Outcome, without.Cause, without.CauseKind, without.CauseIsUnresponsive));
        Assert.Equal(["B", "C"], without.Consequences);
        Assert.Equal(
            string.Join(Environment.NewLine, "Outcome: Failed", "Cause: A", "Consequences: B, C",
                "  A.out -> B.in: sent=0 received=0", "  B.out -> C.in: sent=0 received=0"),
            without.Describe());
    }

    /// <summary>A fragment's own fault is judged before any category: the edge its peers report is a consequence of it.</summary>
    [Fact]
    public void AFragmentsOwnFault_OutranksAnEdgeThatFailedTheEncryption()
    {
        var local = new FragmentExitReport("C", 1, FaultOrigin.Local, "C failed", new Dictionary<string, long> { ["in"] = 0 });

        var result = Determine(Chain, Remote("A", ("out", Sealing)), Remote("B", ("in", Abandoned)), local);

        Assert.Equal((RunOutcome.Failed, "C", CauseKinds.Fragment), (result.Outcome, result.Cause, result.CauseKind));
        Assert.Equal(["A", "B"], result.Consequences);
    }

    /// <summary>A sender that had no acknowledgement names the receiver of its edge, and is itself a consequence; a receiver's own timeout names nobody.</summary>
    [Fact]
    public void ASenderThatHadNoAcknowledgement_NamesItsReceiver()
    {
        var result = Determine(Pair, Remote("A", ("out", TransferFailures.AcknowledgementTimedOut)), Remote("B", ("in", Abandoned)));
        var onTheReceiver = Determine(Pair, Remote("A", ("out", Abandoned)), Remote("B", ("in", TransferFailures.AcknowledgementTimedOut)));

        Assert.Equal((RunOutcome.Failed, "B", CauseKinds.MuteReceiver, false), (result.Outcome, result.Cause, result.CauseKind, result.CauseIsUnresponsive));
        Assert.Equal(["A"], result.Consequences);
        Assert.Equal(("A", CauseKinds.Fragment), (onTheReceiver.Cause, onTheReceiver.CauseKind));
    }

    /// <summary>A text that is no category is dropped when the report is heard, and changes no verdict where a report carries it anyway.</summary>
    [Fact]
    public void AWordThatIsNoCategory_HasNoEffect()
    {
        FragmentExitReport[] foreign = [Remote("A", ("out", "SealedByMagic")), Remote("B", ("in", "endtoendencryptionfailed"))];

        var result = Determine(Pair, foreign);
        var plain = Determine(Pair, [.. foreign.Select(WithoutCategories)]);

        Assert.Equal(plain.Describe(), result.Describe());
        Assert.Equal(("A", CauseKinds.Fragment), (result.Cause, result.CauseKind));
        Assert.Equal(plain.Consequences, result.Consequences);
        Assert.Null(RunOrchestrator.FailuresReported(new Dictionary<string, string> { ["out"] = "SealedByMagic" }));
        Assert.Equal([("in", Sealing)], RunOrchestrator.FailuresReported(
            new Dictionary<string, string> { ["out"] = "SealedByMagic", ["in"] = Sealing })!.Select(e => (e.Key, e.Value)));
    }
}
