using System.Collections.Concurrent;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What a binding's mode found when its brick was read against its pin. <see cref="Blocking"/>: the verdict that refuses a run,
/// <see cref="BindingVerdicts.Breaking"/> or <see cref="BindingVerdicts.Unread"/> (the node could not tell, which is no verdict
/// on the brick but never lets a run through).
/// </summary>
public sealed record BindingVerdict(string Verdict, string Report = "")
{
    public bool Blocking => Verdict is BindingVerdicts.Breaking or BindingVerdicts.Unread;
}

/// <summary>Whether a verdict is known, being computed, or neither, as <see cref="BindingVerifier.StateOf"/> says without asking a node.</summary>
public enum VerdictState { Absent, InFlight, Known }

/// <summary>
/// The verdict of a binding whose brick may have changed since a check pinned it. A strict binding's is <see cref="BindingVerdicts.Exact"/>
/// (its text is the brick: <see cref="AutoPlacer.Check"/> judged it). For another mode, a brick whose version is the pin's is
/// <see cref="BindingVerdicts.Unchanged"/>; a changed one is <see cref="BindingVerdicts.Tolerated"/> in <c>tolerant</c> with no node asked,
/// and in <c>contract</c> or <c>compatible</c> is the answer of the node that hosts it: a source judges its own sample against the pinned contract
/// (<c>CheckSource</c>), a sink its target against it (<c>CheckSink</c>). The server runs no dtpipe.
/// <para>
/// A definitive answer is kept by pipeline, commit, alias, brick version and the binding's own hash (a bindings save moves no commit but
/// re-pins), so a brick is read once per version and not once per run; one lazy computation is shared by concurrent callers. An answer
/// that is no verdict (<see cref="BindingVerdicts.Unread"/>: a node offline, a timeout, a node too old to know the request) is never kept.
/// </para>
/// </summary>
public class BindingVerifier(BrickCatalog bricks, NodeRequests nodes)
{
    private static readonly TimeSpan NodeBound = TimeSpan.FromSeconds(40);

    private readonly ConcurrentDictionary<(string Pipeline, string Commit, string Alias, string Version, string Binding), Lazy<Task<BindingVerdict>>> _kept = new();

    private static (string, string, string, string, string) KeyOf(string pipelineId, string commit, string alias, string version, Binding binding) =>
        (pipelineId, commit, alias, version, Bindings.ShaOf(binding));

    public virtual async Task<BindingVerdict> VerdictAsync(string pipelineId, string commit, string alias, Binding binding, CancellationToken ct)
    {
        var mode = Bindings.ModeOf(binding);
        if (mode == BindingModes.Strict) return new BindingVerdict(BindingVerdicts.Exact);
        if (bricks.Find($"{binding.Node}/{binding.Brick}") is not { } brick)
            return new BindingVerdict(BindingVerdicts.Unread, $"{binding.Node} offers no brick '{binding.Brick}'.");
        if (binding.Pin is not { } pin)
            return new BindingVerdict(BindingVerdicts.Unread, "No check has pinned this binding.");
        if (pin.Version == brick.Version) return new BindingVerdict(BindingVerdicts.Unchanged);
        if (mode == BindingModes.Tolerant) return new BindingVerdict(BindingVerdicts.Tolerated);

        var key = KeyOf(pipelineId, commit, alias, brick.Version, binding);
        var computing = _kept.GetOrAdd(key, _ => new Lazy<Task<BindingVerdict>>(() => ComputeAsync(brick, pin, mode)));
        var verdict = await computing.Value.WaitAsync(ct);
        // A verdict that says nothing about the brick is asked again next time.
        if (verdict.Verdict == BindingVerdicts.Unread) _kept.TryRemove(KeyValuePair.Create(key, computing));
        return verdict;
    }

    /// <summary>
    /// Whether the verdict of a binding at its brick's version is kept, being computed, or neither: what a health read needs without
    /// asking a node. A mode that needs no node (strict, an unchanged brick, tolerant) is <see cref="VerdictState.Known"/>.
    /// </summary>
    public VerdictState StateOf(string pipelineId, string commit, string alias, Binding binding)
    {
        var mode = Bindings.ModeOf(binding);
        if (mode == BindingModes.Strict) return VerdictState.Known;
        if (bricks.Find($"{binding.Node}/{binding.Brick}") is not { } brick || binding.Pin is not { } pin) return VerdictState.Absent;
        if (pin.Version == brick.Version || mode == BindingModes.Tolerant) return VerdictState.Known;
        return _kept.TryGetValue(KeyOf(pipelineId, commit, alias, brick.Version, binding), out var computing)
            ? computing.IsValueCreated && computing.Value.IsCompletedSuccessfully ? VerdictState.Known : VerdictState.InFlight
            : VerdictState.Absent;
    }

    private async Task<BindingVerdict> ComputeAsync(BrickView brick, BindingPin pin, string mode)
    {
        try
        {
            return await AskAsync(brick, pin, mode);
        }
        catch (Exception ex)
        {
            // An offline node, a timeout, a node too old to know the request: none of them says what the brick is.
            return new BindingVerdict(BindingVerdicts.Unread, ex is OperationCanceledException
                ? $"{brick.Node} did not answer within {NodeBound.TotalSeconds:F0}s." : $"{brick.Node}: {ex.Message}");
        }
    }

    /// <summary>The node's answer about <paramref name="brick"/> against <paramref name="pin"/>; a test replaces it.</summary>
    protected virtual async Task<BindingVerdict> AskAsync(BrickView brick, BindingPin pin, string mode)
    {
        if (pin.Contract is null) return new BindingVerdict(BindingVerdicts.Unread, "The pin holds no contract.");
        if (brick.Kind == BrickKind.Sink)
        {
            var sink = await nodes.AskAsync<SinkCheck>(brick.Node, NodeChannel.CheckSink, [brick.Id, pin.Contract], NodeBound, CancellationToken.None);
            return new BindingVerdict(sink.Accepted ? BindingVerdicts.Accepted : BindingVerdicts.Breaking, sink.Report);
        }
        var source = await nodes.AskAsync<SourceCheck>(brick.Node, NodeChannel.CheckSource, [brick.Id, pin.Contract, mode], NodeBound, CancellationToken.None);
        return new BindingVerdict(source.Verdict switch
        {
            SourceVerdicts.Same => BindingVerdicts.Same,
            SourceVerdicts.Compatible => BindingVerdicts.Compatible,
            SourceVerdicts.Breaking => BindingVerdicts.Breaking,
            _ => BindingVerdicts.Unread,
        }, source.Report);
    }
}
