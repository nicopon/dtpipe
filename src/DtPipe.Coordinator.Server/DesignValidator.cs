using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What a check found. <see cref="Pins"/>: for every brick it sampled (a source) or whose target it judged (a sink), by alias, the
/// brick and the pin a binding in a mode other than strict keeps (<see cref="Bindings.Pinned"/>); only a passing check has any.
/// </summary>
public sealed record DesignCheck(bool Ok, string? Report, IReadOnlyDictionary<string, CheckedPin>? Pins = null);

/// <summary>
/// Checks a designer's job through the real engine without the server opening a node's database or
/// running dtpipe. Each source brick becomes <c>input: arrow:-</c>, bound to the first row its node sends
/// (<c>SampleBrick</c>, the bytes its fragment would send the runner); each sink brick becomes an
/// <c>arrow:-</c> end that saves the contract reaching it. A runner dry-runs that job
/// (<c>ValidateJob</c>), exactly the runner's view of the pipeline, then each sink brick's node judges its
/// own target against its contract (<c>CheckSink</c>). Every other branch is dtpipe's as written; one that
/// reaches a node's data without being its brick is refused before anything runs.
/// </summary>
public class DesignValidator(BrickCatalog bricks, NodeRequests nodes)
{
    private static readonly TimeSpan NodeBound = TimeSpan.FromSeconds(40);
    private static readonly TimeSpan RunnerBound = TimeSpan.FromSeconds(75);

    private sealed record Placed(string Alias, BrickView Brick);

    /// <summary>The brick a branch references, or null for any other branch: a reference names its brick, and there is no text to match.</summary>
    public BrickView? BrickOf(string alias, YamlMappingNode branch) =>
        BrickRef.Of(alias, branch) is { } reference ? bricks.Find(reference.Key) : null;

    public virtual async Task<DesignCheck> ValidateAsync(string yaml, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        var rewritten = new YamlMappingNode();
        var sources = new List<Placed>();
        var sinks = new List<Placed>();
        var refusals = new List<string>();
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
        {
            // A reference is the node's to compose, and dtpipe does not read it: one that names no source brick is refused here by name,
            // never copied into the runner's dry-run.
            if (BrickRef.Misuse(alias, branch) is { } misuse) { refusals.Add(misuse); continue; }
            if (BrickRef.Of(alias, branch) is { } named
                && BrickOf(alias, branch)?.Kind != (named.IsSink ? BrickKind.Sink : BrickKind.Source))
            {
                refusals.Add($"'{alias}' references {named.Key}, which no node offers as a {(named.IsSink ? "sink" : "source")}.");
                continue;
            }
            switch (BrickOf(alias, branch))
            {
                case { Kind: BrickKind.Source } source:
                    rewritten.Add(alias, new YamlMappingNode { { "input", BrickRules.ArrowEndpoint } });
                    sources.Add(new Placed(alias, source));
                    break;
                case { Kind: BrickKind.Sink } sink:
                    rewritten.Add(alias, new YamlMappingNode
                    {
                        { "from", branch.Children[new YamlScalarNode("from")] },
                        { "output", BrickRules.ArrowEndpoint },
                        { "contract-save", JobCheck.ContractFile(alias) },
                    });
                    sinks.Add(new Placed(alias, sink));
                    break;
                default:
                    var reached = JobYaml.Variables(alias, branch)
                        .Select(nodes.HostOf).OfType<string>().Distinct().ToList();
                    if (reached.Count > 0) refusals.Add(AutoPlacer.NotABrick(alias, reached));
                    rewritten.Add(alias, JobYaml.CloneBranch(branch));
                    break;
            }
        }
        if (refusals.Count > 0) return new DesignCheck(false, string.Join('\n', refusals));

        var samples = await Task.WhenAll(sources.Select(async s =>
            (s, Answer: await Ask<BrickSample>(s.Brick, NodeChannel.SampleBrick, [s.Brick.Id, 1], e => new BrickSample(null, e), ct))));
        var unread = samples.Where(x => x.Answer.Arrow is null)
            .Select(x => $"'{x.s.Alias}': {x.s.Brick.Key} sent no sample: {x.Answer.Error}").ToList();
        if (unread.Count > 0) return new DesignCheck(false, string.Join('\n', unread));

        var pins = new Dictionary<string, CheckedPin>(StringComparer.Ordinal);
        foreach (var x in samples)
            pins[x.s.Alias] = new CheckedPin(x.s.Brick.Id, new BindingPin(x.s.Brick.Version, x.Answer.Contract));
        var check = new JobCheck(
            JobYaml.Serialize(rewritten),
            samples.ToDictionary(x => x.s.Alias, x => x.Answer.Arrow!, StringComparer.Ordinal),
            [.. sinks.Select(s => s.Alias)]);
        JobCheckResult dryRun;
        try
        {
            dryRun = await nodes.AskRunnerAsync<JobCheckResult>(NodeChannel.ValidateJob, [check], RunnerBound, ct);
        }
        catch (ConflictException ex)
        {
            return new DesignCheck(false, ex.Message);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new DesignCheck(false, $"The runner did not answer within {RunnerBound.TotalSeconds:F0}s.");
        }
        if (!dryRun.Ok) return new DesignCheck(false, dryRun.Report);

        var checks = await Task.WhenAll(sinks.Select(async s =>
        {
            var contract = dryRun.Contracts.GetValueOrDefault(s.Alias)
                ?? throw new ConflictException($"The runner sent no contract for '{s.Alias}'.");
            return (s, Answer: await Ask<SinkCheck>(s.Brick, NodeChannel.CheckSink, [s.Brick.Id, contract], e => new SinkCheck(false, e), ct));
        }));
        var refused = checks.Where(x => !x.Answer.Accepted)
            .Select(x => $"'{x.s.Alias}': {x.s.Brick.Key} does not accept what reaches it:\n{x.Answer.Report}").ToList();
        foreach (var s in sinks) pins[s.Alias] = new CheckedPin(s.Brick.Id, new BindingPin(s.Brick.Version, dryRun.Contracts[s.Alias]));
        return refused.Count == 0 ? new DesignCheck(true, null, pins) : new DesignCheck(false, string.Join("\n\n", refused));
    }

    /// <summary>A node's answer, or <paramref name="failed"/> with why it never came.</summary>
    private async Task<T> Ask<T>(BrickView brick, string method, object?[] args, Func<string, T> failed, CancellationToken ct)
    {
        try
        {
            return await nodes.AskAsync<T>(brick.Node, method, args, NodeBound, ct);
        }
        catch (ConflictException ex)
        {
            return failed(ex.Message);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return failed($"{brick.Node} did not answer within {NodeBound.TotalSeconds:F0}s.");
        }
    }
}
