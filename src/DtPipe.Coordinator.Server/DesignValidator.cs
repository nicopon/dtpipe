using DtPipe.Coordinator.Contracts;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server;

public sealed record DesignCheck(bool Ok, string? Report);

/// <summary>
/// Checks a designer's job through the real engine without the server opening a node's database. Each
/// source brick becomes <c>input: arrow:-</c>, bound to the first row its node sends
/// (<c>SampleBrick</c>, the bytes its fragment would send the runner); each sink brick becomes an
/// <c>arrow:-</c> end that saves the contract reaching it. dtpipe dry-runs that job here, exactly the
/// runner's view of the pipeline, then each sink brick's node judges its own target against its
/// contract (<c>CheckSink</c>). Every other branch is dtpipe's as written; one that reaches a node's
/// data without being its brick is refused before anything runs.
/// </summary>
public sealed class DesignValidator(BrickCatalog bricks, NodeRequests nodes, DtPipeCli cli, CoordinatorServerOptions options)
{
    private static readonly TimeSpan NodeBound = TimeSpan.FromSeconds(40);
    private static readonly TimeSpan DryRunTimeout = TimeSpan.FromSeconds(60);

    private sealed record Placed(string Alias, BrickView Brick);

    public async Task<DesignCheck> ValidateAsync(string yaml, CancellationToken ct)
    {
        var rewritten = new YamlMappingNode();
        var sources = new List<Placed>();
        var sinks = new List<Placed>();
        var refusals = new List<string>();
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
        {
            switch (bricks.Match(branch))
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
                        { "contract-save", ContractFile(alias) },
                    });
                    sinks.Add(new Placed(alias, sink));
                    break;
                default:
                    var reached = JobYaml.Variables(JobYaml.Serialize(new YamlMappingNode { { alias, branch } }))
                        .Select(nodes.HostOf).OfType<string>().Distinct().ToList();
                    if (reached.Count > 0) refusals.Add(AutoPlacer.NotABrick(alias, reached));
                    rewritten.Add(alias, JobYaml.CloneBranch(branch));
                    break;
            }
        }
        if (refusals.Count > 0) return new DesignCheck(false, string.Join('\n', refusals));

        var dir = Path.Combine(options.StateDir, "validate", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);

        var samples = await Task.WhenAll(sources.Select(async s =>
            (s, Answer: await Ask<BrickSample>(s.Brick, NodeChannel.SampleBrick, [s.Brick.Id, 1], e => new BrickSample(null, e), ct))));
        var unread = samples.Where(x => x.Answer.Arrow is null)
            .Select(x => $"'{x.s.Alias}': {x.s.Brick.Key} sent no sample: {x.Answer.Error}").ToList();
        if (unread.Count > 0) return new DesignCheck(false, string.Join('\n', unread));
        foreach (var (s, answer) in samples)
            await File.WriteAllBytesAsync(Path.Combine(dir, SampleFile(s.Alias)), answer.Arrow!, ct);

        await File.WriteAllTextAsync(Path.Combine(dir, "job.yaml"), JobYaml.Serialize(rewritten), ct);
        // Every sink is bound to a file of its own: the writer is neutralised, so none is written.
        string[] args =
        [
            "--job", "job.yaml", "--dry-run", "1", "--no-stats",
            .. sources.Count > 0 ? ["--bind-input", string.Join(',', sources.Select(s => $"{s.Alias}={SampleFile(s.Alias)}"))] : Array.Empty<string>(),
            .. sinks.Count > 0 ? ["--bind-output", string.Join(',', sinks.Select(s => $"{s.Alias}={s.Alias}.sink"))] : Array.Empty<string>(),
        ];
        var dryRun = await cli.RunAsync(args, dir, DryRunTimeout, ct);
        if (!dryRun.Succeeded) return new DesignCheck(false, dryRun.Diagnostic(8));

        var checks = await Task.WhenAll(sinks.Select(async s =>
        {
            var contract = await File.ReadAllTextAsync(Path.Combine(dir, ContractFile(s.Alias)), ct);
            return (s, Answer: await Ask<SinkCheck>(s.Brick, NodeChannel.CheckSink, [s.Brick.Id, contract], e => new SinkCheck(false, e), ct));
        }));
        var refused = checks.Where(x => !x.Answer.Accepted)
            .Select(x => $"'{x.s.Alias}': {x.s.Brick.Key} does not accept what reaches it:\n{x.Answer.Report}").ToList();
        return refused.Count == 0 ? new DesignCheck(true, null) : new DesignCheck(false, string.Join("\n\n", refused));
    }

    private static string SampleFile(string alias) => alias + ".arrows";

    private static string ContractFile(string alias) => alias + ".contract.json";

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
