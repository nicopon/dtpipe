using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Places a job with the help of its placement: a branch that is a brick runs on the node the placement names for it,
/// every other branch on the runner, which the placement never names. It only chooses a placement;
/// <see cref="PlanBuilder"/> builds the fragments from it exactly as for a placement the user picked, and cuts nothing.
/// A source brick wired straight into a sink brick therefore crosses no runner.
/// </summary>
public sealed class AutoPlacer(BrickCatalog bricks, NodeInventory inventory, PlanBuilder planner)
{
    private static readonly IReadOnlyDictionary<string, string> NoPlacement = new Dictionary<string, string>();

    /// <summary>Why a branch that reaches <paramref name="nodes"/>' data without being one of their bricks is refused.</summary>
    public static string NotABrick(string alias, IEnumerable<string> nodes) =>
        $"'{alias}' reads or writes {string.Join(", ", nodes)} without being one of its bricks. Rebuild it from the node's bricks.";

    /// <summary>
    /// Which brick each branch of <paramref name="yaml"/> is, by alias, on the node <paramref name="placement"/> names for it:
    /// what a page needs to name a placed unit. Empty for a job that is not readable.
    /// </summary>
    public IReadOnlyDictionary<string, string> BrickAliases(string yaml, IReadOnlyDictionary<string, string>? placement = null)
    {
        placement ??= NoPlacement;
        var brickOf = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (placement.TryGetValue(alias, out var node) && bricks.MatchAll(branch).FirstOrDefault(b => b.Node == node) is { } brick)
                    brickOf[alias] = brick.Key;
        }
        catch (Exception) { }
        return brickOf;
    }

    /// <summary>The nodes that offer each brick branch of <paramref name="yaml"/>, by alias: the choice a placement makes. Empty for a job that is not readable.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> Offers(string yaml)
    {
        var offers = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (bricks.MatchAll(branch) is { Count: > 0 } offered)
                    offers[alias] = offered.Select(b => b.Node).Distinct().Order(StringComparer.Ordinal).ToList();
        }
        catch (Exception) { }
        return offers;
    }

    /// <summary>
    /// Whether <paramref name="placement"/> holds for <paramref name="yaml"/> now: every branch that is a brick is placed on a node
    /// that exists and offers it, and the placement names nothing else. Empty for a job that is not readable, which planning reports.
    /// </summary>
    public IReadOnlyList<PlacementProblem> Check(string yaml, IReadOnlyDictionary<string, string> placement)
    {
        var problems = new List<PlacementProblem>();
        var nodes = inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        var aliases = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                aliases.Add(alias);
                var offered = bricks.MatchAll(branch);
                var named = placement.TryGetValue(alias, out var node);
                if (offered.Count == 0)
                {
                    if (named)
                        problems.Add(new(alias, PlacementCauses.NotABrick, $"'{alias}' is no brick: it runs on the runner, which a placement never names.", true));
                }
                else if (!named)
                    problems.Add(new(alias, PlacementCauses.Unassigned,
                        $"'{alias}' is a brick of {string.Join(", ", offered.Select(b => b.Node).Distinct())}: name the node it runs on.", true));
                else if (!nodes.TryGetValue(node!, out var view))
                    problems.Add(new(alias, PlacementCauses.UnknownNode, $"'{alias}' is placed on {node}, which is no node of this coordinator.", true));
                else if (offered.All(b => b.Node != node))
                    problems.Add(new(alias, PlacementCauses.NotOffered,
                        $"'{alias}' is placed on {node}, which does not offer this brick (offered by {string.Join(", ", offered.Select(b => b.Node).Distinct())}).", true));
                else if (!view.Online)
                    problems.Add(new(alias, PlacementCauses.Offline, $"'{alias}' is placed on {node}, which has no connected replica.", false));
            }
        }
        catch (Exception)
        {
            return [];
        }
        problems.AddRange(placement.Keys.Where(a => !aliases.Contains(a)).Order(StringComparer.Ordinal)
            .Select(a => new PlacementProblem(a, PlacementCauses.NoBranch, $"The placement names '{a}', which is no branch of the job.", true)));
        return problems;
    }

    public async Task<DistributionPlan> PlanAsync(string pipelineId, string yaml, IReadOnlyDictionary<string, string> placement, CancellationToken ct)
    {
        var errors = new List<string>();
        var placed = new Dictionary<string, string>(StringComparer.Ordinal);
        var nodes = inventory.Snapshot();
        var runner = inventory.PreferredRunner();
        var hostOf = nodes.SelectMany(n => n.Datasets.Select(d => (d.Variable, n.Name))).ToDictionary(x => x.Variable, x => x.Name, StringComparer.Ordinal);
        var problems = Check(yaml, placement);
        var refused = problems.Where(p => p.Blocking).Select(p => p.Alias).ToHashSet(StringComparer.Ordinal);
        errors.AddRange(problems.Where(p => p.Blocking).Select(p => p.Message));

        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                if (bricks.MatchAll(branch).Count > 0)
                {
                    if (!refused.Contains(alias)) placed[alias] = placement[alias];
                    continue;
                }
                // A node's database is reached only through a brick its owner declared.
                var variables = JobYaml.Variables(JobYaml.Serialize(new() { { alias, branch } })).Where(hostOf.ContainsKey).ToList();
                if (variables.Count > 0)
                {
                    errors.Add(NotABrick(alias, variables.Select(v => hostOf[v]).Distinct()));
                    continue;
                }
                if (runner is null) errors.Add($"'{alias}' needs a runner, and none is connected.");
                else placed[alias] = runner.Name;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"The job is not readable: {ex.Message}");
        }

        var plan = await planner.BuildAsync(new PlanRequest(pipelineId, yaml, [], placed), PlanMode.Library, ct);
        var warnings = problems.Where(p => !p.Blocking).Select(p => p.Message).ToList();
        return plan with { Errors = [.. errors, .. plan.Errors], Warnings = [.. warnings, .. plan.Warnings] };
    }
}
