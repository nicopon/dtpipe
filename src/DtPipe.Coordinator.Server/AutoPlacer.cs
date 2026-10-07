using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Places a job with no help from the user: a branch that is a brick runs on the node that offers
/// it, every other branch on the runner. It only chooses a placement; <see cref="PlanBuilder"/>
/// builds the fragments from it exactly as for a placement the user picked, and cuts nothing.
/// A source brick wired straight into a sink brick therefore crosses no runner.
/// </summary>
public sealed class AutoPlacer(BrickCatalog bricks, NodeInventory inventory, PlanBuilder planner)
{
    /// <summary>Why a branch that reaches <paramref name="nodes"/>' data without being one of their bricks is refused.</summary>
    public static string NotABrick(string alias, IEnumerable<string> nodes) =>
        $"'{alias}' reads or writes {string.Join(", ", nodes)} without being one of its bricks. Rebuild it from the node's bricks.";

    /// <summary>Which brick each branch of <paramref name="yaml"/> is, by alias: what a page needs to name a placed unit. Empty for a job that is not readable.</summary>
    public IReadOnlyDictionary<string, string> BrickAliases(string yaml)
    {
        var brickOf = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (bricks.Match(branch) is { } brick) brickOf[alias] = brick.Key;
        }
        catch (Exception) { }
        return brickOf;
    }

    public async Task<DistributionPlan> PlanAsync(string pipelineId, string yaml, CancellationToken ct)
    {
        var errors = new List<string>();
        var placement = new Dictionary<string, string>(StringComparer.Ordinal);
        var nodes = inventory.Snapshot();
        var runner = inventory.PreferredRunner();
        var hostOf = nodes.SelectMany(n => n.Datasets.Select(d => (d.Variable, n.Name))).ToDictionary(x => x.Variable, x => x.Name, StringComparer.Ordinal);

        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                var brick = bricks.Match(branch);
                if (brick is not null)
                {
                    placement[alias] = brick.Node;
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
                else placement[alias] = runner.Name;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"The job is not readable: {ex.Message}");
        }

        var plan = await planner.BuildAsync(new PlanRequest(pipelineId, yaml, [], placement), ct);
        return errors.Count == 0 ? plan : plan with { Errors = [.. errors, .. plan.Errors] };
    }
}
