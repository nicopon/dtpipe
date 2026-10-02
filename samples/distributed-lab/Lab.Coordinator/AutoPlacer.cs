using DtPipe.Lab.Contracts;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// Places a job with no help from the user: a branch that is a brick runs on the node that offers
/// it, every other branch on the runner. It only chooses a placement; <see cref="PlanBuilder"/>
/// builds the fragments from it exactly as for a placement the user picked, and cuts nothing.
/// A source brick wired straight into a sink brick therefore crosses no runner.
/// </summary>
public sealed class AutoPlacer(BrickCatalog bricks, NodeInventory inventory, PlanBuilder planner, RightsStore rights)
{
    public async Task<LabPlan> PlanAsync(string pipelineId, string yaml, CancellationToken ct)
    {
        var errors = new List<string>();
        var placement = new Dictionary<string, string>(StringComparer.Ordinal);
        var nodes = inventory.Snapshot();
        var runner = nodes.Where(n => n.Role == NodeRole.Runner).OrderByDescending(n => n.Online).ThenBy(n => n.Name, StringComparer.Ordinal).FirstOrDefault();
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
                    errors.Add($"'{alias}' reads or writes {string.Join(", ", variables.Select(v => hostOf[v]).Distinct())} " +
                               "without being one of its bricks. Rebuild it from the node's bricks, or place it by hand in the Lab view.");
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

        var plan = rights.WithPolicies(await planner.BuildAsync(new PlanRequest(pipelineId, yaml, [], placement), ct), yaml, bricks);
        return errors.Count == 0 ? plan : plan with { Errors = [.. errors, .. plan.Errors] };
    }
}
