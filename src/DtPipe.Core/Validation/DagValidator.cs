using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Validation;

/// <summary>
/// Validates Directed Acyclic Graph (DAG) structures independently of the input source (CLI or YAML).
/// Handles alias resolution, cycle detection, and structural constraints like output usage.
/// </summary>
public static class DagValidator
{
    public static IReadOnlyList<string> Validate(JobDagDefinition dag)
    {
        var errors = new List<string>();
        var registeredAliases = new HashSet<string>(dag.Branches.Select(b => b.Alias), StringComparer.OrdinalIgnoreCase);

        // 1. Identify aliases fed into Processors or Fan-outs
        var fedIntoProcessor = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var fedIntoFrom = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            if (branch.IsProcessor)
            {
                if (!string.IsNullOrEmpty(branch.MainAlias)) fedIntoProcessor.Add(branch.MainAlias);
                foreach (var ra in branch.RefAliases) fedIntoProcessor.Add(ra);
            }
            else if (!string.IsNullOrEmpty(branch.FromAlias))
            {
                fedIntoFrom.Add(branch.FromAlias);
            }
        }

        foreach (var branch in dag.Branches)
        {
            // 2. Prohibit explicit output in branches fed into downstream nodes (orchestrator will use memory channel)
            if ((fedIntoProcessor.Contains(branch.Alias) || fedIntoFrom.Contains(branch.Alias)) && !string.IsNullOrEmpty(branch.Output))
            {
                errors.Add($"Branch '{branch.Alias}' is used as an input for a downstream branch and cannot have its own '--output'.");
            }

            // 3. Topology validation for Processor branches
            if (branch.IsProcessor)
            {
                if (string.IsNullOrEmpty(branch.MainAlias))
                {
                    errors.Add($"Processor branch '{branch.Alias}' is missing its main source alias. Please specify it using '--from <alias>'.");
                }
                else if (!registeredAliases.Contains(branch.MainAlias))
                {
                    errors.Add($"Processor branch '{branch.Alias}' references unknown main alias '{branch.MainAlias}'.");
                }

                foreach (var refAlias in branch.RefAliases)
                {
                    if (!registeredAliases.Contains(refAlias))
                    {
                        errors.Add($"Processor branch '{branch.Alias}' references unknown secondary alias '{refAlias}'.");
                    }
                }
            }
            // Topology validation for Fan-out branches
            else if (!string.IsNullOrEmpty(branch.FromAlias))
            {
                if (!registeredAliases.Contains(branch.FromAlias))
                {
                    errors.Add($"Branch '{branch.Alias}' references unknown upstream alias '{branch.FromAlias}' via '--from'.");
                }
            }
        }

        // 4. Cycle Detection (DFS)
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var stack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var branch in dag.Branches)
        {
            if (HasCycle(branch.Alias, dag.Branches, visited, stack))
            {
                errors.Add($"Circular dependency detected involving branch '{branch.Alias}'. The pipeline must be a Directed Acyclic Graph (DAG).");
                break; // Stop at first cycle found
            }
        }

        return errors;
    }

    private static bool HasCycle(string currentAlias, IReadOnlyList<BranchDefinition> branches, HashSet<string> visited, HashSet<string> stack)
    {
        if (stack.Contains(currentAlias)) return true;
        if (visited.Contains(currentAlias)) return false;

        visited.Add(currentAlias);
        stack.Add(currentAlias);

        var currentBranch = branches.FirstOrDefault(b => b.Alias.Equals(currentAlias, StringComparison.OrdinalIgnoreCase));
        if (currentBranch != null)
        {
            var dependencies = new List<string>();
            if (!string.IsNullOrEmpty(currentBranch.MainAlias)) dependencies.Add(currentBranch.MainAlias);
            if (!string.IsNullOrEmpty(currentBranch.FromAlias)) dependencies.Add(currentBranch.FromAlias);
            dependencies.AddRange(currentBranch.RefAliases);

            foreach (var dep in dependencies)
            {
                if (HasCycle(dep, branches, visited, stack)) return true;
            }
        }

        stack.Remove(currentAlias);
        return false;
    }
}
