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

        // 1. Identify aliases fed into stream transformers or fan-outs
        var fedIntoTransformer = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var fedIntoFrom = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            if (branch.HasStreamTransformer)
            {
                if (!string.IsNullOrEmpty(branch.FromAlias)) fedIntoTransformer.Add(branch.FromAlias);
                foreach (var ra in branch.RefAliases) fedIntoTransformer.Add(ra);
                foreach (var ma in branch.MergeAliases) fedIntoTransformer.Add(ma);
            }
            else if (!string.IsNullOrEmpty(branch.FromAlias))
            {
                fedIntoFrom.Add(branch.FromAlias);
            }
        }

        foreach (var branch in dag.Branches)
        {
            // 2. Prohibit explicit output in branches fed into downstream nodes
            if ((fedIntoTransformer.Contains(branch.Alias) || fedIntoFrom.Contains(branch.Alias))
                && !string.IsNullOrEmpty(branch.Output))
            {
                errors.Add($"Branch '{branch.Alias}' is used as an input for a downstream branch and cannot have its own '--output'.");
            }

            // 3. Topology validation for stream-transformer branches
            if (branch.HasStreamTransformer)
            {
                if (string.IsNullOrEmpty(branch.FromAlias))
                {
                    errors.Add($"Stream-transformer branch '{branch.Alias}' is missing its main source alias. Please specify it using '--from <alias>'.");
                }
                else if (!registeredAliases.Contains(branch.FromAlias))
                {
                    errors.Add($"Stream-transformer branch '{branch.Alias}' references unknown main alias '{branch.FromAlias}'.");
                }

                foreach (var refAlias in branch.RefAliases)
                {
                    if (!registeredAliases.Contains(refAlias))
                        errors.Add($"Stream-transformer branch '{branch.Alias}' references unknown secondary alias '{refAlias}'.");
                }

                foreach (var mergeAlias in branch.MergeAliases)
                {
                    if (!registeredAliases.Contains(mergeAlias))
                        errors.Add($"Stream-transformer branch '{branch.Alias}' references unknown merge alias '{mergeAlias}'.");
                }
            }
            // Topology validation for fan-out branches
            else if (!string.IsNullOrEmpty(branch.FromAlias))
            {
                if (!registeredAliases.Contains(branch.FromAlias))
                    errors.Add($"Branch '{branch.Alias}' references unknown upstream alias '{branch.FromAlias}' via '--from'.");
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
                break;
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
            if (!string.IsNullOrEmpty(currentBranch.FromAlias)) dependencies.Add(currentBranch.FromAlias);
            dependencies.AddRange(currentBranch.RefAliases);
            dependencies.AddRange(currentBranch.MergeAliases);

            foreach (var dep in dependencies)
            {
                if (HasCycle(dep, branches, visited, stack)) return true;
            }
        }

        stack.Remove(currentAlias);
        return false;
    }
}
