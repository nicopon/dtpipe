using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Validation;

/// <summary>
/// Validates Directed Acyclic Graph (DAG) structures independently of the input source (CLI or YAML).
/// Handles alias resolution, cycle detection, and structural constraints like output usage.
/// Pass processor factories to <see cref="Validate"/> to also validate stream/lookup capacity constraints.
/// </summary>
public static class DagValidator
{
    public static IReadOnlyList<string> Validate(JobDagDefinition dag,
        IEnumerable<IStreamTransformerFactory>? processorFactories = null)
    {
        var errors = new List<string>();
        var registeredAliases = new HashSet<string>(dag.Branches.Select(b => b.Alias), StringComparer.OrdinalIgnoreCase);
        var factories = processorFactories?.ToList();

        // 1. Identify aliases fed into stream transformers or fan-outs
        var fedIntoTransformer = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var fedIntoFrom = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            if (branch.HasStreamTransformer)
            {
                foreach (var a in branch.StreamingAliases) fedIntoTransformer.Add(a);
                foreach (var ra in branch.RefAliases) fedIntoTransformer.Add(ra);
            }
            else if (branch.StreamingAliases.Count > 0)
            {
                fedIntoFrom.Add(branch.StreamingAliases[0]);
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
                if (branch.StreamingAliases.Count == 0)
                {
                    errors.Add($"Stream-transformer branch '{branch.Alias}' is missing its main source alias. Please specify it using '--from <alias>'.");
                }
                else
                {
                    foreach (var streamAlias in branch.StreamingAliases)
                    {
                        if (!registeredAliases.Contains(streamAlias))
                            errors.Add($"Stream-transformer branch '{branch.Alias}' references unknown streaming alias '{streamAlias}'.");
                    }
                }

                foreach (var refAlias in branch.RefAliases)
                {
                    if (!registeredAliases.Contains(refAlias))
                        errors.Add($"Stream-transformer branch '{branch.Alias}' references unknown secondary alias '{refAlias}'.");
                }

                // 4. Processor capability validation (if factories provided)
                if (factories != null)
                {
                    var factory = factories.FirstOrDefault(f => f.IsApplicable(branch.Arguments));
                    if (factory != null)
                    {
                        int streams = branch.StreamingAliases.Count;
                        int lookups = branch.RefAliases.Count;

                        if (streams < factory.MinStreams)
                            errors.Add($"Processor '{factory.ComponentName}' on branch '{branch.Alias}' requires at least {factory.MinStreams} streaming source(s) via '--from', but got {streams}.");
                        if (factory.MaxStreams >= 0 && streams > factory.MaxStreams)
                            errors.Add($"Processor '{factory.ComponentName}' on branch '{branch.Alias}' accepts at most {factory.MaxStreams} streaming source(s) via '--from', but got {streams}.");
                        if (lookups < factory.MinLookups)
                            errors.Add($"Processor '{factory.ComponentName}' on branch '{branch.Alias}' requires at least {factory.MinLookups} materialized lookup(s) via '--ref', but got {lookups}.");
                        if (factory.MaxLookups >= 0 && lookups > factory.MaxLookups)
                            errors.Add($"Processor '{factory.ComponentName}' on branch '{branch.Alias}' accepts at most {factory.MaxLookups} materialized lookup(s) via '--ref', but got {lookups}.");
                    }
                }
            }
            // Topology validation for fan-out branches
            else if (branch.StreamingAliases.Count > 0)
            {
                if (!registeredAliases.Contains(branch.StreamingAliases[0]))
                    errors.Add($"Branch '{branch.Alias}' references unknown upstream alias '{branch.StreamingAliases[0]}' via '--from'.");
            }
        }

        // 5. Cycle Detection (DFS)
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
            var dependencies = new List<string>(currentBranch.StreamingAliases);
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
