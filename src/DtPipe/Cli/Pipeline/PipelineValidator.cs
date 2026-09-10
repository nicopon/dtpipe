using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Pipeline;

public static class PipelineValidator
{
    public static List<string> Validate(JobDagDefinition dag, Dictionary<string, JobDefinition> jobs, IEnumerable<IStreamTransformerFactory> processorFactories)
    {
        var errors = new List<string>();

        // 1. Topology Validation
        var definedAliases = dag.Branches.Select(b => b.Alias).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var branch in dag.Branches)
        {
            foreach (var from in branch.StreamingAliases)
            {
                if (!definedAliases.Contains(from))
                    errors.Add($"Branch '{branch.Alias}' depends on unknown source '{from}'.");
            }

            foreach (var @ref in branch.RefAliases)
            {
                if (!definedAliases.Contains(@ref))
                    errors.Add($"Branch '{branch.Alias}' refers to unknown branch '{@ref}'.");
            }
        }

        // 2. A source of 'from' / 'ref' must be a branch that publishes a channel, and a branch
        //    publishes one only when it has no 'output:' of its own (DagOrchestrator pre-registers
        //    channels for output-less branches). Without this the run reaches the processor and
        //    dies on "An Arrow channel with the alias 'x' is not registered", which names an
        //    internal object and neither the cause nor the fix. Building two related tables is the
        //    shape that hits it: the table everyone reads must be produced by one branch and
        //    written by another.
        foreach (var branch in dag.Branches)
        {
            foreach (var upstream in branch.StreamingAliases.Concat(branch.RefAliases).Distinct(StringComparer.OrdinalIgnoreCase))
            {
                if (!definedAliases.Contains(upstream)) continue;   // already reported above
                if (!jobs.TryGetValue(upstream, out var source) || string.IsNullOrEmpty(source.Output)) continue;

                errors.Add($"Branch '{branch.Alias}' reads branch '{upstream}', but '{upstream}' has an "
                         + $"'output:' of its own and therefore publishes nothing to read. A branch either "
                         + $"writes to a target or feeds other branches, never both: drop the 'output:' from "
                         + $"'{upstream}' and add a branch that does the writing ('from: {upstream}' plus the "
                         + $"'output:').");
            }
        }

        // 3. A 'ref' is materialised for a stream processor to query, so a branch declaring one
        //    without a processor has no consumer for it: the referenced branch was read in full
        //    and discarded, and the run reported success over a result missing its columns
        //    entirely. Writing '--query' where '--sql' was meant produces exactly that shape.
        foreach (var branch in dag.Branches)
        {
            if (branch.RefAliases.Count == 0 || branch.HasStreamTransformer) continue;

            var refs = string.Join(",", branch.RefAliases);
            errors.Add($"Branch '{branch.Alias}' declares ref '{refs}' but runs no stream processor, "
                     + $"so '{refs}' would be read and discarded. A ref exists for a processor to query: "
                     + $"add --sql \"<query>\" (or another processor flag) to this branch, or drop the ref.");
        }

        // 4. Loop detection
        if (HasCycle(dag))
            errors.Add("Circular dependency detected in pipeline graph.");

        // 5. Cursor state file uniqueness
        errors.AddRange(CursorStateValidator.Validate(dag, jobs));

        return errors;
    }

    private static bool HasCycle(JobDagDefinition dag)
    {
        var adj = dag.Branches.ToDictionary(
            b => b.Alias, 
            b => b.StreamingAliases.Concat(b.RefAliases).ToList(),
            StringComparer.OrdinalIgnoreCase);

        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var stack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var node in adj.Keys)
        {
            if (CheckCycle(node, adj, visited, stack)) return true;
        }

        return false;
    }

    private static bool CheckCycle(string node, Dictionary<string, List<string>> adj, HashSet<string> visited, HashSet<string> stack)
    {
        if (stack.Contains(node)) return true;
        if (visited.Contains(node)) return false;

        visited.Add(node);
        stack.Add(node);

        if (adj.TryGetValue(node, out var neighbors))
        {
            foreach (var neighbor in neighbors)
            {
                if (CheckCycle(neighbor, adj, visited, stack)) return true;
            }
        }

        stack.Remove(node);
        return false;
    }
}
