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

        // 2. Loop detection
        if (HasCycle(dag))
            errors.Add("Circular dependency detected in pipeline graph.");

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
