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
        //    entirely. Writing '--query' where a processor was meant produces exactly that shape.
        foreach (var branch in dag.Branches)
        {
            if (branch.RefAliases.Count == 0 || branch.HasStreamTransformer) continue;

            var refs = string.Join(",", branch.RefAliases);
            errors.Add($"Branch '{branch.Alias}' declares ref '{refs}' but runs no stream processor, "
                     + $"so '{refs}' would be read and discarded. A ref exists for a processor to query: "
                     + $"add one to this branch{ProcessorTriggers(processorFactories)}, or drop the ref.");
        }

        // 4. A stream-processor branch consumes the branches it names, never an input of its own:
        //    the orchestrator injects no reader into it, and the input is not merely ignored — it
        //    is never opened. Measured: a processor branch pointed at a path that does not exist
        //    runs to completion and exits 0, while the same path without a processor fails on
        //    FileNotFoundException. So an input declared here is a source nothing reads, and
        //    nothing said so.
        //
        //    Written against HasStreamTransformer rather than any flag, so a processor added
        //    later is covered without editing this.
        foreach (var branch in dag.Branches)
        {
            if (!branch.HasStreamTransformer || string.IsNullOrEmpty(branch.Input)) continue;

            var input = DtPipe.Core.Security.ConnectionStringSanitizer.Redact(branch.Input);
            errors.Add($"Branch '{branch.Alias}' runs the '{branch.ProcessorName}' processor and also "
                     + $"declares the input '{input}', which a processor branch never reads — it is not "
                     + $"even opened. Give that source a branch of its own and name it here "
                     + $"(--from <alias> on the command line, 'from:' in a job file), or drop the input.");
        }

        // 5. Loop detection
        if (HasCycle(dag))
            errors.Add("Circular dependency detected in pipeline graph.");

        // 6. Cursor state file uniqueness
        errors.AddRange(CursorStateValidator.Validate(dag, jobs));

        return errors;
    }

    /// <summary>
    /// The processor triggers the catalogue actually carries, with the value each one takes.
    /// Naming one in a message would be wrong the day a processor is added, renamed or retired,
    /// and nothing would catch it — the factories already declare their own triggers.
    /// </summary>
    private static string ProcessorTriggers(IEnumerable<IStreamTransformerFactory> processorFactories)
    {
        var triggers = processorFactories
            .SelectMany(f => f.CliTriggerFlags)
            .Select(t => t.IsBoolean ? t.Flag : $"{t.Flag} <value>")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();

        return triggers.Count == 0 ? string.Empty : $" ({string.Join(", ", triggers)})";
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
