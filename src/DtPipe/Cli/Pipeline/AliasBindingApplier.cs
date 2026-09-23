using System;
using System.Collections.Generic;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Substitutes --bind-input/--bind-output onto matching branches' Input/Output, wiring a
/// run-specific location into the 'arrow:-' boundary a fragment (<c>dtpipe split</c>) declares.
/// Applied on the job dictionary, indexed by alias, before <see cref="PipelineValidator"/>: a
/// bound fragment then passes through exactly the same rules as a complete job.
/// </summary>
public static class AliasBindingApplier
{
    private const string ArrowStreamLink = "arrow:-";

    /// <summary>
    /// Mutates <paramref name="jobs"/> in place for every valid pair and returns one error per
    /// invalid one, each naming the branch. An empty result means every pair (if any) applied.
    /// </summary>
    public static List<string> Apply(Dictionary<string, JobDefinition> jobs, string? bindInput, string? bindOutput)
    {
        var errors = new List<string>();
        ApplyDirection(jobs, bindInput, isInput: true, errors);
        ApplyDirection(jobs, bindOutput, isInput: false, errors);
        return errors;
    }

    private static void ApplyDirection(Dictionary<string, JobDefinition> jobs, string? raw, bool isInput, List<string> errors)
    {
        if (string.IsNullOrEmpty(raw)) return;

        var flag = isInput ? "--bind-input" : "--bind-output";
        var seenAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var pair in raw.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            // Cut at the first '=': a location cannot itself carry one before it, but nothing
            // stops it appearing inside a path after it.
            var eq = pair.IndexOf('=');
            if (eq <= 0)
            {
                errors.Add($"{flag} entry '{pair}' is not an alias=location pair.");
                continue;
            }

            var alias = pair[..eq];
            var location = pair[(eq + 1)..];

            if (!seenAliases.Add(alias))
            {
                errors.Add($"{flag} names '{alias}' more than once.");
                continue;
            }

            if (!jobs.TryGetValue(alias, out var job))
            {
                errors.Add($"{flag} names unknown branch '{alias}'.");
                continue;
            }

            var current = isInput ? job.Input : job.Output;

            // Rule 1: only the link binds. The fragment's text says where its boundaries are;
            // binding never redirects a real source or target.
            if (!string.Equals(current, ArrowStreamLink, StringComparison.Ordinal))
            {
                errors.Add($"{flag} targets branch '{alias}', whose {(isInput ? "input" : "output")} is "
                         + $"'{current ?? "(none)"}', not '{ArrowStreamLink}'. Only a branch boundary "
                         + $"written as '{ArrowStreamLink}' can be bound.");
                continue;
            }

            // Rule 2: a location, not an adapter — the boundary stays an Arrow IPC stream. The
            // arrow: reader and writer switch to file format on these two extensions, which would
            // silently change what the binding means.
            if (location.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase)
             || location.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase))
            {
                errors.Add($"{flag} location '{location}' for branch '{alias}' ends in '.arrow' or "
                         + $"'.arrowfile', which the arrow: reader and writer treat as a file rather "
                         + $"than a stream. Name a path, FIFO or named pipe with no such extension.");
                continue;
            }

            var bound = $"arrow:{location}";
            jobs[alias] = isInput ? job with { Input = bound } : job with { Output = bound };
        }
    }
}
