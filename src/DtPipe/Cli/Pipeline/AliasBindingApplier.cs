using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Substitutes --bind-input/--bind-output onto matching branches' Input/Output, wiring a
/// run-specific location into the 'arrow:-' boundary a fragment declares.
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
    public static List<string> Apply(Dictionary<string, JobDefinition> jobs, string? bindInput, string? bindOutput,
        string? bindCompression = null, string? bindCompressionLevel = null)
    {
        var errors = new List<string>();
        var compression = ParseCompression(bindCompression, bindCompressionLevel, errors);
        ApplyDirection(jobs, bindInput, isInput: true, errors, null);
        ApplyDirection(jobs, bindOutput, isInput: false, errors, compression);
        // An output still written 'arrow:-' is the boundary that rides the process's stdout: it is as
        // much a stream edge as a bound one, and the codec follows it. No other output is touched.
        if (compression is not null)
            foreach (var (alias, job) in jobs.ToList())
                if (string.Equals(job.Output, ArrowStreamLink, StringComparison.Ordinal))
                    jobs[alias] = WithCompression(job, compression);
        return errors;
    }

    /// <summary>The codec and level --bind-compression gives the outputs --bind-output binds.</summary>
    private sealed record BoundCompression(string Codec, int? Level);

    private static BoundCompression? ParseCompression(string? codec, string? level, List<string> errors)
    {
        if (string.IsNullOrEmpty(codec))
        {
            if (!string.IsNullOrEmpty(level)) errors.Add("--bind-compression-level needs --bind-compression.");
            return null;
        }
        try
        {
            DtPipe.Adapters.Arrow.ArrowAdapterDataWriter.ParseCompression(codec);
        }
        catch (ArgumentException ex)
        {
            errors.Add($"--bind-compression: {ex.Message}");
            return null;
        }

        int? parsed = null;
        if (!string.IsNullOrEmpty(level))
        {
            if (!int.TryParse(level, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var n))
            {
                errors.Add($"--bind-compression-level '{level}' is not an integer.");
                return null;
            }
            parsed = n;
        }
        return new BoundCompression(codec.Trim().ToLowerInvariant(), parsed);
    }

    // The arrow writer's options key for a bound output. A fragment that already names a compression
    // for its boundary keeps it: the binding supplies a default for the transport, not an override.
    private static JobDefinition WithCompression(JobDefinition job, BoundCompression compression)
    {
        var options = job.ProviderOptions is null
            ? new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, Dictionary<string, object?>>(job.ProviderOptions, StringComparer.OrdinalIgnoreCase);

        foreach (var key in new[] { "arrow", "arrow-writer" })
        {
            if (options.TryGetValue(key, out var existing)
                && existing.Keys.Any(k => k.Replace("-", "").Replace("_", "").Equals("compression", StringComparison.OrdinalIgnoreCase)))
                return job;
        }

        var block = options.TryGetValue("arrow-writer", out var current)
            ? new Dictionary<string, object?>(current)
            : new Dictionary<string, object?>();
        block["compression"] = compression.Codec;
        if (compression.Level is { } level) block["compression-level"] = level;
        options["arrow-writer"] = block;
        return job with { ProviderOptions = options };
    }

    private static void ApplyDirection(Dictionary<string, JobDefinition> jobs, string? raw, bool isInput, List<string> errors,
        BoundCompression? compression)
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
                         + $"than a stream. Name a path or a named pipe (pipe://<name>) with no such extension.");
                continue;
            }

            var bound = $"arrow:{location}";
            jobs[alias] = isInput
                ? job with { Input = bound }
                : compression is null ? job with { Output = bound } : WithCompression(job with { Output = bound }, compression);
        }
    }
}
