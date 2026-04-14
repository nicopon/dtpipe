using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli;

namespace DtPipe.Cli.Validation;

/// <summary>
/// Central source of truth for CLI semantic rules and constraints.
/// Shared between the parser, validator, and autocompletion engine.
/// </summary>
public static class CliPipelineRules
{
    /// <summary>
    /// Phase 0: Options available at the very start of a command.
    /// </summary>
    public static readonly HashSet<string> StartRules = new(StringComparer.OrdinalIgnoreCase)
    {
        "--job", "--help", "--version", "inspect", "providers", "sql-engines", "completion", "secret",
        "-i", "--input", "--sql", "--from"
    };

    /// <summary>
    /// Phase 1: Options available after an input/processor is defined but before an output.
    /// Derived from <see cref="CoreOptionsBuilder.CoreFlagPhases"/> (Reader phase) plus short aliases.
    /// </summary>
    public static readonly HashSet<string> InputRules = new(
        CoreOptionsBuilder.CoreFlagPhases
            .Where(kv => kv.Value == CliPipelinePhase.Reader)
            .Select(kv => kv.Key)
            .Concat(new[] { "-q", "--alias" }),
        StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Phase 2: Options available after an output is defined.
    /// Derived from <see cref="CoreOptionsBuilder.CoreFlagPhases"/> (Writer phase) plus short aliases.
    /// </summary>
    public static readonly HashSet<string> OutputRules = new(
        CoreOptionsBuilder.CoreFlagPhases
            .Where(kv => kv.Value == CliPipelinePhase.Writer)
            .Select(kv => kv.Key)
            .Concat(new[] { "-s", "-t" }),
        StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Global Options: Available in any phase after initialization.
    /// Derived from <see cref="CoreOptionsBuilder.CoreFlagPhases"/> (Global phase) plus deprecated aliases.
    /// </summary>
    public static readonly HashSet<string> GlobalRules = new(
        CoreOptionsBuilder.CoreFlagPhases
            .Where(kv => kv.Value == CliPipelinePhase.Global)
            .Select(kv => kv.Key)
            .Concat(new[] { "-b", "--sample-rate", "--sample-seed" }),
        StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Flags that can only appear once within a single pipeline branch.
    /// </summary>
    public static readonly HashSet<string> SingletonFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-o", "--output",
        "-q", "--query",
        "--alias",
        "--limit",
        "--batch-size", "-b",
        "--sampling-rate", "--sample-rate",
        "--sampling-seed", "--sample-seed",
        "--strategy", "-s",
        "--insert-mode",
        "--table", "-t",
        "--job"
    };

    /// <summary>
    /// Flags that define the start of a NEW data branch.
    /// </summary>
    public static readonly HashSet<string> PrimarySourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-i", "--input", "--sql", "--from"
    };

    /// <summary>
    /// Flags that are sources but typically used within a stream-transformer branch.
    /// </summary>
    public static readonly HashSet<string> JunctionSourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "--ref"
    };

    /// <summary>
    /// Union of all source-related flags.
    /// </summary>
    public static readonly HashSet<string> SourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-i", "--input", "--sql", "--ref", "--from"
    };

    /// <summary>
    /// Flags that logically terminate a sequence of transformations.
    /// No more transformers should be suggested or allowed after these.
    /// </summary>
    public static readonly HashSet<string> TerminatorFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-o", "--output"
    };

    /// <summary>
    /// Value-bearing processor mode flags. Presence of one of these flags in branch args identifies
    /// the processor type; the following token is the processor's primary argument
    /// (e.g. <c>--sql "&lt;query&gt;"</c> activates the SQL/DataFusion processor).
    /// </summary>
    public static readonly HashSet<string> ValueProcessorFlags = new(StringComparer.OrdinalIgnoreCase) { "--sql" };

    /// <summary>
    /// Boolean processor mode flags (no value). Presence of one of these flags in branch args
    /// identifies the processor type (e.g. <c>--merge</c> activates the MergeTransformer).
    /// </summary>
    public static readonly HashSet<string> BooleanProcessorFlags = new(StringComparer.OrdinalIgnoreCase) { "--merge" };
    public static readonly HashSet<string> InputFlags = new(StringComparer.OrdinalIgnoreCase) { "-i", "--input" };

    /// <summary>
    /// Checks if a flag is a singleton.
    /// </summary>
    public static bool IsSingleton(string flag) => SingletonFlags.Contains(flag);

    /// <summary>
    /// Checks if a flag is a terminator.
    /// </summary>
    public static bool IsTerminator(string flag) => TerminatorFlags.Contains(flag);
}
