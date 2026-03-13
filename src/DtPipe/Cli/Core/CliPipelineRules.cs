using System;
using System.Collections.Generic;

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
        "--job", "--help", "--version", "inspect", "providers", "completion", "secret",
        "-i", "--input", "--sql", "--from"
    };

    /// <summary>
    /// Phase 1: Options available after an input/processor is defined but before an output.
    /// </summary>
    public static readonly HashSet<string> InputRules = new(StringComparer.OrdinalIgnoreCase)
    {
        "-q", "--query", "--connection-timeout", "--query-timeout", "--unsafe-query", "--alias"
    };

    /// <summary>
    /// Phase 2: Options available after an output is defined.
    /// </summary>
    public static readonly HashSet<string> OutputRules = new(StringComparer.OrdinalIgnoreCase)
    {
        "-s", "--strategy", "--table", "-t", "--pre-exec", "--post-exec", "--on-error-exec",
        "--finally-exec", "--insert-mode", "--strict-schema", "--no-schema-val", "--auto-migrate"
    };

    /// <summary>
    /// Global Options: Available in any phase after initialization.
    /// </summary>
    public static readonly HashSet<string> GlobalRules = new(StringComparer.OrdinalIgnoreCase)
    {
        "-b", "--batch-size", "--limit", "--dry-run", "--no-stats",
        "--sample-rate", "--sampling-rate", "--sample-seed", "--sampling-seed",
        "--log", "--key", "--max-retries", "--retry-delay-ms", "--metrics-path", "--export-job"
    };

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
        "--job",
        "--main"
    };

    /// <summary>
    /// Flags that define the start of a NEW data branch.
    /// </summary>
    public static readonly HashSet<string> PrimarySourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-i", "--input", "--sql", "--from"
    };

    /// <summary>
    /// Flags that are sources but typically used within a Processor branch.
    /// </summary>
    public static readonly HashSet<string> JunctionSourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "--main", "--ref"
    };

    /// <summary>
    /// Union of all source-related flags.
    /// </summary>
    public static readonly HashSet<string> SourceFlags = new(StringComparer.OrdinalIgnoreCase)
    {
        "-i", "--input", "--sql", "--main", "--ref", "--from"
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
    /// Flags that define a new branch or a Processor junction.
    /// </summary>
    public static readonly HashSet<string> ProcessorFlags = new(StringComparer.OrdinalIgnoreCase) { "--sql" };
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
