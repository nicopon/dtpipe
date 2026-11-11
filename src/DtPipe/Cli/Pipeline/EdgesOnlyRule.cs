using System;
using System.Collections.Generic;
using DtPipe.Core.Models;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// What <c>--edges-only</c> lets a job reach: its <c>arrow:</c> edges and nothing else. A pipeline node gives the flag to
/// a runner's child, whose host the fragments of every run share, so a file, a database, a checkpoint or a cursor file
/// would carry rows between two flows the coordinator never judged together. The text of the job names only
/// <c>arrow:-</c>; a named pipe is a location the node binds on the command line (<see cref="AliasBindingApplier"/>), so
/// <see cref="TextRefusals"/> reads the job before the binding and <see cref="BoundRefusals"/> after it. A file named
/// bare, with no directory, is one of the working directory, which the node gives the child as its own: a bound location
/// may be one (a runner's dry run binds its samples there), and so may a <c>contract-save</c>, which writes a schema and
/// no row.
/// </summary>
public static class EdgesOnlyRule
{
    private const string Edge = "arrow:-";
    private const string BoundPipe = "arrow:pipe://";
    private const string Why = "under --edges-only a branch reads and writes its arrow:- edges only";

    /// <summary>Every reader, writer or place on disk the job's text names that is no edge, each naming its branch.</summary>
    public static List<string> TextRefusals(IReadOnlyDictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();
        foreach (var (alias, job) in jobs)
        {
            if (!string.IsNullOrEmpty(job.Input) && job.Input != Edge)
                errors.Add($"'{alias}' reads '{ConnectionStringSanitizer.Redact(job.Input)}': {Why}.");
            if (!string.IsNullOrEmpty(job.Output) && job.Output != Edge)
                errors.Add($"'{alias}' writes '{ConnectionStringSanitizer.Redact(job.Output)}': {Why}.");
            foreach (var (key, value) in Places(job))
                if (!string.IsNullOrEmpty(value)) errors.Add($"'{alias}' sets {key} '{value}': {Why}.");
            if (!string.IsNullOrEmpty(job.ContractSave) && !IsBareFile(job.ContractSave))
                errors.Add($"'{alias}' sets contract-save '{job.ContractSave}': {Why}, and a contract saved in the working directory.");
        }
        return errors;
    }

    /// <summary>Every endpoint the binding left that is neither the process's stdio nor a named pipe.</summary>
    public static List<string> BoundRefusals(IReadOnlyDictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();
        foreach (var (alias, job) in jobs)
        {
            foreach (var (side, endpoint) in new[] { ("reads", job.Input), ("writes", job.Output) })
            {
                if (string.IsNullOrEmpty(endpoint) || endpoint == Edge
                    || endpoint.StartsWith(BoundPipe, StringComparison.Ordinal) && endpoint.Length > BoundPipe.Length
                    || endpoint.StartsWith("arrow:", StringComparison.Ordinal) && IsBareFile(endpoint["arrow:".Length..])) continue;
                errors.Add($"'{alias}' {side} '{ConnectionStringSanitizer.Redact(endpoint)}' once bound: {Why}, bound to a named pipe or a file of the working directory.");
            }
        }
        return errors;
    }

    private static bool IsBareFile(string name) =>
        name.Length > 0 && name is not "." and not ".." && name.IndexOfAny(['/', '\\', ':']) < 0;

    // What a branch keeps on the host's disk beside its reader and writer.
    private static (string Key, string? Value)[] Places(JobDefinition job) =>
    [
        ("checkpoint", job.Checkpoint), ("from-checkpoint", job.FromCheckpoint), ("from-contract", job.FromContract), ("metrics-path", job.MetricsPath), ("cursor", job.Cursor), ("state", job.State),
    ];
}
