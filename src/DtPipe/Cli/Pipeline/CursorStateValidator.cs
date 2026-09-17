namespace DtPipe.Cli.Pipeline;

using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using System;
using System.Collections.Generic;
using System.IO;

/// <summary>
/// Validates incremental-cursor settings across a DAG: the cursor column and its state file are
/// a pair, and no two branches may claim the same state file.
/// Invoked during DAG validation phase, before pipeline execution.
/// </summary>
public static class CursorStateValidator
{
    public static List<string> Validate(
        JobDagDefinition dag,
        Dictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();

        foreach (var branch in dag.Branches)
            if (jobs.TryGetValue(branch.Alias, out var job))
                RejectHalfAPair(errors, branch.Alias, job);

        BranchPathClaims.RejectShared(
            errors, dag, jobs,
            job => job.State,
            "State file",
            "Each writer must have its own --state file.");

        return errors;
    }

    /// <summary>
    /// Refuses a branch that names only one half of the cursor pair.
    /// </summary>
    /// <remarks>
    /// The tracking decorator, the read of the existing state and the two messages that say a
    /// cursor is in play all sit behind one condition requiring both. With a single flag the whole
    /// branch is skipped: nothing is tracked, no file is written, and the only trace that a cursor
    /// was asked for is inside the block that is not taken — exit 0, and the next run reloads
    /// everything. Both surfaces feed the same JobDefinition, so refusing here covers the command
    /// line and the job file at once.
    /// </remarks>
    private static void RejectHalfAPair(List<string> errors, string alias, JobDefinition job)
    {
        bool hasCursor = !string.IsNullOrEmpty(job.Cursor);
        bool hasState = !string.IsNullOrEmpty(job.State);
        if (hasCursor == hasState) return;

        var (given, givenValue, missing) = hasCursor
            ? ("--cursor", job.Cursor!, "--state <file>")
            : ("--state", job.State!, "--cursor <column>");

        errors.Add(
            $"Branch '{alias}' declares {given} '{givenValue}' without {missing}. "
            + "Incremental loading needs both: the column to track and the file that persists it. "
            + "In a job file the keys are 'cursor:' and 'state:'.");
    }
}
