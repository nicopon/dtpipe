namespace DtPipe.Cli.Pipeline;

using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using System;
using System.Collections.Generic;
using System.IO;

/// <summary>
/// Validates that no two branches in a DAG claim the same --state file path.
/// Invoked during DAG validation phase, before pipeline execution.
/// </summary>
public static class CursorStateValidator
{
    public static List<string> Validate(
        JobDagDefinition dag,
        Dictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();

        var stateFiles = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var branch in dag.Branches)
        {
            if (jobs.TryGetValue(branch.Alias, out var job)
                && !string.IsNullOrEmpty(job.State))
            {
                try
                {
                    var fullPath = Path.GetFullPath(job.State);
                    if (stateFiles.TryGetValue(fullPath, out var existingAlias))
                    {
                        errors.Add(
                            $"State file '{job.State}' is claimed by both branch '{existingAlias}' "
                            + $"and branch '{branch.Alias}'. Each writer must have its own --state file.");
                    }
                    else
                    {
                        stateFiles[fullPath] = branch.Alias;
                    }
                }
                catch (Exception)
                {
                    // If path is invalid, let validation pass or handle elsewhere.
                    // For safety, just use the string itself as a fallback.
                    if (stateFiles.TryGetValue(job.State, out var existingAlias))
                    {
                        errors.Add(
                            $"State file '{job.State}' is claimed by both branch '{existingAlias}' "
                            + $"and branch '{branch.Alias}'. Each writer must have its own --state file.");
                    }
                    else
                    {
                        stateFiles[job.State] = branch.Alias;
                    }
                }
            }
        }

        return errors;
    }
}
