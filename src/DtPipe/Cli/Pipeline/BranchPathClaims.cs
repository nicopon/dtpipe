namespace DtPipe.Cli.Pipeline;

using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using System;
using System.Collections.Generic;
using System.IO;

/// <summary>
/// One rule, applied wherever a branch names a file it alone may write: two branches of the same
/// DAG must not claim the same path.
/// </summary>
/// <remarks>
/// The branches of a DAG run concurrently, so a shared path is not a last-writer-wins situation
/// but two writers interleaving into one file. <c>--metrics-path</c> is the measured case: the
/// JSON comes out corrupt.
///
/// <para>
/// This lives apart from its callers because the alternative is a copy per flag. The
/// <c>--state</c> check came first and the contract one is the same check with another label —
/// writing it twice is how the two would drift, one of them gaining the path normalisation and
/// the other not.
/// </para>
/// </remarks>
public static class BranchPathClaims
{
    /// <summary>
    /// Adds an error for every path two branches both claim through <paramref name="select"/>.
    /// </summary>
    /// <param name="label">How the file is named to the user, e.g. "State file".</param>
    /// <param name="advice">The sentence telling them what to do, ending in a full stop.</param>
    public static void RejectShared(
        List<string> errors,
        JobDagDefinition dag,
        Dictionary<string, JobDefinition> jobs,
        Func<JobDefinition, string?> select,
        string label,
        string advice)
    {
        var claimed = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            if (!jobs.TryGetValue(branch.Alias, out var job)) continue;

            var raw = select(job);
            if (string.IsNullOrEmpty(raw)) continue;

            // Compare resolved paths so './a.json' and 'a.json' are one claim. An unresolvable
            // path is compared as written rather than skipped: a duplicate is still a duplicate,
            // and refusing here beats letting two branches discover it at write time.
            string key;
            try { key = Path.GetFullPath(raw); }
            catch (Exception) { key = raw; }

            if (claimed.TryGetValue(key, out var existingAlias))
                errors.Add($"{label} '{raw}' is claimed by both branch '{existingAlias}' "
                         + $"and branch '{branch.Alias}'. {advice}");
            else
                claimed[key] = branch.Alias;
        }
    }
}
