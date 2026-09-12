using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Incremental;

/// <summary>
/// Says when a job declares a cursor that filters nothing.
///
/// <para>
/// <c>cursor:</c> and <c>state:</c> only TRACK the highest value seen — the cursor decorator
/// wraps the writer and records it. What FILTERS is the <c>cursor://</c> interpolation, which the
/// author places: in the reader's query for a source that takes one, otherwise in a filter
/// expression. A job carrying the two keys and no interpolation re-reads its whole source on every
/// run and appends it again — measured at 50 rows, then 100, on the shape a model writes when it
/// reads the two keys and nothing tells it about the third.
/// </para>
/// </summary>
public static class CursorAdvisory
{
    /// <summary>The token that does the filtering, as it is written.</summary>
    private const string Token = "cursor://";

    /// <summary>A scalar loaded from a file, whose content this text cannot see.</summary>
    private static readonly Regex FileReference = new(@":\s*""?@", RegexOptions.Compiled);

    /// <param name="jobs">The parsed branches.</param>
    /// <param name="jobText">The job as written, BEFORE interpolation — afterwards the token is
    /// already replaced by its value and cannot be found.</param>
    public static IReadOnlyList<string> Advise(
        IReadOnlyDictionary<string, JobDefinition> jobs, string? jobText)
    {
        if (jobText is null) return Array.Empty<string>();

        // Either the interpolation is there, or a value comes from a file this text cannot read:
        // in both cases there is nothing to report, and a warning on a working pipeline is worse
        // than a missing one.
        if (jobText.Contains(Token, StringComparison.OrdinalIgnoreCase)) return Array.Empty<string>();
        if (FileReference.IsMatch(jobText)) return Array.Empty<string>();

        return jobs
            .Where(kv => !string.IsNullOrWhiteSpace(kv.Value.Cursor))
            .Select(kv =>
                $"Branch '{kv.Key}' sets 'cursor: {kv.Value.Cursor}', but no '${{{{{Token}...}}}}' appears in the job. "
                + "Those keys only track the highest value seen; what filters is the interpolation — in the "
                + "reader's query for a source that takes one, otherwise in a filter expression. Without it "
                + "every run re-reads the whole source.")
            .ToList();
    }
}
