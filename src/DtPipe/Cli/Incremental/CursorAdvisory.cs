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

    /// <summary>
    /// A scalar loaded from a file, whose content this text cannot see. One pattern for both
    /// surfaces: a value opens after a YAML <c>key:</c> or after the whitespace that separates
    /// command-line tokens, so <c>query: "@q.sql"</c>, <c>query: @q.sql</c> and
    /// <c>--query @q.sql</c> are the same shape. Silencing on it is deliberate — a warning on a
    /// pipeline that works costs more than one that is missing.
    /// </summary>
    private static readonly Regex FileReference = new(@"(?:^|[:\s])\s*""?@", RegexOptions.Compiled);

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

    /// <summary>
    /// The same rule over a command line, where what the author wrote is the token array.
    /// </summary>
    /// <remarks>
    /// The two surfaces describe one pipeline — <c>--export-job</c> translates a command line into
    /// the very YAML the file path reads — so a cursor that filters nothing had to be reported on
    /// both or the report means nothing. It was reported on neither until the command line joined:
    /// the run printed <c>Cursor saved</c> on every pass, which reads as success, while the next
    /// one re-read the whole source.
    /// </remarks>
    public static IReadOnlyList<string> AdviseCommandLine(
        IReadOnlyDictionary<string, JobDefinition> jobs, IEnumerable<string>? args)
        => args is null ? Array.Empty<string>() : Advise(jobs, string.Join(' ', args));
}
