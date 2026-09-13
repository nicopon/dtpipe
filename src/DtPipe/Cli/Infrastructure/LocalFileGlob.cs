namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Expands a local path whose last segment carries <c>*</c> or <c>?</c> into the files it matches.
/// </summary>
/// <remarks>
/// Object storage globs because DuckDB does it for us (<c>s3://bucket/dt=*/part-*.parquet</c>);
/// a local path had no equivalent and raised FileNotFoundException on the pattern itself, so a
/// directory of daily files needed a shell loop.
///
/// The wildcard is allowed in the file name only. A pattern spanning directories would have to
/// settle what <c>**</c> means and how far it walks, and neither answer is obvious enough to
/// guess; matching one directory covers the case that came up and leaves the rest open.
/// </remarks>
public static class LocalFileGlob
{
    private static readonly char[] Wildcards = ['*', '?'];

    /// <summary>
    /// Whether <paramref name="input"/> is a local path with a wildcard in its file name. A remote
    /// URI is never one: the scheme's own provider globs it.
    /// </summary>
    public static bool IsPattern(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return false;
        if (input.Contains("://")) return false;

        var name = Path.GetFileName(input);
        return name.IndexOfAny(Wildcards) >= 0;
    }

    /// <summary>
    /// The files <paramref name="pattern"/> matches, in ordinal path order so two runs read them
    /// the same way. Throws when nothing matches: an empty expansion silently reading zero rows
    /// is worse than the FileNotFoundException it replaces.
    /// </summary>
    public static IReadOnlyList<string> Expand(string pattern)
    {
        var directory = Path.GetDirectoryName(pattern);
        if (string.IsNullOrEmpty(directory)) directory = ".";

        if (!Directory.Exists(directory))
            throw new DirectoryNotFoundException($"No directory '{directory}' to match '{pattern}' in.");

        var matches = Directory.GetFiles(directory, Path.GetFileName(pattern));
        Array.Sort(matches, StringComparer.Ordinal);

        if (matches.Length == 0)
            throw new FileNotFoundException($"No file matches '{pattern}'.", pattern);

        return matches;
    }
}
