namespace DtPipe.Processors;

/// <summary>
/// Shared helpers for extracting flag values from branch argument arrays.
/// </summary>
public static class BranchArgParser
{
    /// <summary>
    /// Returns the first value following <paramref name="flag"/>, or <see langword="null"/> if the flag is absent
    /// or its next token looks like another flag.
    /// </summary>
    public static string? ExtractValue(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (!args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) continue;
            var val = args[i + 1];
            if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;
            return val;
        }
        return null;
    }

    /// <summary>
    /// Returns all values following <paramref name="flag"/> (one per occurrence).
    /// </summary>
    public static IEnumerable<string> ExtractAllValues(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase))
                yield return args[i + 1];
    }
}
