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

    /// <summary>
    /// Returns the first positional argument (not a flag and not a value for a flag).
    /// </summary>
    public static string? GetPositionalQuery(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.StartsWith('-'))
            {
                // Simple heuristic: skip the next arg if this flag usually takes a value.
                // We check if it's one of the common value-bearing flags.
                if (IsValueFlag(arg) && i + 1 < args.Length)
                {
                    i++;
                }
                continue;
            }

            // This is a positional argument
            return arg;
        }
        return null;
    }

    private static bool IsValueFlag(string flag)
    {
        var f = flag.ToLowerInvariant();
        return f is "--from" or "--alias" or "--output" or "-o" or "--sql" or "--ref" or "--input" or "-i" or "--query" or "-q" or "--table" or "-t" or "--limit" or "--batch-size" or "-b";
    }
}
