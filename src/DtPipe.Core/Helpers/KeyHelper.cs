using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Shared helper for parsing primary key specifications from CLI options.
/// Used by all data writers (both BaseSqlDataWriter subclasses and standalone writers like DuckDB).
/// </summary>
public static class KeyHelper
{
    /// <summary>
    /// Parses a comma-separated key specification (e.g. "Id,Name") into a list of trimmed, non-empty column names.
    /// Returns null if the input is null or empty.
    /// </summary>
    public static IReadOnlyList<string>? ParseKeySpec(string? keySpec)
    {
        if (string.IsNullOrEmpty(keySpec)) return null;
        return keySpec.Split(',').Select(k => k.Trim()).Where(k => !string.IsNullOrEmpty(k)).ToList();
    }
}
