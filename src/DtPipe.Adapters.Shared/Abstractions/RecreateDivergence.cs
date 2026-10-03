namespace DtPipe.Adapters.Shared.Abstractions;

/// <summary>
/// What <c>--strategy Recreate</c> tells the user when the table it replaces is not the table the
/// source describes. Recreate drops the target and builds it from the source schema, so a column
/// the source lacks disappears and a key the source does not name is not carried over; saying so
/// is the only thing standing between that and a silent loss. One rule for every SQL writer.
/// </summary>
/// <remarks>
/// Names are compared without regard to case: each engine folds identifiers its own way, and the
/// target reports them as it stores them, so a strict comparison would flag every column of an
/// Oracle table. Types are not compared: mapping a native type back to a CLR one is the guess the
/// engine core is not allowed to make.
/// </remarks>
public static class RecreateDivergence
{
    /// <summary>The warning to show, or null when the replaced table matches the source.</summary>
    public static string? Describe(
        string table,
        IEnumerable<string> existingColumns,
        IEnumerable<string> sourceColumns,
        IReadOnlyList<string>? existingPrimaryKey,
        bool sourceNamesAKey)
    {
        var existing = existingColumns.ToList();
        var source = sourceColumns.ToList();

        var dropped = existing.Where(c => !source.Contains(c, StringComparer.OrdinalIgnoreCase)).ToList();
        var added = source.Where(c => !existing.Contains(c, StringComparer.OrdinalIgnoreCase)).ToList();
        var lostKey = !sourceNamesAKey && existingPrimaryKey is { Count: > 0 } ? existingPrimaryKey : null;

        if (dropped.Count == 0 && added.Count == 0 && lostKey is null) return null;

        var parts = new List<string>();
        if (dropped.Count > 0)
            parts.Add($"column(s) {string.Join(", ", dropped)} are not in the source and are dropped");
        if (added.Count > 0)
            parts.Add($"column(s) {string.Join(", ", added)} are new");
        if (lostKey is not null)
            parts.Add($"its primary key ({string.Join(", ", lostKey)}) is not recreated; pass --key to give the new table one");

        return $"Recreate rebuilds '{table}' from the source schema, and the existing table differs: "
             + string.Join("; ", parts) + ".";
    }

    public static void Warn(string message) => Console.Error.WriteLine($"[dtpipe] Warning: {message}");
}
