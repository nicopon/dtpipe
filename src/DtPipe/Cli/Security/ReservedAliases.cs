namespace DtPipe.Cli.Security;

/// <summary>
/// The aliases the studio keeps its own secrets under. They are bound to an address and are the studio's: no list
/// of aliases shows them, since the preview of a key has no place there.
/// </summary>
public static class ReservedAliases
{
    public const string StudioPrefix = "studio.";

    public static bool IsReserved(string alias) => alias.StartsWith(StudioPrefix, StringComparison.Ordinal);

    /// <summary>The aliases a person may name, in the order given.</summary>
    public static IEnumerable<KeyValuePair<string, string>> Visible(IEnumerable<KeyValuePair<string, string>> secrets) =>
        secrets.Where(s => !IsReserved(s.Key));
}
