namespace DtPipe.Adapters.Arrow;

/// <summary>
/// The <c>pipe://&lt;name&gt;</c> form <c>arrow:</c> accepts as a location, alongside a plain file
/// path or <c>-</c> for stdio. Client-only, deliberately: the pipeline node that wires a fragment's
/// excess edge always creates the named pipe as server before launching the child carrying this
/// location, so <c>arrow:</c> itself never needs the server role, in either direction.
/// </summary>
internal static class ArrowPipeLocation
{
    private const string Scheme = "pipe://";

    public static bool TryParse(string location, out string pipeName)
    {
        if (location.StartsWith(Scheme, StringComparison.Ordinal) && location.Length > Scheme.Length)
        {
            pipeName = location[Scheme.Length..];
            return true;
        }

        pipeName = "";
        return false;
    }
}
