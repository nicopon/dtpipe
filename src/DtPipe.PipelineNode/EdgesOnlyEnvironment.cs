namespace DtPipe.PipelineNode;

/// <summary>
/// The environment of a child run under <c>--edges-only</c>: of the host's variables it keeps only what a process needs to start
/// and find its runtime, never what a deployment configures. Its job is a designer's text, so a key, a connection string or the
/// settings of a coordinator beside the runner never reach it; what the node gives the run on purpose is set after.
/// </summary>
public static class EdgesOnlyEnvironment
{
    public const string Flag = "--edges-only";

    private static readonly HashSet<string> Kept = new(StringComparer.OrdinalIgnoreCase)
    {
        "PATH", "HOME", "USER", "LOGNAME", "LANG", "LANGUAGE", "TZ", "TMPDIR", "TEMP", "TMP",
        "SystemRoot", "windir", "SystemDrive", "ComSpec", "PATHEXT", "USERPROFILE", "USERNAME", "LOCALAPPDATA", "APPDATA",
        "ProgramData", "ProgramFiles", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "OS",
    };

    private static readonly string[] KeptPrefixes = ["LC_", "DOTNET_"];

    /// <summary>Removes every variable the child does not keep.</summary>
    public static void Restrict(IDictionary<string, string?> environment)
    {
        foreach (var name in environment.Keys.Where(n => !Keeps(n)).ToList())
            environment.Remove(name);
    }

    public static bool Keeps(string name) =>
        Kept.Contains(name) || KeptPrefixes.Any(p => name.StartsWith(p, StringComparison.OrdinalIgnoreCase));
}
