namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// Which dtpipe a node runs. A host that names one (<see cref="DtPipeNodeOptions.DtPipe"/>) gets exactly that. A host that
/// leaves the default gets the <c>dtpipe</c> tool package its build placed beside it, when there is one, and else
/// <c>dtpipe</c> on the PATH.
/// </summary>
internal static class DtPipeLocation
{
    public const string Default = "dtpipe";

    /// <summary>The runtime setting the build writes: the folder of the tool package a host downloaded (<c>build/DtPipe.PipelineNode.Hosting.targets</c>).</summary>
    public const string ToolDirectorySetting = "DtPipe.ToolDirectory";

    private const string EntryPoint = "DtPipe.dll";

    /// <summary>The program to run: <paramref name="configured"/> when it names one, else the bundled entry point, else the default.</summary>
    public static string Resolve(string configured) =>
        Resolve(configured, AppContext.GetData(ToolDirectorySetting) as string, AppContext.BaseDirectory);

    internal static string Resolve(string configured, string? toolDirectory, string baseDirectory)
    {
        if (!string.Equals(configured, Default, StringComparison.Ordinal)) return configured;
        foreach (var directory in new[] { toolDirectory, Path.Combine(baseDirectory, "dtpipe") })
        {
            if (string.IsNullOrEmpty(directory)) continue;
            var entry = Path.Combine(directory, EntryPoint);
            if (File.Exists(entry)) return entry;
        }
        return configured;
    }
}
