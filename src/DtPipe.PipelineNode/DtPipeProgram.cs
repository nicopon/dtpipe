using System.Diagnostics;
using System.Runtime.InteropServices;

namespace DtPipe.PipelineNode;

/// <summary>
/// How a node starts dtpipe. A path ending in <c>.dll</c> is the entry point of the <c>dtpipe</c> tool package, which has
/// no executable of its own: it runs through the dotnet host, with the dll as the first argument. Any other path is
/// an executable and starts as itself.
/// </summary>
public static class DtPipeProgram
{
    /// <summary>True when <paramref name="dtpipe"/> is a managed entry point rather than an executable.</summary>
    public static bool IsManagedEntryPoint(string dtpipe) => dtpipe.EndsWith(".dll", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Sets the program of <paramref name="start"/> and, for a managed entry point, its first argument. Call it before
    /// adding any other argument.
    /// </summary>
    public static void Apply(ProcessStartInfo start, string dtpipe)
    {
        if (!IsManagedEntryPoint(dtpipe))
        {
            start.FileName = dtpipe;
            return;
        }
        start.FileName = FindHost(RuntimeEnvironment.GetRuntimeDirectory(), Environment.GetEnvironmentVariable("DOTNET_ROOT"));
        start.ArgumentList.Add(dtpipe);
    }

    /// <summary>
    /// The dotnet host of the runtime this process runs on: the one at the root of its shared framework, since the
    /// process may be its own apphost and then names no host. Else <paramref name="dotnetRoot"/>, else <c>dotnet</c>
    /// on the PATH.
    /// </summary>
    internal static string FindHost(string runtimeDirectory, string? dotnetRoot)
    {
        var name = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        // <root>/shared/Microsoft.NETCore.App/<version>/
        var framework = new DirectoryInfo(runtimeDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        if (framework.Parent?.Parent?.Parent is { } root && File.Exists(Path.Combine(root.FullName, name)))
            return Path.Combine(root.FullName, name);
        if (!string.IsNullOrEmpty(dotnetRoot) && File.Exists(Path.Combine(dotnetRoot, name)))
            return Path.Combine(dotnetRoot, name);
        return name;
    }
}
