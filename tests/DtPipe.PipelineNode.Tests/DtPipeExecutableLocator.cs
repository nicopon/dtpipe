namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// Every guard here spawns the real, self-contained <c>dtpipe</c> binary — nothing in this suite
/// runs the pipeline in-process — so it locates <c>dist/release/dtpipe</c> by walking up from the
/// test assembly rather than assuming a working directory.
/// </summary>
internal static class DtPipeExecutableLocator
{
    public static string Path => _path.Value;

    private static readonly Lazy<string> _path = new(Locate);

    private static string Locate()
    {
        var extension = OperatingSystem.IsWindows() ? ".exe" : "";
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, "dist", "release", $"dtpipe{extension}");
            if (File.Exists(candidate))
                return candidate;
        }
        throw new FileNotFoundException(
            "dist/release/dtpipe not found above the test assembly's own directory; run ./build.sh first.");
    }
}
