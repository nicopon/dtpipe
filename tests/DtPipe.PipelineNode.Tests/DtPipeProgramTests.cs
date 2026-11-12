using System.Diagnostics;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>An executable starts as itself; the tool package's dll runs through the dotnet host, as its first argument.</summary>
public class DtPipeProgramTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dtpipe-program-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public void An_executable_starts_as_itself()
    {
        var start = new ProcessStartInfo();
        DtPipeProgram.Apply(start, "/opt/dtpipe/dtpipe");
        Assert.Equal("/opt/dtpipe/dtpipe", start.FileName);
        Assert.Empty(start.ArgumentList);
    }

    [Fact]
    public void A_dll_runs_through_the_dotnet_host_with_the_dll_first()
    {
        var start = new ProcessStartInfo();
        DtPipeProgram.Apply(start, "/cache/dtpipe/tools/net10.0/any/DtPipe.dll");
        start.ArgumentList.Add("--job");
        Assert.Equal(["/cache/dtpipe/tools/net10.0/any/DtPipe.dll", "--job"], start.ArgumentList);
        Assert.StartsWith("dotnet", Path.GetFileName(start.FileName));
    }

    [Fact]
    public void The_host_is_the_one_at_the_root_of_the_running_framework()
    {
        var name = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        var framework = Path.Combine(_root, "shared", "Microsoft.NETCore.App", "10.0.0") + Path.DirectorySeparatorChar;
        Directory.CreateDirectory(framework);
        File.WriteAllText(Path.Combine(_root, name), "");
        Assert.Equal(Path.Combine(_root, name), DtPipeProgram.FindHost(framework, dotnetRoot: null));
    }

    [Fact]
    public void Without_one_at_the_framework_root_it_falls_back_to_DOTNET_ROOT_then_to_the_path()
    {
        var name = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        var framework = Path.Combine(_root, "none", "shared", "Microsoft.NETCore.App", "10.0.0");
        Directory.CreateDirectory(framework);
        var elsewhere = Path.Combine(_root, "elsewhere");
        Directory.CreateDirectory(elsewhere);
        File.WriteAllText(Path.Combine(elsewhere, name), "");
        Assert.Equal(Path.Combine(elsewhere, name), DtPipeProgram.FindHost(framework, elsewhere));
        Assert.Equal(name, DtPipeProgram.FindHost(framework, dotnetRoot: null));
    }
}
