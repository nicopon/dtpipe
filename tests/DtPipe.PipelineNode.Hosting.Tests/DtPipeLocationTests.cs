using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>Which dtpipe a host runs: the one it names, else the tool package beside it, else the PATH's.</summary>
public class DtPipeLocationTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "dtpipe-location-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }

    private string Tool(params string[] parts)
    {
        var directory = Path.Combine([_root, .. parts]);
        Directory.CreateDirectory(directory);
        File.WriteAllText(Path.Combine(directory, "DtPipe.dll"), "");
        return directory;
    }

    [Fact]
    public void A_host_that_names_a_dtpipe_gets_exactly_that_one()
    {
        var tool = Tool("tool");
        Assert.Equal("/opt/dtpipe/dtpipe", DtPipeLocation.Resolve("/opt/dtpipe/dtpipe", tool, _root));
    }

    [Fact]
    public void The_default_becomes_the_tool_package_the_build_placed()
    {
        var tool = Tool("cache", "tools", "net10.0", "any");
        Assert.Equal(Path.Combine(tool, "DtPipe.dll"), DtPipeLocation.Resolve("dtpipe", tool, Path.Combine(_root, "app")));
    }

    [Fact]
    public void The_default_becomes_the_dtpipe_folder_beside_the_host_when_the_build_path_is_gone()
    {
        var beside = Tool("app", "dtpipe");
        Assert.Equal(Path.Combine(beside, "DtPipe.dll"),
            DtPipeLocation.Resolve("dtpipe", Path.Combine(_root, "other-machine", "cache"), Path.Combine(_root, "app")));
    }

    [Fact]
    public void The_default_stays_the_path_lookup_when_there_is_no_tool_package()
    {
        Assert.Equal("dtpipe", DtPipeLocation.Resolve("dtpipe", null, Path.Combine(_root, "app")));
    }
}
