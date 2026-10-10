using DtPipe.PipelineNode.Hosting;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>Two replicas of one node, started from the same configuration on the same machine, never share a directory.</summary>
public sealed class ProcessDirectoryTests
{
    [Fact]
    public void Two_Processes_Of_One_Configuration_Have_Two_Directories_Under_The_Same_Parent()
    {
        var options = new DtPipeNodeOptions { Name = "node-4", StateDirectory = Path.Combine(Path.GetTempPath(), "dtpipe-parent") };

        var first = options.ProcessDirectory(Guid.NewGuid());
        var second = options.ProcessDirectory(Guid.NewGuid());

        Assert.NotEqual(first, second);
        Assert.Equal(Path.GetFullPath(options.StateDirectory), Path.GetDirectoryName(first));
        Assert.Equal(Path.GetFullPath(options.StateDirectory), Path.GetDirectoryName(second));
    }

    [Fact]
    public void The_Same_Process_Always_Gets_The_Same_Directory()
    {
        var options = new DtPipeNodeOptions { Name = "node-4", StateDirectory = Path.Combine(Path.GetTempPath(), "dtpipe-parent") };
        var instance = Guid.NewGuid();

        Assert.Equal(options.ProcessDirectory(instance), options.ProcessDirectory(instance));
    }

    [Fact]
    public void Without_A_Configured_Directory_The_Parent_Is_The_Nodes_Own_Under_The_Temp_Directory()
    {
        var options = new DtPipeNodeOptions { Name = "node-4" };

        var directory = options.ProcessDirectory(Guid.NewGuid());

        Assert.Equal(Path.Combine(Path.GetTempPath(), "dtpipe-node", "node-4"), Path.GetDirectoryName(directory));
    }
}
