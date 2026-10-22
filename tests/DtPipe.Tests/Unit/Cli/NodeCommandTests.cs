using System.Reflection;
using DtPipe.Cli;
using DtPipe.Cli.Node;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>TransportR belongs to the node assembly alone, and that assembly loads when a node starts: no other command touches it.</summary>
[Collection("console-serial")]
public sealed class NodeCommandTests
{
    [Fact]
    public async Task The_Ordinary_Commands_Load_Neither_The_Node_Assembly_Nor_TransportR()
    {
        var present = Directory.GetFiles(AppContext.BaseDirectory, "TransportR*.dll").Length > 0
            && File.Exists(Path.Combine(AppContext.BaseDirectory, "DtPipe.PipelineNode.Launcher.dll"));
        Assert.True(present, "the node assembly and TransportR ship beside dtpipe, so their absence below means they were not loaded");

        var stdout = Console.Out;
        Console.SetOut(TextWriter.Null);
        try
        {
            Assert.Equal(0, await Program.Main(["--version"]));
            Assert.Equal(0, await Program.Main(["--help"]));
            var services = new ServiceCollection();
            Program.ConfigureServices(services);
            var jobService = services.BuildServiceProvider().GetRequiredService<JobService>();
            Assert.Contains("node", jobService.Subcommands.Select(c => c.Name));
            Assert.Equal(0, await jobService.BuildSubcommands().Parse(["node", "--help"]).InvokeAsync());
        }
        finally
        {
            Console.SetOut(stdout);
        }

        var loaded = AppDomain.CurrentDomain.GetAssemblies().Select(a => a.GetName().Name!).ToList();
        Assert.DoesNotContain(loaded, n => n.StartsWith("TransportR", StringComparison.Ordinal));
        Assert.DoesNotContain("DtPipe.PipelineNode.Launcher", loaded);
    }

    [Fact]
    public void A_Source_And_A_Sink_Are_Bricks_Named_By_Their_Alias()
    {
        var (bricks, refusal) = NodeBricks.Read("numbers:\n  input: generate:5\ntotals:\n  output: csv:/state/totals.csv\n");

        Assert.Null(refusal);
        Assert.Equal([("numbers", true), ("totals", false)], bricks.Select(b => (b.Id, b.IsSource)));
    }

    [Theory]
    [InlineData("a:\n  input: generate:5\n  output: csv:/x.csv\n", "reads and writes")]
    [InlineData("a:\n  from: b\n  output: csv:/x.csv\n", "reads another branch")]
    [InlineData("a:\n  input: \"pg:Host=h;Database=d;Password=hunter2\"\n", "holds a credential")]
    [InlineData("a:\n  input: \"duck::memory:\"\n  provider-options:\n    duck-reader:\n      query: SELECT * FROM t WHERE password = 'x'\n", "holds a credential")]
    public void A_Branch_That_Is_No_Brick_Or_Holds_A_Credential_Is_Refused_By_Name(string job, string reason)
    {
        var (bricks, refusal) = NodeBricks.Read(job);

        Assert.Empty(bricks);
        Assert.Contains("'a'", refusal);
        Assert.Contains(reason, refusal);
    }

    [Fact]
    public void A_Credential_Written_As_A_Reference_Passes()
    {
        var (bricks, refusal) = NodeBricks.Read("a:\n  input: \"pg:Host=h;Database=d;Password=${{PW}}\"\nb:\n  output: \"pg:Host=h;Password=keyring://pw\"\n");

        Assert.Null(refusal);
        Assert.Equal(2, bricks.Count);
    }
}
