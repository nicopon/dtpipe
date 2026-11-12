using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// Each child works in a run directory of its own, its temp directory, removed once it has ended; a runner's child also
/// works in it and runs under <c>--edges-only</c>, with none of the host's variables but those it keeps. An excess edge's pipe reaches the child by its whole path, since the
/// child's temp directory is not the node's. A recording script stands in for dtpipe.
/// </summary>
public class FragmentIsolationTests : IDisposable
{
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("dtpipe-isolation-");

    public void Dispose() => _dir.Delete(recursive: true);

    private async Task<Dictionary<string, List<string>>> RecordAsync(string runDirectory, bool edgesOnly)
    {
        var record = Path.Combine(_dir.FullName, "record.txt");
        var script = Path.Combine(_dir.FullName, "fake-dtpipe.sh");
        await File.WriteAllTextAsync(script,
            "#!/bin/sh\n"
            + $"for a in \"$@\"; do echo \"ARG=$a\" >> '{record}'; done\n"
            + $"echo \"PWD=$(pwd -P)\" >> '{record}'\n"
            + $"echo \"TMPDIR=$TMPDIR\" >> '{record}'; echo \"TEMP=$TEMP\" >> '{record}'; echo \"TMP=$TMP\" >> '{record}'\n"
            + $"echo \"HOST=$DTPIPE_ISOLATION_HOST\" >> '{record}'; echo \"GIVEN=$DTPIPE_ISOLATION_GIVEN\" >> '{record}'\n"
            + $"echo DONE= >> '{record}'\n",
            TestContext.Current.CancellationToken);
        File.SetUnixFileMode(script, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        Environment.SetEnvironmentVariable("DTPIPE_ISOLATION_HOST", "host");

        var launch = new FragmentLaunch
        {
            DtPipeExecutable = script,
            FragmentJobPath = Path.Combine(_dir.FullName, "job.yaml"),
            Edges = [new EdgeBinding("a", EdgeDirection.Inbound), new EdgeBinding("b", EdgeDirection.Inbound)],
            RunDirectory = runDirectory,
            EdgesOnly = edgesOnly,
            ChildEnvironment = new Dictionary<string, string> { ["DTPIPE_ISOLATION_GIVEN"] = "given" },
        };
        await using (var run = new FragmentRun(launch, NullLogger.Instance))
        {
            run.Launch();
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (DateTime.UtcNow < deadline && !(File.Exists(record) && File.ReadAllText(record).Contains("DONE=")))
                await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.True(Directory.Exists(runDirectory));
        }

        return File.ReadAllLines(record).Select(l => l.Split('=', 2)).GroupBy(p => p[0], p => p[1])
            .ToDictionary(g => g.Key, g => g.ToList());
    }

    [Fact]
    public async Task A_Runners_Child_Works_And_Keeps_Its_Temp_In_Its_Run_Directory_Under_Edges_Only_And_The_Directory_Goes_With_It()
    {
        if (OperatingSystem.IsWindows()) return;
        var runDirectory = Path.Combine(_dir.FullName, "run-1");

        var seen = await RecordAsync(runDirectory, edgesOnly: true);

        var real = new DirectoryInfo(runDirectory).FullName;
        Assert.Contains("--edges-only", seen["ARG"]);
        Assert.EndsWith(Path.GetFileName(real), Assert.Single(seen["PWD"]));
        Assert.All(new[] { "TMPDIR", "TEMP", "TMP" }, name => Assert.Equal(real, Assert.Single(seen[name])));
        Assert.Equal("", Assert.Single(seen["HOST"]));
        Assert.Equal("given", Assert.Single(seen["GIVEN"]));
        Assert.False(Directory.Exists(runDirectory));
    }

    [Fact]
    public async Task A_Data_Nodes_Child_Keeps_The_Hosts_Working_Directory_And_Gets_A_Temp_Of_Its_Own()
    {
        if (OperatingSystem.IsWindows()) return;
        var runDirectory = Path.Combine(_dir.FullName, "run-2");

        var seen = await RecordAsync(runDirectory, edgesOnly: false);

        Assert.DoesNotContain("--edges-only", seen["ARG"]);
        Assert.NotEqual(new DirectoryInfo(runDirectory).FullName, Assert.Single(seen["PWD"]));
        Assert.Equal(runDirectory, Assert.Single(seen["TMPDIR"]));
        Assert.Equal("host", Assert.Single(seen["HOST"]));
        Assert.False(Directory.Exists(runDirectory));
    }

    [Fact]
    public async Task An_Excess_Edge_Reaches_The_Child_By_The_Pipes_Whole_Path_Under_The_Nodes_Temp_Directory()
    {
        if (OperatingSystem.IsWindows()) return;

        var seen = await RecordAsync(Path.Combine(_dir.FullName, "run-3"), edgesOnly: true);

        var bound = seen["ARG"][seen["ARG"].IndexOf("--bind-input") + 1];
        Assert.StartsWith("b=pipe://" + Path.GetTempPath(), bound);
    }
}
