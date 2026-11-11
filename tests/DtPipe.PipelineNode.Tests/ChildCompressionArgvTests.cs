using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The codec reaches the child on its command line, as <c>--bind-compression</c>, and never through
/// its environment. A recording script stands in for dtpipe.
/// </summary>
public class ChildCompressionArgvTests : IDisposable
{
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("dtpipe-argv-");

    public void Dispose() => _dir.Delete(recursive: true);

    private async Task<(List<string> Args, List<string> Env)> RecordAsync(string codec, int? level, params EdgeBinding[] edges)
    {
        var record = Path.Combine(_dir.FullName, "record.txt");
        var script = Path.Combine(_dir.FullName, "fake-dtpipe.sh");
        await File.WriteAllTextAsync(script,
            "#!/bin/sh\n"
            + $"for a in \"$@\"; do echo \"ARG:$a\" >> '{record}'; done\n"
            + $"env | grep '^DTPIPE_ARROW' | sed 's/^/ENV:/' >> '{record}'\n"
            + $"echo DONE >> '{record}'\n",
            TestContext.Current.CancellationToken);
        File.SetUnixFileMode(script, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

        var launch = new FragmentLaunch
        {
            DtPipeExecutable = script,
            FragmentJobPath = Path.Combine(_dir.FullName, "job.yaml"),
            Edges = edges,
            ArrowCompression = codec,
            ArrowCompressionLevel = level,
        };
        await using var run = new FragmentRun(launch, NullLogger.Instance);
        run.Launch();

        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && !(File.Exists(record) && File.ReadAllText(record).Contains("DONE")))
            await Task.Delay(50, TestContext.Current.CancellationToken);

        var lines = File.ReadAllLines(record);
        return (lines.Where(l => l.StartsWith("ARG:")).Select(l => l[4..]).ToList(),
                lines.Where(l => l.StartsWith("ENV:")).Select(l => l[4..]).ToList());
    }

    [Fact]
    public async Task TheCodecAndItsLevelRideTheChildsCommandLine_AndNoVariableIsSet()
    {
        if (OperatingSystem.IsWindows()) return;
        {
            var (args, env) = await RecordAsync("zstd", 7, new EdgeBinding("a", EdgeDirection.Outbound), new EdgeBinding("b", EdgeDirection.Outbound));

            Assert.Equal("zstd", args[args.IndexOf("--bind-compression") + 1]);
            Assert.Equal("7", args[args.IndexOf("--bind-compression-level") + 1]);
            var bound = args[args.IndexOf("--bind-output") + 1];
            Assert.StartsWith("b=pipe://", bound);
            Assert.Empty(env);
        }
    }

    [Fact]
    public async Task WithoutALevel_NoLevelFlagIsGiven()
    {
        if (OperatingSystem.IsWindows()) return;
        var (args, env) = await RecordAsync("lz4", null, new EdgeBinding("a", EdgeDirection.Outbound));

        Assert.Equal("lz4", args[args.IndexOf("--bind-compression") + 1]);
        Assert.DoesNotContain("--bind-compression-level", args);
        Assert.Empty(env);
    }
}
