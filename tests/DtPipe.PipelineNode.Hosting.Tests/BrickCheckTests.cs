using System.Runtime.Versioning;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// A source brick is checked against the contract a binding pinned: the node reads its sample as a sample is read, keeps the contract
/// it produces, and judges it by hash (<c>contract</c>) or by <c>dtpipe contract diff</c> (<c>compatible</c>). The binary here answers
/// by its arguments; its diff stands for dtpipe's own rule, which the engine's tests hold.
/// </summary>
public sealed class BrickCheckTests : IDisposable
{
    private const string Orders = """{"input":"csv:orders.csv"}""";
    private readonly string _dir = Directory.CreateTempSubdirectory("brick-check-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static string Contract(string hash, string type) =>
        $$$"""{"version":1,"hash":"{{{hash}}}","schema":{"fields":[{"name":"n","nullable":false,"type":"{{{type}}}"}]}}""";

    private static readonly string Pinned = Contract("aaaaaaaaaaaaaaaaaaaaaaaa", "int32");
    private static readonly string Widened = Contract("bbbbbbbbbbbbbbbbbbbbbbbb", "int64");
    private static readonly string Narrowed = Contract("cccccccccccccccccccccccc", "string");

    // The contract the brick produces now is what a test puts in now.contract.json; the sample run saves it where its job says.
    [UnsupportedOSPlatform("windows")]
    private DtPipeCommand Binary(string produced)
    {
        File.WriteAllText(Path.Combine(_dir, "now.contract.json"), produced);
        File.WriteAllText(Path.Combine(_dir, "fixture.arrows"), "ARROW-BYTES");
        var path = Path.Combine(_dir, "dtpipe");
        File.WriteAllText(path, """
            #!/bin/sh
            here="$(dirname "$0")"
            echo "$@" >> "$here/calls.log"
            case "$1" in
              --job)
                cp "$2" "$here/job.yaml"
                target="$(sed -n "s/^ *contract-save: *['\"]\{0,1\}\([^'\"]*\).*/\1/p" "$2")"
                [ -n "$target" ] && cp "$here/now.contract.json" "$target"
                cat "$here/fixture.arrows" ;;
              contract)
                if grep -q '"string"' "$5"; then
                  echo '{"oldHash":"a","newHash":"c","compatible":false,"changes":[{"column":"n","kind":"TypeChanged","breaking":true,"detail":"int32 → string"}]}'; exit 1
                elif [ -f "$here/diff.broken" ]; then
                  echo "Unhandled exception: boom" >&2; exit 1
                else
                  echo '{"oldHash":"a","newHash":"b","compatible":true,"changes":[{"column":"n","kind":"TypeWidened","breaking":false,"detail":"int32 → int64, wider"}]}'
                fi ;;
            esac
            """);
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        return new DtPipeCommand(path, _dir, new Dictionary<string, string>());
    }

    [UnsupportedOSPlatform("windows")]
    private async Task<BrickHost> HostAsync(string produced)
    {
        var host = new BrickHost("n1", [new BrickDeclaration("orders", BrickKind.Source, "Orders", null, Orders)], Binary(produced), NullLogger.Instance);
        await host.InspectAsync(TestContext.Current.CancellationToken);
        return host;
    }

    [Fact]
    public async Task A_Sample_Carries_The_Contract_It_Produced_Saved_By_An_Absolute_Path_In_Its_Own_Directory()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Pinned);

        var sample = await host.SampleAsync("orders", 1);

        Assert.Null(sample.Error);
        Assert.Equal(Pinned, sample.Contract);
        var saved = File.ReadAllLines(Path.Combine(_dir, "job.yaml")).Single(l => l.Contains("contract-save:")).Split(':', 2)[1].Trim().Trim('\'', '"');
        Assert.True(Path.IsPathRooted(saved));
        Assert.StartsWith(Path.Combine(_dir, "bricks") + Path.DirectorySeparatorChar, saved);
        // Nothing of the run is left behind.
        Assert.Empty(Directory.GetDirectories(Path.Combine(_dir, "bricks")));
    }

    [Fact]
    public async Task A_Source_Producing_The_Pinned_Schema_Is_The_Same_In_Either_Mode()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Pinned);

        Assert.Equal(SourceVerdicts.Same, (await host.CheckSourceAsync("orders", Pinned, "contract")).Verdict);
        Assert.Equal(SourceVerdicts.Same, (await host.CheckSourceAsync("orders", Pinned, "compatible")).Verdict);
    }

    [Fact]
    public async Task A_Column_That_Widens_Breaks_Contract_And_Passes_Compatible()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Widened);

        var strict = await host.CheckSourceAsync("orders", Pinned, "contract");
        var lenient = await host.CheckSourceAsync("orders", Pinned, "compatible");

        Assert.Equal(SourceVerdicts.Breaking, strict.Verdict);
        Assert.Contains("no longer the pinned one", strict.Report);
        Assert.Equal(SourceVerdicts.Compatible, lenient.Verdict);
        Assert.Contains("int32 → int64", lenient.Report);
    }

    [Fact]
    public async Task A_Column_That_Changes_Type_Breaks_Compatible_And_Names_The_Change()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Narrowed);

        var verdict = await host.CheckSourceAsync("orders", Pinned, "compatible");

        Assert.Equal(SourceVerdicts.Breaking, verdict.Verdict);
        Assert.Equal("n: int32 → string", verdict.Report);
    }

    [Fact]
    public async Task A_Comparison_That_Answers_Nothing_Readable_Is_Unread_Never_Breaking()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Widened);
        File.WriteAllText(Path.Combine(_dir, "diff.broken"), "");

        var verdict = await host.CheckSourceAsync("orders", Pinned, "compatible");

        Assert.Equal(SourceVerdicts.Unread, verdict.Verdict);
    }

    [Fact]
    public async Task A_Brick_That_Is_Unknown_Or_A_Mode_That_Is_Not_Checked_Is_Unread()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(Pinned);

        Assert.Equal(SourceVerdicts.Unread, (await host.CheckSourceAsync("nothing", Pinned, "contract")).Verdict);
        Assert.Equal(SourceVerdicts.Unread, (await host.CheckSourceAsync("orders", Pinned, "tolerant")).Verdict);
        Assert.Equal(SourceVerdicts.Unread, (await host.CheckSourceAsync("orders", "not json", "contract")).Verdict);
    }
}
