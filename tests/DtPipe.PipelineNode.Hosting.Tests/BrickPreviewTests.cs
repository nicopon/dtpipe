using System.Runtime.Versioning;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// A source brick's preview is its declared branch's sample: the options of its reader reach the read, and
/// the types and the values both come from the bytes that read produced.
/// </summary>
public sealed class BrickPreviewTests : IDisposable
{
    private const string Orders = """{"input":"csv:orders.csv","provider-options":{"csv-reader":{"delimiter":";"}}}""";
    private readonly string _dir = Directory.CreateTempSubdirectory("brick-preview-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    // A dtpipe that answers by its arguments: the branch run to Arrow writes fixed bytes, and an Arrow
    // file it is later asked to read must be those very bytes.
    [UnsupportedOSPlatform("windows")]
    private DtPipeCommand Binary()
    {
        File.WriteAllText(Path.Combine(_dir, "fixture.arrows"), "ARROW-BYTES");
        var path = Path.Combine(_dir, "dtpipe");
        File.WriteAllText(path, """
            #!/bin/sh
            here="$(dirname "$0")"
            echo "$@" >> "$here/calls.log"
            same() { case "$1" in arrow:*) cmp -s "${1#arrow:}" "$here/fixture.arrows" || { echo "not the sample's bytes" >&2; exit 7; } ;; esac; }
            case "$1" in
              --job) cp "$2" "$here/job.yaml"; cat "$here/fixture.arrows" ;;
              inspect) same "$3"; echo '[{"name":"id","type":"BIGINT","isNullable":false}]' ;;
              --input) same "$2"; printf 'id\n1\n2\n' ;;
            esac
            """);
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        return new DtPipeCommand(path, _dir, new Dictionary<string, string>());
    }

    [UnsupportedOSPlatform("windows")]
    private async Task<BrickHost> HostAsync(params BrickDeclaration[] bricks)
    {
        var host = new BrickHost("n1", bricks, Binary(), NullLogger.Instance);
        await host.InspectAsync(TestContext.Current.CancellationToken);
        return host;
    }

    [Fact]
    public async Task A_source_preview_runs_the_declared_branch_and_reads_its_types_and_values_from_that_sample()
    {
        if (OperatingSystem.IsWindows()) return;
        var host = await HostAsync(new BrickDeclaration("orders", BrickKind.Source, "Orders", null, Orders));

        var preview = await host.PreviewAsync("orders", 5);

        Assert.Null(preview.Error);
        Assert.Equal("id\n1\n2\n", preview.Csv);
        var column = Assert.Single(preview.Columns!);
        Assert.Equal(("id", "BIGINT"), (column.Name, column.Type));
        var job = File.ReadAllText(Path.Combine(_dir, "job.yaml"));
        Assert.Contains("delimiter", job);
        Assert.Contains("arrow:-", job);
        var calls = File.ReadAllLines(Path.Combine(_dir, "calls.log"));
        Assert.Contains(calls, c => c.StartsWith("--job ", StringComparison.Ordinal) && c.Contains("--limit 5"));
        // The old preview read only the input and the query: no read of the source goes to CSV directly.
        Assert.DoesNotContain(calls, c => c.StartsWith("--input csv:orders.csv", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_source_preview_over_the_bytes_a_sample_may_carry_is_refused_by_name()
    {
        if (OperatingSystem.IsWindows()) return;
        File.WriteAllBytes(Path.Combine(_dir, "fixture.arrows"), new byte[300 * 1024]);
        var path = Path.Combine(_dir, "dtpipe");
        File.WriteAllText(path, "#!/bin/sh\ncase \"$1\" in --job) cat \"$(dirname \"$0\")/fixture.arrows\" ;; inspect) echo '[]' ;; esac\n");
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        var host = new BrickHost("n1", [new BrickDeclaration("orders", BrickKind.Source, "Orders", null, Orders)],
            new DtPipeCommand(path, _dir, new Dictionary<string, string>()), NullLogger.Instance);
        await host.InspectAsync(TestContext.Current.CancellationToken);

        var preview = await host.PreviewAsync("orders", 5);

        Assert.Null(preview.Csv);
        Assert.Contains("over the", preview.Error);
    }
}
