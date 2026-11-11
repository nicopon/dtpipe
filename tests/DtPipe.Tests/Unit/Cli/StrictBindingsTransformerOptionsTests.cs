using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A job file's transformer <c>options:</c> block binds like a <c>provider-options</c> block: an
/// unknown key warns, and fails the run under <c>--strict-bindings</c>. The engine builds a YAML
/// job's steps itself, so the flag has to reach that build: these run the real command line in
/// process, where a unit test of the builder alone passed while the run ignored the flag.
/// </summary>
public class StrictBindingsTransformerOptionsTests : IDisposable
{
    private readonly string _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_strict_{Guid.NewGuid():N}");

    public StrictBindingsTransformerOptionsTests() => Directory.CreateDirectory(_tmp);

    public void Dispose()
    {
        try { Directory.Delete(_tmp, recursive: true); } catch { /* best effort */ }
    }

    private string JobWithUnknownFakeOption()
    {
        var input = Path.Combine(_tmp, "in.csv");
        File.WriteAllText(input, "a,b\n1,2\n");
        var job = Path.Combine(_tmp, "job.yaml");
        File.WriteAllText(job, $"""
            main:
              input: '{input}'
              output: '{Path.Combine(_tmp, "out.csv")}'
              transformers:
              - type: fake
                mappings:
                  a: name.fullName
                options:
                  bogus-key: 1
            """);
        return job;
    }

    [Fact]
    public async Task An_Unknown_Transformer_Option_Warns_And_The_Run_Succeeds()
        => (await Program.Main(["--job", JobWithUnknownFakeOption(), "--no-stats"])).Should().Be(0);

    [Fact]
    public async Task An_Unknown_Transformer_Option_Fails_The_Run_Under_Strict_Bindings()
        => (await Program.Main(["--job", JobWithUnknownFakeOption(), "--strict-bindings", "--no-stats"]))
            .Should().Be(1);
}
