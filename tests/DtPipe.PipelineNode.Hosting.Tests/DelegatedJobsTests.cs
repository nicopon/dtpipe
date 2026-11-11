using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class DelegatedJobsTests
{
    // A dtpipe that cannot start: a request that reaches it fails differently from a refusal.
    private static readonly DtPipeCommand Unreachable = new("/nonexistent/dtpipe", Path.GetTempPath(), new Dictionary<string, string>());

    private const string Job = "src:\n  input: arrow:-\ndst:\n  from: src\n  output: arrow:-\n  contract-save: dst.contract.json\n";

    private static JobCheck Check(string sampleAlias = "src", string sinkAlias = "dst", string job = Job) =>
        new(job, new Dictionary<string, byte[]> { [sampleAlias] = [1, 2, 3] }, [sinkAlias]);

    [Theory]
    [InlineData("src:\n  input: arrow:-\nleak:\n  input: csv:/etc/hosts\ndst:\n  from: src\n  output: arrow:-\n  contract-save: dst.contract.json\n", "'leak' reads 'csv:/etc/hosts'")]
    [InlineData("src:\n  input: arrow:-\ndst:\n  from: src\n  output: arrow:-\n  contract-save: /tmp/stolen.json\n", "'dst' saves its contract elsewhere")]
    public async Task A_runner_refuses_a_dry_run_whose_job_reaches_past_its_samples_and_sinks_before_dtpipe_starts(string job, string expected)
    {
        var answer = await new DelegatedJobs("runner-1", enabled: true, Unreachable).ValidateAsync(Check(job: job));

        Assert.False(answer.Ok);
        Assert.StartsWith(expected, answer.Report);
    }

    [Fact]
    public async Task A_host_that_is_no_runner_checks_nothing_for_the_coordinator()
    {
        var answer = await new DelegatedJobs("node-1", enabled: false, Unreachable).ValidateAsync(Check());

        Assert.False(answer.Ok);
        Assert.Contains("only a runner does", answer.Report);
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    [InlineData("1st")]
    [InlineData("a b")]
    public async Task An_alias_that_is_no_plain_word_never_names_a_file(string alias)
    {
        var asSample = await new DelegatedJobs("runner-1", enabled: true, Unreachable).ValidateAsync(Check(sampleAlias: alias));
        var asSink = await new DelegatedJobs("runner-1", enabled: true, Unreachable).ValidateAsync(Check(sinkAlias: alias));

        Assert.False(asSample.Ok);
        Assert.Contains("is not an alias", asSample.Report);
        Assert.False(asSink.Ok);
        Assert.Contains("is not an alias", asSink.Report);
    }

    [Fact]
    public async Task A_runner_whose_dtpipe_cannot_start_says_so_and_leaves_nothing_behind()
    {
        var root = Path.Combine(Path.GetTempPath(), "delegated-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var command = new DtPipeCommand("/nonexistent/dtpipe", root, new Dictionary<string, string>());
            var answer = await new DelegatedJobs("runner-1", enabled: true, command).ValidateAsync(Check());

            Assert.False(answer.Ok);
            Assert.NotEmpty(answer.Report);
            Assert.Empty(Directory.GetDirectories(Path.Combine(root, "validate")));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
