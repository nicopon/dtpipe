using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

public class DelegatedJobsTests
{
    // A dtpipe that cannot start: a request that reaches it fails differently from a refusal.
    private static readonly DtPipeCommand Unreachable = new("/nonexistent/dtpipe", Path.GetTempPath(), new Dictionary<string, string>());

    private static JobCheck Check(string sampleAlias = "src", string sinkAlias = "dst") =>
        new("job", new Dictionary<string, byte[]> { [sampleAlias] = [1, 2, 3] }, [sinkAlias]);

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
