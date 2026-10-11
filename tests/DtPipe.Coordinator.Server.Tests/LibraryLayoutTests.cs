using System.Diagnostics;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The library keeps a job and its plan, nothing a designer drew: a <c>layout.json</c> it still tracks
/// is dropped in one commit of its own when the library first opens, and only then.
/// </summary>
public sealed class LibraryLayoutTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "library-layout-" + Guid.NewGuid().ToString("N"));

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PipelineLibrary Open() => new(new CoordinatorServerOptions
    {
        StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
    }, _ => null);

    private string Git(params string[] args)
    {
        var psi = new ProcessStartInfo("git") { WorkingDirectory = Path.Combine(_dir, "library"), RedirectStandardOutput = true };
        foreach (var arg in new[] { "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false" }.Concat(args)) psi.ArgumentList.Add(arg);
        using var process = Process.Start(psi)!;
        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        Assert.Equal(0, process.ExitCode);
        return output;
    }

    [Fact]
    public async Task A_tracked_layout_is_dropped_in_one_commit_and_the_job_keeps_its_version()
    {
        var first = Open();
        await first.SaveAsync("orders", "a:\n  input: x\n", "create", Ct);
        await first.SaveAsync("other", "b:\n  input: y\n", "create", Ct);
        foreach (var id in new[] { "orders", "other" })
            File.WriteAllText(Path.Combine(_dir, "library", "pipelines", id, "layout.json"), "{}");
        Git("add", "-A");
        Git("commit", "-q", "-m", "A designer saved layouts");
        var version = (await first.GetAsync("orders", Ct))!.Commit;

        var reopened = Open();
        var doc = await reopened.GetAsync("orders", Ct);

        Assert.Empty(Git("ls-files", ":(glob)pipelines/*/layout.json").Split('\n', StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(1, Git("log", "--format=%s").Split('\n').Count(l => l == "Remove the designer's layouts"));
        Assert.Equal(version, doc!.Commit);
        Assert.False(File.Exists(Path.Combine(_dir, "library", "pipelines", "orders", "layout.json")));

        await Open().ListAsync(Ct);
        Assert.Equal(1, Git("log", "--format=%s").Split('\n').Count(l => l == "Remove the designer's layouts"));
    }

    [Fact]
    public async Task A_library_with_no_layout_commits_nothing_more()
    {
        var library = Open();
        await library.SaveAsync("orders", "a:\n  input: x\n", "create", Ct);
        var before = Git("rev-parse", "HEAD");

        await Open().ListAsync(Ct);

        Assert.Equal(before, Git("rev-parse", "HEAD"));
    }
}
