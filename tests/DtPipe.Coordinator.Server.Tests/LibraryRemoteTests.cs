using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The library pushed to a git remote, against a real bare repository (needs only <c>git</c>): a save arrives there, a push
/// the remote refuses undoes the save whole, a lost state directory is read back, and the token reaches git by its environment only.
/// A remote's hooks run once the library is open: a library that opens pushes its first commit.
/// </summary>
public sealed class LibraryRemoteTests : IDisposable
{
    private const string Job = "a:\n  input: x\n";
    private const string Edited = "a:\n  input: y\n";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "library-remote-" + Guid.NewGuid().ToString("N"));
    private readonly string _bare;

    public LibraryRemoteTests()
    {
        _bare = Path.Combine(_dir, "remote.git");
        Directory.CreateDirectory(_bare);
        Git(_bare, "init", "-q", "--bare", "-b", "main");
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string Git(string dir, params string[] args)
    {
        var psi = new ProcessStartInfo("git") { WorkingDirectory = dir, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false" }.Concat(args)) psi.ArgumentList.Add(arg);
        using var process = Process.Start(psi)!;
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0, $"git {string.Join(' ', args)}: {error}");
        return output.Trim();
    }

    private string State(string name) => Path.Combine(_dir, name);

    private string Local(string state) => Path.Combine(state, "library");

    private string RemoteHead => Git(_bare, "rev-parse", "main");

    private PipelineLibrary Open(string state, LibraryRemoteSettings? remote = null, bool begin = true, ILogger? log = null) => new(
        new CoordinatorServerOptions
        {
            StateDir = state, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret",
            LibraryRemote = remote ?? new LibraryRemoteSettings(_bare), Initialize = begin,
        }, logger: log);

    private void Hook(string name, string script)
    {
        var path = Path.Combine(_bare, "hooks", name);
        File.WriteAllText(path, "#!/bin/sh\n" + script + "\n");
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }

    private void NoHook(string name) => File.Delete(Path.Combine(_bare, "hooks", name));

    private sealed class Lines : ILogger
    {
        public List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception) + exception?.Message);
    }

    [Fact]
    public async Task A_save_reaches_the_remote_and_opening_pushes_the_first_commit()
    {
        var library = Open(State("a"));
        await library.OpenAsync(Ct);
        Assert.Equal(Git(Local(State("a")), "rev-parse", "HEAD"), RemoteHead);

        var saved = await library.SaveAsync("orders", Job, "create", Ct);

        Assert.Equal(saved.Commit, Git(_bare, "log", "-1", "--format=%H", "--", "pipelines/orders/pipeline.yaml"));
        Assert.Equal(RemoteHead, Git(Local(State("a")), "rev-parse", "HEAD"));
        Assert.Contains("create", Git(_bare, "log", "-1", "--format=%s"));
    }

    [Fact]
    public async Task A_push_the_remote_refuses_leaves_the_library_exactly_as_it_was()
    {
        var library = Open(State("a"));
        await library.SaveAsync("orders", Job, "create", Ct);
        var local = Local(State("a"));
        var head = Git(local, "rev-parse", "HEAD");
        Hook("pre-receive", "echo 'no thanks' >&2; exit 1");

        // A pipeline that is new, a pipeline that is edited, one that is bound and one that is deleted: each leaves nothing behind.
        var created = await Assert.ThrowsAsync<StoreUnavailableException>(() => library.SaveAsync("other", Job, "create", Ct));
        await Assert.ThrowsAsync<StoreUnavailableException>(() => library.SaveAsync("orders", Edited, "edit", Ct));
        await Assert.ThrowsAsync<StoreUnavailableException>(() =>
            library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = new Binding("n1", "b") }, "bind", Ct));
        await Assert.ThrowsAsync<StoreUnavailableException>(() => library.DeleteAsync("orders", Ct));

        Assert.Contains("refused the save, so nothing was saved", created.Message);
        Assert.DoesNotContain("no thanks", created.Message);
        Assert.Equal(head, Git(local, "rev-parse", "HEAD"));
        Assert.Equal(head, RemoteHead);
        Assert.Empty(Git(local, "status", "--porcelain", "--untracked-files=all"));
        Assert.False(Directory.Exists(Path.Combine(local, "pipelines", "other")));
        Assert.Equal(Job, File.ReadAllText(Path.Combine(local, "pipelines", "orders", "pipeline.yaml")));
        Assert.Null(await library.GetAsync("other", Ct));
        await Assert.ThrowsAsync<ConflictException>(() => library.DeleteAsync("other", Ct));

        NoHook("pre-receive");
        var again = await library.SaveAsync("other", Job, "create", Ct);
        Assert.Equal(again.Commit, (await library.GetAsync("other", Ct))!.Commit);
        Assert.Equal(RemoteHead, Git(local, "rev-parse", "HEAD"));
    }

    [Fact]
    public async Task A_state_directory_that_is_lost_is_read_back_from_the_remote()
    {
        var first = Open(State("a"));
        var saved = await first.SaveAsync("orders", Job, "create", Ct);

        var second = Open(State("b"));
        var doc = await second.GetAsync("orders", Ct);

        Assert.Equal((Job, saved.Commit), (doc!.Yaml, doc.Commit));
        // Nothing about the remote is written to the repository: its address is an argument, never its configuration.
        Assert.DoesNotContain(_bare, File.ReadAllText(Path.Combine(Local(State("b")), ".git", "config")));
    }

    [Fact]
    public async Task A_write_follows_the_remote_before_it_writes()
    {
        var first = Open(State("a"));
        var second = Open(State("b"));
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        // The remote moves on, as it does when a push that timed out had been applied.
        await first.SaveAsync("one", Job, "first", Ct);
        await second.SaveAsync("two", Job, "second", Ct);

        Assert.NotNull(await second.GetAsync("one", Ct));
        Assert.Equal(RemoteHead, Git(Local(State("b")), "rev-parse", "HEAD"));
        Assert.Equal(2, Git(_bare, "log", "--format=%s", "--", "pipelines").Split('\n').Length);
    }

    [Fact]
    public async Task Histories_that_have_parted_stop_the_start()
    {
        var first = Open(State("a"));
        await first.SaveAsync("one", Job, "first", Ct);
        Git(Local(State("a")), "commit", "-q", "--allow-empty", "-m", "never pushed");
        await Open(State("b")).SaveAsync("two", Job, "second", Ct);

        var ex = await Assert.ThrowsAsync<StoreUnavailableException>(() => Open(State("a")).OpenAsync(Ct));

        Assert.Contains("each commits the other lacks", ex.Message);
        Assert.Equal("never pushed", Git(Local(State("a")), "log", "-1", "--format=%s"));
    }

    [Fact]
    public async Task A_commit_the_remote_does_not_have_is_dropped_by_the_next_start()
    {
        var library = Open(State("a"));
        await library.SaveAsync("one", Job, "first", Ct);
        var head = RemoteHead;
        Git(Local(State("a")), "commit", "-q", "--allow-empty", "-m", "never pushed");
        var log = new Lines();

        await Open(State("a"), log: log).OpenAsync(Ct);

        Assert.Equal(head, Git(Local(State("a")), "rev-parse", "HEAD"));
        Assert.Contains(log.Messages, m => m.Contains("never pushed"));
    }

    [Fact]
    public async Task A_remote_with_nothing_in_it_is_begun_only_when_the_start_may_begin()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => Open(State("a"), begin: false).OpenAsync(Ct));

        await Open(State("b"), begin: true).OpenAsync(Ct);

        Assert.Equal("Start the library", Git(_bare, "log", "-1", "--format=%s", "main"));
    }

    [Fact]
    public async Task The_token_is_in_no_argument_no_message_no_log_and_no_configuration()
    {
        const string Token = "s3cr3t-t0ken";
        var remote = new LibraryRemoteSettings(_bare, Token, "ann");
        var log = new Lines();
        var library = Open(State("a"), remote, log: log);
        await library.SaveAsync("orders", Job, "create", Ct);

        var psi = library.Prepare(["push", "--porcelain", _bare, "HEAD:refs/heads/main"], remote: true);
        Assert.DoesNotContain(psi.ArgumentList, a => a.Contains(Token) || a.Contains("Authorization"));
        Assert.Equal($"http.{_bare}.extraHeader", psi.Environment["GIT_CONFIG_KEY_0"]);
        Assert.Equal("Authorization: Basic " + Convert.ToBase64String("ann:s3cr3t-t0ken"u8.ToArray()), psi.Environment["GIT_CONFIG_VALUE_0"]);
        Assert.Equal("0", psi.Environment["GIT_TERMINAL_PROMPT"]);
        Assert.False(library.Prepare(["status"], remote: false).Environment.ContainsKey("GIT_CONFIG_VALUE_0"));

        // The coordinator's own environment holds its settings, the token's among them: no git process inherits it.
        Environment.SetEnvironmentVariable("Coordinator__LibraryRemoteTests__Probe", "inherited");
        try
        {
            Assert.DoesNotContain(library.Prepare(["status"], remote: false).Environment.Keys, k => k.StartsWith("Coordinator__"));
            Assert.DoesNotContain(psi.Environment.Keys, k => k.StartsWith("Coordinator__"));
            Assert.DoesNotContain(library.Prepare(["push"], remote: true).Environment.Keys, k => k.StartsWith("Coordinator__"));
        }
        finally { Environment.SetEnvironmentVariable("Coordinator__LibraryRemoteTests__Probe", null); }

        // The remote says the token back in its refusal; neither the error nor the log repeats it.
        Hook("pre-receive", $"echo 'denied {Token}' >&2; exit 1");
        var ex = await Assert.ThrowsAsync<StoreUnavailableException>(() => library.SaveAsync("other", Job, "create", Ct));

        Assert.DoesNotContain(Token, ex.Message);
        Assert.DoesNotContain(log.Messages, m => m.Contains(Token));
        Assert.Contains(log.Messages, m => m.Contains("denied ***"));
        Assert.DoesNotContain(Token, File.ReadAllText(Path.Combine(Local(State("a")), ".git", "config")));
    }

    [Fact]
    public async Task A_launch_reads_while_a_push_is_in_flight_and_a_stalled_push_is_cut_and_undone()
    {
        var library = Open(State("a"), new LibraryRemoteSettings(_bare) { Timeout = TimeSpan.FromSeconds(3) });
        var saved = await library.SaveAsync("orders", Job, "create", Ct);
        var local = Local(State("a"));
        var head = Git(local, "rev-parse", "HEAD");
        var started = Path.Combine(_dir, "started");
        Hook("pre-receive", $"touch '{started}'; sleep 30");

        var edit = library.SaveAsync("orders", Edited, "edit", Ct);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!File.Exists(started) && DateTime.UtcNow < deadline) await Task.Delay(20, Ct);
        Assert.True(File.Exists(started));

        // The job a run was armed with is read without waiting for the push.
        Assert.Equal(Job, await library.JobAtAsync("orders", saved.Commit!, Ct));
        Assert.False(edit.IsCompleted);

        var ex = await Assert.ThrowsAsync<StoreUnavailableException>(() => edit);
        Assert.Contains("did not answer within 3 s", ex.Message);
        Assert.Equal(head, Git(local, "rev-parse", "HEAD"));
        Assert.Equal(Job, File.ReadAllText(Path.Combine(local, "pipelines", "orders", "pipeline.yaml")));
        Assert.Empty(Git(local, "status", "--porcelain"));
    }

    [Fact]
    public async Task A_remote_that_cannot_be_reached_refuses_the_write_before_it_touches_the_disk()
    {
        var library = Open(State("a"));
        await library.SaveAsync("orders", Job, "create", Ct);
        var head = Git(Local(State("a")), "rev-parse", "HEAD");
        Directory.Delete(_bare, recursive: true);

        var ex = await Assert.ThrowsAsync<StoreUnavailableException>(() => library.SaveAsync("orders", Edited, "edit", Ct));

        Assert.Contains("could not be reached", ex.Message);
        Assert.Equal(head, Git(Local(State("a")), "rev-parse", "HEAD"));
        Assert.Equal(Job, (await library.GetAsync("orders", Ct))!.Yaml);
        await Assert.ThrowsAsync<StoreUnavailableException>(() => Open(State("b")).OpenAsync(Ct));
    }

    [Theory]
    [InlineData("https://ann:pw@git.example/lib.git")]
    [InlineData("https://git.example/lib.git?token=x")]
    [InlineData("ssh://git@git.example/lib.git")]
    [InlineData("git@git.example:lib.git")]
    [InlineData("lib.git")]
    public void A_remote_that_carries_credentials_or_speaks_ssh_is_refused_at_start(string url)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Settings(("Url", url)));
        Assert.DoesNotContain(url, ex.Message);
    }

    [Fact]
    public void A_token_is_for_an_https_remote_and_needs_a_remote()
    {
        Assert.Throws<InvalidOperationException>(() => Settings(("Url", "http://git.example/lib.git"), ("Token", "t")));
        Assert.Throws<InvalidOperationException>(() => Settings(("Url", "/srv/lib.git"), ("Token", "t")));
        Assert.Throws<InvalidOperationException>(() => Settings(("Token", "t")));
        Assert.Null(Settings());
        var https = Settings(("Url", "https://git.example/lib.git"), ("Token", "t"), ("User", "ann"), ("Timeout", "00:00:05"))!;
        Assert.Equal(("t", "ann", TimeSpan.FromSeconds(5)), (https.Token, https.User, https.Timeout));
    }

    private static LibraryRemoteSettings? Settings(params (string Key, string Value)[] values) => LibraryRemoteSettings.FromConfiguration(
        new Microsoft.Extensions.Configuration.ConfigurationBuilder()
            .AddInMemoryCollection(values.ToDictionary(v => "Coordinator:Library:Remote:" + v.Key, v => (string?)v.Value))
            .Build().GetSection("Coordinator"));

    /// <summary>The route answers 503 with the server's own words when the remote refuses, and writes nothing.</summary>
    [Fact]
    public async Task A_save_over_http_answers_503_when_the_remote_refuses()
    {
        using var root = new CoordinatorHost(check: _ => new DesignCheck(true, null));
        using var host = root.WithWebHostBuilder(b =>
        {
            b.UseSetting("Coordinator:Library:Remote:Url", _bare);
            b.UseSetting("Coordinator:Initialize", "true");
        });
        using var client = host.CreateClient();
        client.DefaultRequestHeaders.Authorization = new("Bearer", CoordinatorHost.Person(CoordinatorHost.Designer));
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        Hook("pre-receive", "echo 'private reason' >&2; exit 1");

        using var response = await client.PutAsJsonAsync(CoordinatorApi.Library("orders"), new LibrarySave(Job, "m"), Ct);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync(Ct);
        Assert.Contains("refused the save, so nothing was saved", body);
        Assert.DoesNotContain("private reason", body);
        using var read = await client.GetAsync(CoordinatorApi.Library("orders"), Ct);
        Assert.Equal(HttpStatusCode.NotFound, read.StatusCode);
    }
}
