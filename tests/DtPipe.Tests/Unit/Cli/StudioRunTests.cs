using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The studio's real run of the saved job: confirmed first, one engine action at a time, stopped with
/// the verdict 130 whatever the engine answers to its cancellation, and never beside a sample.
/// </summary>
[Collection("console-serial")]
public sealed class StudioRunTests : IAsyncLifetime
{
    private const string Linear = "main:\n  input: \"generate:2000000000\"\n  output: \"null:\"\n";
    private const string Dag = "a:\n  input: \"generate:2000000000\"\n  output: \"null:\"\nb:\n  input: \"generate:2000000000\"\n  output: \"null:\"\n";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-run-").FullName;
    private readonly HttpClient _http = new(new HttpClientHandler { AllowAutoRedirect = false });
    private readonly StudioFakes _fakes = new();
    private CoordinatorLink? _link;
    private StudioServer? _server;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (_server is not null) await _server.DisposeAsync();
        _link?.Dispose();
        await _fakes.DisposeAsync();
        _http.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static StudioEngine Real()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        return StudioEngines.Real(services.BuildServiceProvider());
    }

    private async Task StartAsync(Func<string, StudioBindings, ConcurrentDictionary<string, string>, CancellationToken, Task<int>>? runner = null,
        Func<string, int, StudioBindings, CancellationToken, Task<string>>? sampler = null, bool coordinator = false)
    {
        IStudioEngine engine = runner is null
            ? Real()
            : new FakeStudioEngine { Run = runner, Sample = sampler ?? ((_, _, _, _) => Task.FromResult("{}")) };
        if (coordinator)
        {
            await _fakes.StartAsync();
            _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl));
        }
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            engine, new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link,
            _link is null ? null : lookup => new DesignService(lookup)), Ct);
        if (coordinator)
        {
            var (_, login) = await CallAsync(HttpMethod.Post, "/api/coordinator/login", new { });
            using var authorize = await _http.GetAsync(login.GetProperty("url").GetString(), Ct);
            using var callback = await _http.GetAsync(authorize.Headers.Location, Ct);
            Assert.Equal(HttpStatusCode.OK, callback.StatusCode);
            await CallAsync(HttpMethod.Get, "/api/bricks");
        }
    }

    private async Task<(HttpStatusCode Status, JsonElement Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_server!.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, Ct);
        var text = await response.Content.ReadAsStringAsync(Ct);
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    private async Task<string> SaveAsync(string yaml, string name = "job.yaml")
    {
        File.WriteAllText(Path.Combine(_dir, name), yaml);
        return (await CallAsync(HttpMethod.Get, $"/api/job?path={name}")).Body.GetProperty("hash").GetString()!;
    }

    private async Task<string> ConfirmAsync(string hash, string name = "job.yaml")
    {
        var (status, prepared) = await CallAsync(HttpMethod.Post, "/api/run/prepare", new { path = name, hash });
        Assert.Equal(HttpStatusCode.OK, status);
        return prepared.GetProperty("digest").GetString()!;
    }

    private async Task<HttpStatusCode> RunAsync(string hash, string digest, string name = "job.yaml") =>
        (await CallAsync(HttpMethod.Post, "/api/run", new { path = name, hash, digest })).Status;

    private async Task<JsonElement> UntilAsync(Func<JsonElement, bool> done)
    {
        for (var i = 0; i < 300; i++)
        {
            var (_, status) = await CallAsync(HttpMethod.Get, "/api/run");
            if (done(status)) return status;
            await Task.Delay(100, Ct);
        }
        throw new TimeoutException("the run did not reach the state awaited");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_stopped_run_ends_with_130_in_a_linear_job_and_in_a_dag(bool dag)
    {
        await StartAsync();
        var hash = await SaveAsync(dag ? Dag : Linear);
        Assert.Equal(HttpStatusCode.Accepted, await RunAsync(hash, await ConfirmAsync(hash)));
        await Task.Delay(1500, Ct);
        Assert.True((await CallAsync(HttpMethod.Get, "/api/run")).Body.GetProperty("running").GetBoolean());

        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/run/stop")).Status);
        var done = await UntilAsync(s => !s.GetProperty("running").GetBoolean());

        Assert.Equal(130, done.GetProperty("code").GetInt32());
        Assert.True(done.GetProperty("stopRequested").GetBoolean());
    }

    [Theory]
    [InlineData("throws")]
    [InlineData("answers 1")]
    [InlineData("answers 0")]
    public async Task A_stop_is_130_whatever_the_engine_answers_to_it(string how)
    {
        var started = new TaskCompletionSource();
        await StartAsync((yaml, b, f, ct) => RunStub(how, started, ct));
        var hash = await SaveAsync(Linear);
        await RunAsync(hash, await ConfirmAsync(hash));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

        await CallAsync(HttpMethod.Post, "/api/run/stop");
        var done = await UntilAsync(s => !s.GetProperty("running").GetBoolean());

        Assert.Equal(130, done.GetProperty("code").GetInt32());
    }

    private static async Task<int> RunStub(string how, TaskCompletionSource started, CancellationToken ct)
    {
        started.TrySetResult();
        try { await Task.Delay(Timeout.Infinite, ct); }
        catch (OperationCanceledException) when (how != "throws") { }
        return how == "answers 0" ? 0 : 1;
    }

    [Fact]
    public async Task A_run_that_finished_before_a_stop_keeps_its_code_and_a_stop_then_is_refused()
    {
        await StartAsync((_, _, _, _) => Task.FromResult(3));
        var hash = await SaveAsync(Linear);
        await RunAsync(hash, await ConfirmAsync(hash));
        var done = await UntilAsync(s => s.GetProperty("started").GetBoolean() && !s.GetProperty("running").GetBoolean());

        Assert.Equal(3, done.GetProperty("code").GetInt32());
        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Post, "/api/run/stop")).Status);
        Assert.Equal(3, (await CallAsync(HttpMethod.Get, "/api/run")).Body.GetProperty("code").GetInt32());
    }

    [Fact]
    public async Task While_a_run_goes_on_nothing_else_runs_and_the_studio_does_not_quit()
    {
        var release = new TaskCompletionSource();
        var started = new TaskCompletionSource();
        await StartAsync(async (_, _, _, ct) => { started.TrySetResult(); await release.Task.WaitAsync(ct); return 0; },
            (_, _, _, _) => Task.FromResult("{}"));
        var hash = await SaveAsync(Linear);
        var digest = await ConfirmAsync(hash);
        await RunAsync(hash, digest);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(HttpStatusCode.Conflict, await RunAsync(hash, digest));
        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Post, "/api/sample", new { yaml = Linear, rows = 3 })).Status);
        var (quit, answer) = await CallAsync(HttpMethod.Post, "/api/quit");
        Assert.Equal(HttpStatusCode.Conflict, quit);
        Assert.Contains("run", answer.GetProperty("error").GetString());

        release.SetResult();
        await UntilAsync(s => !s.GetProperty("running").GetBoolean());
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/quit")).Status);
    }

    [Fact]
    public async Task The_confirmation_lists_what_is_written_and_a_run_needs_it_unchanged()
    {
        await StartAsync((_, _, _, _) => Task.FromResult(0));
        const string job = "main:\n  input: \"generate:3\"\n  output: \"pg:Host=db;Username=u;Password=hunter2;Database=d\"\n  provider-options:\n    pg-writer:\n      table: orders\n";
        var hash = await SaveAsync(job);

        var (status, prepared) = await CallAsync(HttpMethod.Post, "/api/run/prepare", new { path = "job.yaml", hash });

        Assert.Equal(HttpStatusCode.OK, status);
        var write = Assert.Single(prepared.GetProperty("writes").EnumerateArray());
        Assert.Equal("main", write.GetProperty("alias").GetString());
        Assert.DoesNotContain("hunter2", write.GetProperty("output").GetString());
        Assert.DoesNotContain("hunter2", write.GetProperty("block").GetString());
        Assert.Contains("table: orders", write.GetProperty("block").GetString());
        var digest = prepared.GetProperty("digest").GetString()!;

        Assert.Equal(HttpStatusCode.Conflict, await RunAsync(hash, "0000"));
        // The file changed on disk since the page saved it: the saved hash no longer holds.
        File.AppendAllText(Path.Combine(_dir, "job.yaml"), "# changed\n");
        Assert.Equal(HttpStatusCode.Conflict, await RunAsync(hash, digest));
        var newer = (await CallAsync(HttpMethod.Get, "/api/job?path=job.yaml")).Body.GetProperty("hash").GetString()!;
        Assert.Equal(HttpStatusCode.Conflict, await RunAsync(newer, digest));
        Assert.Equal(HttpStatusCode.Accepted, await RunAsync(newer, await ConfirmAsync(newer)));
    }

    [Fact]
    public async Task The_digest_follows_the_values_the_job_launches_with()
    {
        await StartAsync((_, _, _, _) => Task.FromResult(0));
        var hash = await SaveAsync("main:\n  input: \"generate:${{STUDIO_RUN_N}}\"\n  output: \"null:\"\n");
        async Task Choose(string value) => await CallAsync(HttpMethod.Put, "/api/launch",
            new { path = "job.yaml", overrides = new Dictionary<string, object> { ["STUDIO_RUN_N"] = new { value } } });

        await Choose("3");
        var digest = await ConfirmAsync(hash);
        await Choose("4");

        Assert.Equal(HttpStatusCode.Conflict, await RunAsync(hash, digest));
        Assert.Equal(HttpStatusCode.Accepted, await RunAsync(hash, await ConfirmAsync(hash)));
    }

    [Fact]
    public async Task A_failure_of_a_branch_is_shown_without_a_secret()
    {
        await StartAsync((_, _, failures, _) => { failures["main"] = "cannot connect: Password=hunter2;Host=db"; return Task.FromResult(1); });
        var hash = await SaveAsync(Linear);
        await RunAsync(hash, await ConfirmAsync(hash));

        var done = await UntilAsync(s => s.GetProperty("started").GetBoolean() && !s.GetProperty("running").GetBoolean());

        Assert.Equal(1, done.GetProperty("code").GetInt32());
        var shown = done.GetProperty("failures").GetProperty("main").GetString()!;
        Assert.Contains("cannot connect", shown);
        Assert.DoesNotContain("hunter2", shown);
    }

    [Fact]
    public async Task A_run_writes_to_the_real_target_and_leaves_the_environment_alone()
    {
        await StartAsync();
        var target = Path.Combine(_dir, "out.csv");
        var hash = await SaveAsync($"main:\n  input: \"generate:5\"\n  output: \"csv:{target}\"\n");
        Assert.Null(Environment.GetEnvironmentVariable("STUDIO_RUN_N"));

        await RunAsync(hash, await ConfirmAsync(hash));
        var done = await UntilAsync(s => s.GetProperty("started").GetBoolean() && !s.GetProperty("running").GetBoolean());

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(6, File.ReadAllLines(target).Length);
    }

    [Fact]
    public async Task A_job_that_reads_a_brick_is_refused_by_name_before_anything_runs()
    {
        await StartAsync(coordinator: true);
        var hash = await SaveAsync("orders:\n  input: \"brick:n1/orders\"\nmain:\n  from: orders\n  output: \"null:\"\n");

        var (status, body) = await CallAsync(HttpMethod.Post, "/api/run/prepare",
            new { path = "job.yaml", hash, bricks = new[] { new { branch = "orders", key = StudioFakes.SourceKey } } });

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("'orders'", body.GetProperty("error").GetString());
        Assert.Contains(StudioFakes.SourceKey, body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_reference_no_card_binds_is_refused_by_name_before_it_reaches_the_engine()
    {
        await StartAsync(coordinator: true);
        var hash = await SaveAsync("orders:\n  input: \"brick:n1/orders\"\nmain:\n  from: orders\n  output: \"null:\"\n");

        var (status, body) = await CallAsync(HttpMethod.Post, "/api/run/prepare", new { path = "job.yaml", hash });

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("'orders' (n1/orders)", body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_sink_brick_is_written_to_a_throwaway_file_and_the_confirmation_says_so()
    {
        await StartAsync(coordinator: true);
        var hash = await SaveAsync("a:\n  input: \"generate:3\"\nb:\n  from: a\n  output: \"brick:n1/archive\"\n");
        var bricks = new[] { new { branch = "b", key = StudioFakes.SinkKey } };

        var (_, prepared) = await CallAsync(HttpMethod.Post, "/api/run/prepare", new { path = "job.yaml", hash, bricks });
        var write = Assert.Single(prepared.GetProperty("writes").EnumerateArray());
        Assert.True(write.GetProperty("throwaway").GetBoolean());
        Assert.Equal(JsonValueKind.Null, write.GetProperty("output").ValueKind);

        var digest = prepared.GetProperty("digest").GetString()!;
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/run", new { path = "job.yaml", hash, bricks, digest })).Status);
        var done = await UntilAsync(s => s.GetProperty("started").GetBoolean() && !s.GetProperty("running").GetBoolean());

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Path.Combine(_dir, "brick-target.csv")));
        Assert.Empty(Directory.GetFiles(Path.Combine(_dir, ".dtpipe", "studio", "samples")));
    }
}

