using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Split;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The studio's real run cut at the cards a person watches: it stops before the writer, holds every row, and
/// writes only when it is continued. Nothing leaves the process before the last stage: not a row of the target, not
/// the cursor's state.
/// </summary>
[Collection("console-serial")]
public sealed class StudioStagedRunTests : IAsyncLifetime
{
    private readonly string _dir = Directory.CreateTempSubdirectory("studio-staged-").FullName;
    private readonly string _previous = Directory.GetCurrentDirectory();
    private readonly HttpClient _http = new();
    private ServiceProvider _services = null!;
    private StudioServer _server = null!;

    public async ValueTask InitializeAsync()
    {
        Directory.SetCurrentDirectory(_dir);
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        _services = services.BuildServiceProvider();
        var mcp = _services.GetRequiredService<DtPipeMcpTools>();
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            (y, r, b, ct) => mcp.SampleAsync(y, r, shownRows: r, ct, b.Input, b.Output), new DesignService(IBrickLookup.None), [], [],
            new InMemorySecretsManager(),
            Run: (y, b, f, ct) => mcp.RunJobAsync(y, b.Input, b.Output, f, ct),
            Debug: Cutter(),
            DebugSample: (y, r, b, ct) => mcp.SampleAsync(y, r, shownRows: Math.Min(r, 1000), ct, b.Input, b.Output, maxRows: StudioServer.DebugMaxRows),
            DebugRun: (y, b, f, c, ct) => mcp.RunJobAsync(y, b.Input, b.Output, f, ct, c)), CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _server.DisposeAsync();
        Directory.SetCurrentDirectory(_previous);
        await _services.DisposeAsync();
        _http.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private DebugCutter Cutter() => new(
        _services.GetServices<IStreamReaderFactory>().ToList(), _services.GetServices<IDataWriterFactory>().ToList(),
        _services.GetServices<IStreamTransformerFactory>().ToList());

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string Out => Path.Combine(_dir, "out.csv");

    private string State => Path.Combine(_dir, "s.json");

    /// <summary>One branch with a cursor, one transformer, a real writer: the shape whose stop is "just before the writer".</summary>
    private string Cursored(string output = "csv:out.csv") =>
        "a:\n  input: generate:20\n  cursor: GenerateIndex\n  state: " + State.Replace('\\', '/')
        + "\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex * 10: ''\n  output: " + output + "\n";

    private string Plain => "a:\n  input: generate:20\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex * 10: ''\n  output: csv:out.csv\n";

    private async Task<(HttpStatusCode Status, JsonElement Body)> PostAsync(string route, object body)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"http://127.0.0.1:{_server.Port}{route}")
        {
            Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        using var response = await _http.SendAsync(request, Ct);
        var text = await response.Content.ReadAsStringAsync(Ct);
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    private async Task<JsonElement> StatusAsync()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{_server.Port}/api/run");
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        using var response = await _http.SendAsync(request, Ct);
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement.Clone();
    }

    private async Task<string> SaveAsync(string yaml)
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), yaml);
        using var request = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{_server.Port}/api/job?path=job.yaml");
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        using var response = await _http.SendAsync(request, Ct);
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement.GetProperty("hash").GetString()!;
    }

    private static object At(string branch, string at, bool @break = false) => new { branch, at, @break };

    private Task<(HttpStatusCode Status, JsonElement Body)> WatchAsync(string yaml, params object[] watch) =>
        PostAsync("/api/debug/state", new { yaml, rows = 20, path = "job.yaml", watch });

    private async Task<(string Digest, JsonElement Prepared)> PrepareAsync(string hash, object? upTo)
    {
        var (status, prepared) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true, upTo });
        Assert.Equal(HttpStatusCode.OK, status);
        return (prepared.GetProperty("digest").GetString()!, prepared);
    }

    private async Task<JsonElement> RunAsync(string hash, object? upTo, string? digest = null)
    {
        digest ??= (await PrepareAsync(hash, upTo)).Digest;
        var (status, body) = await PostAsync("/api/run", new { path = "job.yaml", hash, debug = true, upTo, digest });
        Assert.True(status == HttpStatusCode.Accepted, body.ToString());
        for (var i = 0; i < 300; i++)
        {
            var now = await StatusAsync();
            if (now.GetProperty("started").GetBoolean() && !now.GetProperty("running").GetBoolean()) return now;
            await Task.Delay(100, Ct);
        }
        throw new TimeoutException("the run did not finish");
    }

    private static string? KeyOf(JsonElement view, string branch, string at) => view.GetProperty("watch").EnumerateArray()
        .First(w => w.GetProperty("branch").GetString() == branch && w.GetProperty("at").GetString() == at).GetProperty("key") is { ValueKind: JsonValueKind.String } key
        ? key.GetString() : null;

    private static Dictionary<string, JobDefinition> Parsed(DebugCut cut) =>
        DtPipe.Configuration.JobFileParser.ParseContent(cut.Yaml, null, interpolate: false);

    // ---- the cut

    [Fact]
    public void A_stop_in_a_real_run_leaves_no_branch_with_a_writer()
    {
        const string job = "a:\n  input: generate:20\n  output: csv:first.csv\nb:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: csv:second.csv\n";

        var cut = Cutter().Cut(job, [new DebugWatch("b", DebugAt.End)], new DebugWatch("b", DebugAt.End), write: true);

        // The upstream branch writes its own target and the stopped one writes its: neither may, until the stop is past.
        Assert.All(Parsed(cut).Values, piece => Assert.True(piece.Output is null or "null:", piece.Output));
        Assert.DoesNotContain("csv:", cut.Yaml);
        // A sample keeps the output: the engine neutralises the writer there.
        var sample = Cutter().Cut(job, [new DebugWatch("b", DebugAt.End)], new DebugWatch("b", DebugAt.End));
        Assert.Contains("csv:second.csv", sample.Yaml);
    }

    [Fact]
    public void The_cursor_follows_the_piece_that_writes_and_never_a_piece_that_only_holds_a_checkpoint()
    {
        var job = Cursored();

        var cut = Parsed(Cutter().Cut(job, [new DebugWatch("a", DebugAt.Read)], write: true));
        var stopped = Parsed(Cutter().Cut(job, [new DebugWatch("a", DebugAt.Read)], new DebugWatch("a", DebugAt.Read), write: true));
        var key = new string('a', 64);
        var resumed = Parsed(Cutter().Cut(job, [new DebugWatch("a", DebugAt.Read)], null, [new DebugHeld("a", 0, key)], write: true));

        Assert.Null(cut["a__level0"].Cursor);
        Assert.Null(cut["a__level0"].State);
        Assert.Equal("GenerateIndex", cut["a"].Cursor);
        Assert.NotNull(cut["a"].State);
        Assert.All(stopped.Values, piece => { Assert.Null(piece.Cursor); Assert.Null(piece.State); });
        Assert.Equal(key, resumed["a__level0"].FromCheckpoint);
        Assert.Null(resumed["a__level0"].Cursor);
        Assert.Equal("GenerateIndex", resumed["a"].Cursor);
        Assert.NotNull(resumed["a"].State);
        // A sample's cut keeps them where JobCutter puts them: it advances no cursor.
        Assert.Equal("GenerateIndex", Parsed(Cutter().Cut(job, [new DebugWatch("a", DebugAt.Read)]))["a__level0"].Cursor);
    }

    [Fact]
    public void A_branch_that_runs_whole_is_not_stopped_before_a_writer_it_cannot_leave_out()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\n  output: csv:b.csv\n";

        var error = Assert.Throws<InvalidOperationException>(() =>
            Cutter().Cut(job, [new DebugWatch("b", DebugAt.End)], new DebugWatch("b", DebugAt.End), write: true));

        Assert.Contains("runs whole", error.Message);
        Assert.Contains("b.csv", Cutter().Cut(job, [new DebugWatch("b", DebugAt.End)], null, null, write: true).Yaml);
    }

    [Fact]
    public async Task A_branch_without_a_transformer_stops_before_its_writer_too()
    {
        const string job = "a:\n  input: generate:20\n  output: csv:out.csv\n";
        var hash = await SaveAsync(job);
        await WatchAsync(job, At("a", "read", @break: true));

        var stopped = await RunAsync(hash, At("a", "read"));

        Assert.Equal(0, stopped.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        var done = await RunAsync(hash, null);
        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(21, File.ReadAllLines(Out).Length);
    }

    [Fact]
    public void A_real_levels_digest_follows_the_values_it_was_read_with_and_never_a_samples()
    {
        var sample = Cutter().Plan(Plain, 20).Levels.First(l => l.Level == 0).Digest;
        var real = Cutter().Plan(Plain, 0, "one").Levels.First(l => l.Level == 0).Digest;
        var other = Cutter().Plan(Plain, 0, "two").Levels.First(l => l.Level == 0).Digest;

        Assert.NotEqual(sample, real);
        Assert.NotEqual(real, other);
        Assert.Equal(real, Cutter().Plan(Plain, 0, "one").Levels.First(l => l.Level == 0).Digest);
    }

    // ---- the run

    [Fact]
    public async Task A_run_stopped_before_the_writer_writes_no_row_and_moves_no_cursor_then_continuing_writes_them()
    {
        var job = Cursored();
        var hash = await SaveAsync(job);
        await WatchAsync(job, At("a", "end", @break: true));

        var (_, plan) = await PrepareAsync(hash, At("a", "end"));
        var staged = plan.GetProperty("staged");
        Assert.False(staged.GetProperty("writesNow").GetBoolean());
        Assert.All(staged.GetProperty("stages").EnumerateArray(), s => Assert.Equal(JsonValueKind.Null, s.GetProperty("writes").ValueKind));

        var stopped = await RunAsync(hash, At("a", "end"));

        Assert.Equal(0, stopped.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        Assert.False(File.Exists(State));
        var view = stopped.GetProperty("view");
        Assert.True(view.GetProperty("real").GetBoolean());
        var key = KeyOf(view, "a", "end");
        Assert.NotNull(key);

        // What is held can be looked at before anything is written.
        var (_, looked) = await PostAsync("/api/debug/rows", new { path = "job.yaml", key });
        Assert.Equal(20, looked.GetProperty("stage").GetProperty("rowsSeen").GetInt32());

        // The confirmation of the continuation says what it writes and what it reads back.
        var (digest, next) = await PrepareAsync(hash, null);
        var after = next.GetProperty("staged");
        Assert.True(after.GetProperty("writesNow").GetBoolean());
        Assert.Equal("its checkpoint", after.GetProperty("stages")[0].GetProperty("reads").GetString());
        Assert.Contains("out.csv", after.GetProperty("stages")[0].GetProperty("writes").GetString());
        Assert.True(after.GetProperty("held")[0].GetProperty("bytes").GetInt64() > 0);

        var done = await RunAsync(hash, null, digest);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(21, File.ReadAllLines(Out).Length);
        Assert.True(File.Exists(State));
        Assert.Contains("\"value\": \"19\"", File.ReadAllText(State).Replace("\r", ""));
        // The write consumed the rows that fed it: the next run reads the source again.
        Assert.False(done.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.Null(KeyOf(done.GetProperty("view"), "a", "end"));
        Assert.False(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, key!)));
    }

    [Fact]
    public async Task A_staged_run_writes_the_rows_a_plain_run_writes_in_the_same_order()
    {
        var hash = await SaveAsync(Plain);
        var plain = await RunPlainAsync(hash);
        Assert.Equal(0, plain.GetProperty("code").GetInt32());
        var expected = File.ReadAllText(Out);
        File.Delete(Out);

        await WatchAsync(Plain, At("a", "read", @break: true), At("a", "end", @break: true));
        var first = await RunAsync(hash, At("a", "read"));
        Assert.Equal(0, first.GetProperty("code").GetInt32());
        var second = await RunAsync(hash, At("a", "end"));
        Assert.Equal(0, second.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        var done = await RunAsync(hash, null);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(expected, File.ReadAllText(Out));
    }

    private async Task<JsonElement> RunPlainAsync(string hash)
    {
        var (_, prepared) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash });
        await PostAsync("/api/run", new { path = "job.yaml", hash, digest = prepared.GetProperty("digest").GetString() });
        for (var i = 0; i < 300; i++)
        {
            var now = await StatusAsync();
            if (now.GetProperty("started").GetBoolean() && !now.GetProperty("running").GetBoolean()) return now;
            await Task.Delay(100, Ct);
        }
        throw new TimeoutException("the run did not finish");
    }

    [Fact]
    public async Task While_a_real_run_is_suspended_a_sample_and_a_change_of_watchers_are_refused_until_it_is_abandoned()
    {
        var hash = await SaveAsync(Plain);
        await WatchAsync(Plain, At("a", "end", @break: true));
        var stopped = await RunAsync(hash, At("a", "end"));
        var key = KeyOf(stopped.GetProperty("view"), "a", "end")!;

        var (sample, _) = await PostAsync("/api/debug", new { yaml = Plain, rows = 20, path = "job.yaml", upTo = At("a", "end") });
        var (change, _) = await WatchAsync(Plain, At("a", "read"));
        // Asking what it holds changes nothing, and the rows are still there for the page that reloads.
        var (asked, held) = await PostAsync("/api/debug/state", new { yaml = Plain, rows = 20, path = "job.yaml" });
        Assert.Equal(HttpStatusCode.OK, asked);
        Assert.True(held.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.Equal(key, KeyOf(held.GetProperty("view"), "a", "end"));
        var (abandon, view) = await PostAsync("/api/debug/state", new { yaml = Plain, rows = 20, path = "job.yaml", resetAll = true });

        Assert.Equal(HttpStatusCode.Conflict, sample);
        Assert.Equal(HttpStatusCode.Conflict, change);
        Assert.Equal(HttpStatusCode.OK, abandon);
        Assert.False(view.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.False(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, key)));
        Assert.False(File.Exists(Out));
    }

    [Fact]
    public async Task What_a_sample_holds_does_not_serve_a_real_run_and_is_forgotten_when_it_starts()
    {
        var hash = await SaveAsync(Plain);
        await WatchAsync(Plain, At("a", "end"));
        var (_, sample) = await PostAsync("/api/debug", new { yaml = Plain, rows = 20, path = "job.yaml", upTo = At("a", "end") });
        var sampled = KeyOf(sample.GetProperty("view"), "a", "end")!;
        Assert.True(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, sampled)));

        var (_, plan) = await PrepareAsync(hash, null);
        Assert.Equal(1, plan.GetProperty("staged").GetProperty("discards").GetInt32());
        Assert.Equal(0, plan.GetProperty("staged").GetProperty("held").GetArrayLength());

        var done = await RunAsync(hash, null);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(21, File.ReadAllLines(Out).Length);
        Assert.False(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, sampled)));
    }

    [Fact]
    public async Task A_cursors_state_changed_since_the_rows_were_read_forgets_them()
    {
        var job = Cursored();
        var hash = await SaveAsync(job);
        await WatchAsync(job, At("a", "end", @break: true));
        var stopped = await RunAsync(hash, At("a", "end"));
        var key = KeyOf(stopped.GetProperty("view"), "a", "end")!;
        var (_, held) = await PrepareAsync(hash, null);
        Assert.Equal(1, held.GetProperty("staged").GetProperty("held").GetArrayLength());

        // Another run advanced the cursor: these rows would be written again.
        File.WriteAllText(State, "{\"version\":1,\"cursor\":{\"column\":\"GenerateIndex\",\"value\":\"7\",\"type\":\"string\"}}");
        var (_, changed) = await PrepareAsync(hash, null);

        Assert.Equal(0, changed.GetProperty("staged").GetProperty("held").GetArrayLength());
        Assert.Equal(1, changed.GetProperty("staged").GetProperty("discards").GetInt32());
        Assert.NotEmpty(key);
    }

    [Fact]
    public async Task A_confirmation_binds_the_stop_and_what_is_held()
    {
        var hash = await SaveAsync(Plain);
        await WatchAsync(Plain, At("a", "read", @break: true), At("a", "end", @break: true));
        var (digest, _) = await PrepareAsync(hash, At("a", "read"));

        var (other, body) = await PostAsync("/api/run", new { path = "job.yaml", hash, debug = true, upTo = At("a", "end"), digest });

        Assert.Equal(HttpStatusCode.Conflict, other);
        Assert.Contains("confirm", body.GetProperty("error").GetString());
        Assert.False(File.Exists(Out));
    }
}
