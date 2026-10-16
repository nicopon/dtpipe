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
/// The studio's real run: a review stops every branch that writes just before its writer, holds every row, and
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

    private static object At(string branch, string at) => new { branch, at };

    private string DebugFile => Path.Combine(_dir, ".dtpipe", "studio", "job.yaml-debug.json");

    /// <summary>Two branches that write, each from its own transformer: the shape whose review holds two queues.</summary>
    private string Twin() =>
        "a:\n  input: generate:20\nb:\n  from: a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex % 2 == 0: ''\n  output: csv:b.csv"
        + "\nc:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex * 10: ''\n  output: csv:c.csv\n";

    private Task<(HttpStatusCode Status, JsonElement Body)> WatchAsync(string yaml, params object[] watch) =>
        PostAsync("/api/debug/state", new { yaml, rows = 20, path = "job.yaml", watch });

    private async Task<(string Digest, JsonElement Prepared)> PrepareAsync(string hash, bool review)
    {
        var (status, prepared) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true, review });
        Assert.True(status == HttpStatusCode.OK, prepared.ToString());
        return (prepared.GetProperty("digest").GetString()!, prepared);
    }

    private async Task<JsonElement> RunAsync(string hash, bool review, string? digest = null)
    {
        digest ??= (await PrepareAsync(hash, review)).Digest;
        var (status, body) = await PostAsync("/api/run", new { path = "job.yaml", hash, debug = true, review, digest });
        Assert.True(status == HttpStatusCode.Accepted, body.ToString());
        for (var i = 0; i < 300; i++)
        {
            var now = await StatusAsync();
            if (now.GetProperty("started").GetBoolean() && !now.GetProperty("running").GetBoolean()) return now;
            await Task.Delay(100, Ct);
        }
        throw new TimeoutException("the run did not finish");
    }

    /// <summary>The queue of a branch that writes: where a suspended run waits, with the key of its rows and their count when they are held.</summary>
    private static JsonElement QueueOf(JsonElement view, string branch) =>
        view.GetProperty("queue").EnumerateArray().First(q => q.GetProperty("branch").GetString() == branch);

    private static string? KeyOf(JsonElement view, string branch) => QueueOf(view, branch).GetProperty("key") is { ValueKind: JsonValueKind.String } key ? key.GetString() : null;

    private static string? PinKeyOf(JsonElement view, string branch, string at) => view.GetProperty("watch").EnumerateArray()
        .First(w => w.GetProperty("branch").GetString() == branch && w.GetProperty("at").GetString() == at).GetProperty("key") is { ValueKind: JsonValueKind.String } key
        ? key.GetString() : null;

    private int Pins() => JsonDocument.Parse(File.ReadAllText(DebugFile)).RootElement.GetProperty("watch").GetArrayLength();

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
    public void A_branch_that_runs_whole_is_stopped_before_its_writer_with_the_writer_s_keys_left_out()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\n    csv-writer:\n      separator: ';'\n"
            + "  prefix: p\n  contract-save: c.json\n  output: csv:b.csv\n";

        var stopped = Parsed(Cutter().Cut(job, [], new DebugWatch("b", DebugAt.End), write: true));
        var reviewed = Parsed(Cutter().Cut(job, [], null, null, write: true, beforeWriters: true));

        foreach (var cut in new[] { stopped, reviewed })
        {
            Assert.Equal("null:", cut["b"].Output);
            Assert.Equal("level-0", cut["b"].Checkpoint);
            Assert.Null(cut["b"].Prefix);
            Assert.Null(cut["b"].ContractSave);
            Assert.Equal(["sql"], cut["b"].ProviderOptions!.Keys.ToArray());
        }
        // Without a stop the branch is the job's own.
        Assert.Contains("b.csv", Cutter().Cut(job, [new DebugWatch("b", DebugAt.End)], null, null, write: true).Yaml);
    }

    [Fact]
    public async Task A_branch_without_a_transformer_stops_before_its_writer_too()
    {
        const string job = "a:\n  input: generate:20\n  output: csv:out.csv\n";
        var hash = await SaveAsync(job);

        var stopped = await RunAsync(hash, review: true);

        Assert.Equal(0, stopped.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        var done = await RunAsync(hash, review: false);
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

        var (_, plan) = await PrepareAsync(hash, review: true);
        var staged = plan.GetProperty("staged");
        Assert.False(staged.GetProperty("writesNow").GetBoolean());
        Assert.Equal("a", staged.GetProperty("holds")[0].GetString());
        Assert.All(staged.GetProperty("stages").EnumerateArray(), s => Assert.Equal(JsonValueKind.Null, s.GetProperty("writes").ValueKind));

        var stopped = await RunAsync(hash, review: true);

        Assert.Equal(0, stopped.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        Assert.False(File.Exists(State));
        var view = stopped.GetProperty("view");
        Assert.True(view.GetProperty("real").GetBoolean());
        var key = KeyOf(view, "a");
        Assert.NotNull(key);
        // The rows that wait are counted, and no card was pinned to hold them.
        Assert.Equal(20, QueueOf(view, "a").GetProperty("rows").GetInt64());
        Assert.Equal(0, view.GetProperty("watch").GetArrayLength());
        Assert.Equal(0, Pins());

        // What is held can be looked at before anything is written.
        var (_, looked) = await PostAsync("/api/debug/rows", new { path = "job.yaml", key });
        Assert.Equal(20, looked.GetProperty("stage").GetProperty("rowsSeen").GetInt32());

        // The confirmation of the continuation says what it writes and what it reads back.
        var (digest, next) = await PrepareAsync(hash, review: false);
        var after = next.GetProperty("staged");
        Assert.True(after.GetProperty("writesNow").GetBoolean());
        Assert.Equal("its checkpoint", after.GetProperty("stages")[0].GetProperty("reads").GetString());
        Assert.Contains("out.csv", after.GetProperty("stages")[0].GetProperty("writes").GetString());
        Assert.True(after.GetProperty("held")[0].GetProperty("bytes").GetInt64() > 0);

        var done = await RunAsync(hash, review: false, digest);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(21, File.ReadAllLines(Out).Length);
        Assert.True(File.Exists(State));
        Assert.Contains("\"value\": \"19\"", File.ReadAllText(State).Replace("\r", ""));
        // The write consumed the rows that fed it: the next run reads the source again.
        Assert.False(done.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.Empty(done.GetProperty("view").GetProperty("queue").EnumerateArray());
        Assert.False(File.Exists(DebugFile));
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

        await WatchAsync(Plain, At("a", "read"), At("a", "end"));
        var first = await RunAsync(hash, review: true);
        Assert.Equal(0, first.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Out));
        // The end of a branch cut at its read is held as well: the tail keeps its checkpoint when the writer's keys go.
        Assert.Equal(2, JsonDocument.Parse(File.ReadAllText(DebugFile)).RootElement.GetProperty("levels").GetArrayLength());
        var done = await RunAsync(hash, review: false);

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
        await WatchAsync(Plain, At("a", "end"));
        var stopped = await RunAsync(hash, review: true);
        var key = KeyOf(stopped.GetProperty("view"), "a")!;

        var (sample, _) = await PostAsync("/api/debug", new { yaml = Plain, rows = 20, path = "job.yaml", upTo = At("a", "end") });
        var (change, _) = await WatchAsync(Plain, At("a", "read"));
        // Asking what it holds changes nothing, and the rows are still there for the page that reloads.
        var (asked, held) = await PostAsync("/api/debug/state", new { yaml = Plain, rows = 20, path = "job.yaml" });
        Assert.Equal(HttpStatusCode.OK, asked);
        Assert.True(held.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.Equal(key, KeyOf(held.GetProperty("view"), "a"));
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
        var sampled = PinKeyOf(sample.GetProperty("view"), "a", "end")!;
        Assert.True(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, sampled)));

        var (_, plan) = await PrepareAsync(hash, review: false);
        Assert.Equal(1, plan.GetProperty("staged").GetProperty("discards").GetInt32());
        Assert.Equal(0, plan.GetProperty("staged").GetProperty("held").GetArrayLength());

        var done = await RunAsync(hash, review: false);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(21, File.ReadAllLines(Out).Length);
        Assert.False(Directory.Exists(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, sampled)));
    }

    [Fact]
    public async Task A_cursors_state_changed_since_the_rows_were_read_forgets_them()
    {
        var job = Cursored();
        var hash = await SaveAsync(job);
        var stopped = await RunAsync(hash, review: true);
        var key = KeyOf(stopped.GetProperty("view"), "a")!;
        var (_, held) = await PrepareAsync(hash, review: false);
        Assert.Equal(1, held.GetProperty("staged").GetProperty("held").GetArrayLength());

        // Another run advanced the cursor: these rows would be written again, so the run is no longer one to continue.
        File.WriteAllText(State, "{\"version\":1,\"cursor\":{\"column\":\"GenerateIndex\",\"value\":\"7\",\"type\":\"string\"}}");
        var (status, changed) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true });

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("does not hold the rows of every target", changed.GetProperty("error").GetString());
        Assert.NotEmpty(key);
    }

    [Fact]
    public async Task A_confirmation_binds_the_mode_and_what_is_held()
    {
        var hash = await SaveAsync(Plain);
        await WatchAsync(Plain, At("a", "read"), At("a", "end"));
        var (digest, _) = await PrepareAsync(hash, review: true);

        var (other, body) = await PostAsync("/api/run", new { path = "job.yaml", hash, debug = true, review = false, digest });

        Assert.Equal(HttpStatusCode.Conflict, other);
        Assert.Contains("confirm", body.GetProperty("error").GetString());
        Assert.False(File.Exists(Out));
    }

    // ---- the review

    [Fact]
    public void A_review_stops_every_branch_that_writes_and_none_writes_before_it()
    {
        var cutter = Cutter();
        var plan = cutter.Plan(Twin(), 0);

        var cut = cutter.Cut(Twin(), [], null, null, write: true, beforeWriters: true);

        Assert.Equal(["b", "c"], plan.Branches.Where(b => b.Writes).Select(b => b.Branch).Order());
        Assert.All(Parsed(cut).Values, piece => Assert.True(piece.Output is null or "null:", piece.Output));
        Assert.DoesNotContain("csv:", cut.Yaml);
        // Both ends keep a checkpoint, so the continuation can read each back.
        Assert.All(Parsed(cut).Where(p => p.Key is "b" or "c"), p => Assert.False(string.IsNullOrEmpty(p.Value.Checkpoint)));
    }

    [Fact]
    public void A_review_of_a_job_no_branch_of_which_writes_is_refused()
    {
        var error = Assert.Throws<InvalidOperationException>(() =>
            Cutter().Cut("a:\n  input: generate:20\nb:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n", [], null, null, write: true, beforeWriters: true));

        Assert.Contains("No branch of this job writes", error.Message);
    }

    [Fact]
    public async Task A_review_of_two_targets_writes_none_holds_both_and_pins_nothing_then_continuing_writes_both()
    {
        var hash = await SaveAsync(Twin());

        var reviewed = await RunAsync(hash, review: true);

        Assert.Equal(0, reviewed.GetProperty("code").GetInt32());
        Assert.False(File.Exists(Path.Combine(_dir, "b.csv")));
        Assert.False(File.Exists(Path.Combine(_dir, "c.csv")));
        var view = reviewed.GetProperty("view");
        Assert.True(view.GetProperty("real").GetBoolean());
        Assert.Equal(10, QueueOf(view, "b").GetProperty("rows").GetInt64());
        Assert.Equal(20, QueueOf(view, "c").GetProperty("rows").GetInt64());
        Assert.Equal(0, view.GetProperty("watch").GetArrayLength());
        // The file keeps what the person pinned and what the run holds, and the holds are no pin.
        Assert.Equal(0, Pins());

        // The queues survive a reload: asked again, the state tells the same.
        var (_, again) = await PostAsync("/api/debug/state", new { yaml = Twin(), rows = 20, path = "job.yaml" });
        Assert.Equal(20, QueueOf(again.GetProperty("view"), "c").GetProperty("rows").GetInt64());

        var done = await RunAsync(hash, review: false);

        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(11, File.ReadAllLines(Path.Combine(_dir, "b.csv")).Length);
        Assert.Equal(21, File.ReadAllLines(Path.Combine(_dir, "c.csv")).Length);
        Assert.False(File.Exists(DebugFile));
    }

    [Fact]
    public async Task A_review_leaves_the_pins_a_person_made_as_they_were()
    {
        var hash = await SaveAsync(Twin());
        await WatchAsync(Twin(), At("a", "read"));

        var reviewed = await RunAsync(hash, review: true);

        Assert.Equal(1, reviewed.GetProperty("view").GetProperty("watch").GetArrayLength());
        Assert.Equal(1, Pins());
    }

    [Fact]
    public async Task A_suspended_run_is_continued_whole_or_not_at_all()
    {
        var hash = await SaveAsync(Twin());
        await RunAsync(hash, review: true);

        // One target lost its rows: continuing would read it from its source and write what nobody reviewed.
        var state = System.Text.Json.Nodes.JsonNode.Parse(File.ReadAllText(DebugFile))!;
        var levels = (System.Text.Json.Nodes.JsonArray)state["levels"]!;
        levels.RemoveAt(0);
        File.WriteAllText(DebugFile, state.ToJsonString());
        var (status, body) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true });

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("does not hold the rows of every target", body.GetProperty("error").GetString());
        // Abandoning is the way out.
        var (abandon, view) = await PostAsync("/api/debug/state", new { yaml = Twin(), rows = 20, path = "job.yaml", resetAll = true });
        Assert.Equal(HttpStatusCode.OK, abandon);
        Assert.False(view.GetProperty("view").GetProperty("real").GetBoolean());
        Assert.False(File.Exists(Path.Combine(_dir, "b.csv")));
    }

    [Fact]
    public async Task While_a_run_is_suspended_only_its_continuation_runs()
    {
        var hash = await SaveAsync(Plain);
        await RunAsync(hash, review: true);

        var (plain, body) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash });
        var (another, _) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true, review = true });
        var (continuation, _) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true });

        Assert.Equal(HttpStatusCode.Conflict, plain);
        Assert.Contains("suspended", body.GetProperty("error").GetString());
        Assert.Equal(HttpStatusCode.Conflict, another);
        Assert.Equal(HttpStatusCode.OK, continuation);
    }

    [Fact]
    public async Task A_review_of_a_job_no_branch_of_which_writes_is_refused_at_the_confirmation()
    {
        var none = await SaveAsync("a:\n  input: generate:20\nb:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n");

        var (status, body) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash = none, debug = true, review = true });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, status);
        Assert.Contains("No branch of this job writes", body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_review_holds_the_rows_of_a_branch_with_a_processor_and_writes_them_when_continued()
    {
        var b = Path.Combine(_dir, "b.csv");
        var hash = await SaveAsync("a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a WHERE GenerateIndex < 7\n"
            + "  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex > 1: ''\n  output: csv:b.csv\n");

        var reviewed = await RunAsync(hash, review: true);

        Assert.Equal(0, reviewed.GetProperty("code").GetInt32());
        Assert.False(File.Exists(b));
        var done = await RunAsync(hash, review: false);
        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(1 + 5, File.ReadAllLines(b).Length);
    }

    [Fact]
    public async Task A_suspended_state_an_earlier_studio_left_shows_every_target_waiting_and_is_abandoned_from_there()
    {
        var hash = await SaveAsync(Plain);
        Directory.CreateDirectory(Path.GetDirectoryName(DebugFile)!);
        File.WriteAllText(DebugFile, "{\"session\":\"studio-debug\",\"real\":true,\"watch\":[{\"branch\":\"a\",\"at\":\"read\"}],"
            + "\"levels\":[{\"branch\":\"a\",\"level\":0,\"digest\":\"x\",\"key\":\"deadbeefdeadbeef\"}]}");

        var (_, asked) = await PostAsync("/api/debug/state", new { yaml = Plain, rows = 20, path = "job.yaml" });
        var view = asked.GetProperty("view");
        var (status, _) = await PostAsync("/api/run/prepare", new { path = "job.yaml", hash, debug = true });
        var (abandon, after) = await PostAsync("/api/debug/state", new { yaml = Plain, rows = 20, path = "job.yaml", resetAll = true });

        Assert.True(view.GetProperty("real").GetBoolean());
        Assert.Null(KeyOf(view, "a"));
        Assert.Equal(JsonValueKind.Null, QueueOf(view, "a").GetProperty("rows").ValueKind);
        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Equal(HttpStatusCode.OK, abandon);
        Assert.False(after.GetProperty("view").GetProperty("real").GetBoolean());
    }

    [Fact]
    public async Task A_review_holds_the_rows_of_sinks_that_only_write_what_another_branch_delivers()
    {
        var b = Path.Combine(_dir, "b.csv");
        var e = Path.Combine(_dir, "e.csv");
        var job = "a:\n  input: generate:20\nb:\n  from: a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex % 2 == 0: ''\nc:\n  from: b\n  output: csv:b.csv"
            + "\nd:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex * 10: ''\ne:\n  from: d\n  output: csv:e.csv\n";
        var hash = await SaveAsync(job);

        var reviewed = await RunAsync(hash, review: true);

        Assert.Equal(0, reviewed.GetProperty("code").GetInt32());
        Assert.False(File.Exists(b));
        Assert.False(File.Exists(e));
        var view = reviewed.GetProperty("view");
        Assert.True(view.GetProperty("real").GetBoolean());
        Assert.Equal(10, QueueOf(view, "c").GetProperty("rows").GetInt64());
        Assert.Equal(20, QueueOf(view, "e").GetProperty("rows").GetInt64());

        // The page asks again with the cards it composes, which join each sink to its branch: the queue still names the branches the rows are held under.
        var composed = "a:\n  input: generate:20\nb:\n  from: a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex % 2 == 0: ''\n  output: csv:b.csv"
            + "\nd:\n  from: a\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex * 10: ''\n  output: csv:e.csv\n";
        var (_, again) = await PostAsync("/api/debug/state", new { yaml = composed, rows = 20, path = "job.yaml" });
        Assert.Equal(10, QueueOf(again.GetProperty("view"), "c").GetProperty("rows").GetInt64());
        Assert.Equal(20, QueueOf(again.GetProperty("view"), "e").GetProperty("rows").GetInt64());

        var done = await RunAsync(hash, review: false);
        Assert.Equal(0, done.GetProperty("code").GetInt32());
        Assert.Equal(11, File.ReadAllLines(b).Length);
        Assert.Equal(21, File.ReadAllLines(e).Length);
    }
}
