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
using DtPipe.Sessions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The debug run: the job cut at the cards a person watches, each watched level kept in the studio's session,
/// and a later run that stops at a card and starts from the levels it holds. The cut is a copy for one sample;
/// the job is never changed.
/// </summary>
[Collection("console-serial")]
public sealed class StudioDebugTests : IAsyncLifetime
{
    private const string Chain = """
        a:
          input: generate:20
        b:
          from: a
          transformers:
          - type: filter
            mappings:
              row.GenerateIndex % 2 == 0: ''
          - type: compute
            mappings:
              x:row.GenerateIndex * 10: ''
        c:
          from: b
          output: "null:"
        """;

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-debug-").FullName;
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
            Debug: Cutter(),
            DebugSample: (y, r, b, ct) => mcp.SampleAsync(y, r, shownRows: Math.Min(r, 1000), ct, b.Input, b.Output, maxRows: StudioServer.DebugMaxRows)), CancellationToken.None);
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

    private static readonly string Draft = new('a', 32);

    private async Task<(HttpStatusCode Status, JsonElement Body)> PostAsync(string route, object body)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"http://127.0.0.1:{_server.Port}{route}")
        {
            Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        using var response = await _http.SendAsync(request, Ct);
        return (response.StatusCode, JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement.Clone());
    }

    private Task<(HttpStatusCode Status, JsonElement Body)> DebugAsync(object body) => PostAsync("/api/debug", body);

    private Task<(HttpStatusCode Status, JsonElement Body)> StateAsync(object body) => PostAsync("/api/debug/state", body);

    private static object At(string branch, string at) => new { branch, at };

    private static JsonElement WatcherOf(JsonElement answer, string branch, string at) => answer.GetProperty("view").GetProperty("watch").EnumerateArray()
        .First(w => w.GetProperty("branch").GetString() == branch && w.GetProperty("at").GetString() == at);

    private static string? KeyOf(JsonElement answer, string branch, string at) =>
        WatcherOf(answer, branch, at).GetProperty("key") is { ValueKind: JsonValueKind.String } key ? key.GetString() : null;

    private string KeyDirectory(string key) => Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, key);

    private async Task<long> RowsOfAsync(string key)
    {
        var store = new CheckpointStore(SessionStore.Resolve(DebugCutter.SessionName, _dir));
        long rows = 0;
        await foreach (var batch in store.ReadAsync(key)) using (batch) rows += batch.Length;
        return rows;
    }

    private static List<string?> Ran(JsonElement answer) =>
        answer.GetProperty("report").GetProperty("branches").EnumerateArray().Select(b => b.GetProperty("branch").GetString()).ToList();

    private static Dictionary<string, JobDefinition> Parsed(DebugCut cut) =>
        DtPipe.Configuration.JobFileParser.ParseContent(cut.Yaml, null, interpolate: false);

    private static DebugWatch Read(string branch) => new(branch, DebugAt.Read);

    private static DebugWatch End(string branch) => new(branch, DebugAt.End);

    // ---- the plan

    [Fact]
    public void A_levels_digest_follows_what_produces_it_and_never_which_levels_are_watched()
    {
        var before = Cutter().Plan(Chain, 20);
        var edited = Cutter().Plan(Chain.Replace("% 2 == 0", "% 3 == 0"), 20);
        var upstream = Cutter().Plan(Chain.Replace("generate:20", "generate:30"), 20);
        var rows = Cutter().Plan(Chain, 21);
        string Digest(DebugPlan plan, string branch, int level) => plan.Levels.First(l => l.Branch == branch && l.Level == level).Digest;

        Assert.Equal(Digest(before, "b", 0), Digest(edited, "b", 0));
        Assert.NotEqual(Digest(before, "b", 2), Digest(edited, "b", 2));
        Assert.NotEqual(Digest(before, "b", 0), Digest(upstream, "b", 0));
        Assert.NotEqual(Digest(before, "b", 0), Digest(rows, "b", 0));
        // c reads b: what b does to its rows is c's.
        Assert.NotEqual(Digest(before, "c", 0), Digest(edited, "c", 0));
        Assert.Equal(before.Levels.Select(l => l.Digest), Cutter().Plan(Chain, 20).Levels.Select(l => l.Digest));
    }

    [Fact]
    public void A_branch_with_a_processor_or_no_transformer_runs_whole_and_is_watched_at_its_end_only()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex > 1: ''\n  output: \"null:\"\n";

        var plan = Cutter().Plan(job, 20);

        Assert.Equal([false, false], plan.Branches.Select(b => b.Cuttable));
        Assert.Equal([("a", 0), ("b", 1)], plan.Levels.Select(l => (l.Branch, l.Level)));
        Assert.Null(plan.Resolve(Read("b")));
        Assert.Equal(1, plan.Resolve(End("b")));
        var refused = Assert.Throws<InvalidOperationException>(() => Cutter().Cut(job, [], Read("b")));
        Assert.Contains("runs whole", refused.Message);

        var cut = Cutter().Cut(job, [End("b"), Read("b")]);

        Assert.Equal([new DebugPiece("b", 1, "b")], cut.Pieces);
        var jobs = Parsed(cut);
        Assert.Equal(["a", "b"], jobs.Keys);
        Assert.Equal("level-1", jobs["b"].Checkpoint);
    }

    // ---- the cut

    [Fact]
    public void Without_a_watcher_nothing_is_cut_and_the_job_is_not_changed()
    {
        var cut = Cutter().Cut(Chain, []);

        Assert.Empty(cut.Pieces);
        Assert.Empty(cut.Dropped);
        Assert.Equal(["a", "b", "c"], Parsed(cut).Keys);
        Assert.Equal(cut.Yaml, Cutter().Cut(Chain, []).Yaml);
        Assert.Contains("generate:20", Chain);
    }

    [Fact]
    public void A_watcher_at_the_read_cuts_the_branch_there_and_the_last_piece_keeps_its_alias_and_its_output()
    {
        const string job = "a:\n  input: generate:20\n  limit: 8\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex % 2 == 0: ''\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: \"null:\"\n  prefix: p\nz:\n  from: a\n  output: \"null:\"\n";

        var cut = Cutter().Cut(job, [Read("a")]);

        Assert.Equal([new DebugPiece("a", 0, "a__level0")], cut.Pieces);
        var jobs = Parsed(cut);
        Assert.Equal(["a__level0", "a", "z"], jobs.Keys);
        // The head keeps the reader and what bounds it; the tail keeps the writer's side and the alias z reads.
        Assert.Equal(("generate:20", 8, null), (jobs["a__level0"].Input, jobs["a__level0"].Limit, jobs["a__level0"].Output));
        Assert.Equal(("null:", "p", "a__level0"), (jobs["a"].Output, jobs["a"].Prefix, jobs["a"].From));
        Assert.Equal(0, jobs["a"].Limit);
        Assert.Equal(2, jobs["a"].Transformers!.Count);
        Assert.Equal("a", jobs["z"].From);
        Assert.Equal("level-0", jobs["a__level0"].Checkpoint);
        Assert.Null(jobs["a"].Checkpoint);
        Assert.All(new[] { "a__level0", "a" }, alias => Assert.Equal(DebugCutter.SessionName, jobs[alias].Session));
    }

    [Fact]
    public void A_watcher_at_the_end_takes_a_checkpoint_without_cutting()
    {
        var cut = Cutter().Cut(Chain, [End("b")]);

        Assert.Equal([new DebugPiece("b", 2, "b")], cut.Pieces);
        var jobs = Parsed(cut);
        Assert.Equal(["a", "b", "c"], jobs.Keys);
        Assert.Equal("level-2", jobs["b"].Checkpoint);
        Assert.Equal(2, jobs["b"].Transformers!.Count);
    }

    [Fact]
    public void An_alias_that_holds_the_reserved_mark_is_refused_by_name()
    {
        var error = Assert.Throws<InvalidOperationException>(() => Cutter().Cut("a__level0:\n  input: generate:3\n  output: \"null:\"\n", []));
        Assert.Contains("a__level0", error.Message);
    }

    // ---- stopping and reading back

    [Fact]
    public void A_stop_keeps_what_the_card_needs_and_drops_the_rest_by_name()
    {
        var atRead = Cutter().Cut(Chain, [], Read("b"));

        Assert.Equal(["a", "b__level0"], Parsed(atRead).Keys);
        Assert.Equal("null:", Parsed(atRead)["b__level0"].Output);
        Assert.Equal(["c"], atRead.Dropped);
        Assert.Equal([new DebugPiece("b", 0, "b__level0")], atRead.Pieces);

        // b writes nowhere and c, which read it, is dropped: the engine would refuse a branch whose rows go nowhere.
        var atEnd = Cutter().Cut(Chain, [], End("b"));

        Assert.Equal(["a", "b"], Parsed(atEnd).Keys);
        Assert.Equal("null:", Parsed(atEnd)["b"].Output);
        Assert.Equal(["c"], atEnd.Dropped);
    }

    [Fact]
    public void A_level_held_is_read_back_and_nothing_upstream_of_it_runs()
    {
        var key = "0123456789abcdef0123456789abcdef";

        var fromRead = Cutter().Cut(Chain, [], End("b"), [new DebugHeld("b", 0, key)]);

        var jobs = Parsed(fromRead);
        Assert.Equal(["b__level0", "b"], jobs.Keys);
        Assert.Equal(key, jobs["b__level0"].FromCheckpoint);
        Assert.Null(jobs["b__level0"].Input);
        Assert.Equal("b__level0", jobs["b"].From);
        Assert.Equal(["a", "c"], fromRead.Dropped.Order());
        Assert.Contains("b", fromRead.Unread);

        // The end of b held: c is the card, b is read back whole, with its writer side.
        var fromEnd = Cutter().Cut(Chain, [], End("c"), [new DebugHeld("b", 2, key)]);

        var ended = Parsed(fromEnd);
        Assert.Equal(["b", "c"], ended.Keys);
        Assert.Equal(key, ended["b"].FromCheckpoint);
        Assert.Null(ended["b"].Transformers);
        Assert.Equal("b", ended["c"].From);
        Assert.Equal(["a"], fromEnd.Dropped);
    }

    // ---- the run

    [Fact]
    public async Task A_run_to_a_card_keeps_the_watched_levels_and_the_next_run_starts_from_them()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        var (_, armed) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "read"), At("b", "end"), At("c", "end") } });
        Assert.Null(KeyOf(armed, "b", "read"));

        var (status, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("b", "end") });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(whole.GetProperty("report").GetProperty("success").GetBoolean(), whole.ToString());
        var held = new List<long> { await RowsOfAsync(KeyOf(whole, "b", "read")!), await RowsOfAsync(KeyOf(whole, "b", "end")!) };
        Assert.Equal([20L, 10L], held);
        // The stop is the card: c, which reads b, did not run.
        Assert.DoesNotContain("c", Ran(whole));

        var (nextStatus, next) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("c", "end") });

        Assert.Equal(HttpStatusCode.OK, nextStatus);
        Assert.True(next.GetProperty("report").GetProperty("success").GetBoolean(), next.ToString());
        // b is read back at its end: neither a, nor the read, nor the transformers ran again, and no key changed.
        Assert.DoesNotContain("a", Ran(next));
        Assert.DoesNotContain("b__level0", Ran(next));
        Assert.Contains("c", Ran(next));
        Assert.Equal(KeyOf(whole, "b", "end"), KeyOf(next, "b", "end"));
        Assert.Equal(KeyOf(whole, "b", "read"), KeyOf(next, "b", "read"));
        Assert.Equal(10, await RowsOfAsync(KeyOf(next, "c", "end")!));
    }

    [Fact]
    public async Task A_change_forgets_the_checkpoints_it_invalidates_from_the_session_and_keeps_the_others()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "read"), At("b", "end") } });
        var (_, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("b", "end") });
        var read = KeyOf(whole, "b", "read")!;
        var end = KeyOf(whole, "b", "end")!;

        // A transformer of b changed: its read is as it was, its end is not.
        var (_, edited) = await StateAsync(new { yaml = Chain.Replace("% 2 == 0", "% 3 == 0"), rows = 20, path = "job.yaml" });

        Assert.Equal(read, KeyOf(edited, "b", "read"));
        Assert.Null(KeyOf(edited, "b", "end"));
        Assert.True(Directory.Exists(KeyDirectory(read)));
        Assert.False(Directory.Exists(KeyDirectory(end)));

        // The rows a run reads are part of what produced a level.
        var (_, more) = await StateAsync(new { yaml = Chain, rows = 21, path = "job.yaml" });

        Assert.Null(KeyOf(more, "b", "read"));
        Assert.False(Directory.Exists(KeyDirectory(read)));
    }

    [Fact]
    public async Task A_reset_forgets_the_card_and_every_checkpoint_downstream_of_it()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "read"), At("b", "end"), At("c", "end") } });
        var (_, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("c", "end") });
        var keys = new[] { KeyOf(whole, "b", "read")!, KeyOf(whole, "b", "end")!, KeyOf(whole, "c", "end")! };
        Assert.All(keys, key => Assert.True(Directory.Exists(KeyDirectory(key))));

        var (_, reset) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", reset = new[] { At("b", "end") } });

        Assert.Equal(keys[0], KeyOf(reset, "b", "read"));
        Assert.Null(KeyOf(reset, "b", "end"));
        Assert.Null(KeyOf(reset, "c", "end"));
        Assert.True(Directory.Exists(KeyDirectory(keys[0])));
        Assert.False(Directory.Exists(KeyDirectory(keys[1])));
        Assert.False(Directory.Exists(KeyDirectory(keys[2])));

        var (_, all) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", resetAll = true });

        Assert.Null(KeyOf(all, "b", "read"));
        Assert.False(Directory.Exists(KeyDirectory(keys[0])));
        // The watchers stay armed: a reset forgets what was kept, not what the person watches.
        Assert.Equal(3, all.GetProperty("view").GetProperty("watch").GetArrayLength());
    }

    [Fact]
    public async Task Turning_a_watcher_off_forgets_its_checkpoint()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "read"), At("b", "end") } });
        var (_, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("b", "end") });
        var read = KeyOf(whole, "b", "read")!;

        var (_, off) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "end") } });

        Assert.Equal(1, off.GetProperty("view").GetProperty("watch").GetArrayLength());
        Assert.False(Directory.Exists(KeyDirectory(read)));
    }

    [Fact]
    public async Task A_file_written_with_a_breakpoint_still_loads_and_the_flag_is_not_answered()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        var file = Path.Combine(_dir, ".dtpipe", "studio", "job.yaml-debug.json");
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        File.WriteAllText(file, """{"session":"studio-debug","real":false,"watch":[{"branch":"b","at":"end","card":"b","break":true}],"levels":[]}""");

        var (status, view) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Equal(HttpStatusCode.OK, status);
        var watcher = WatcherOf(view, "b", "end");
        Assert.Equal("b", watcher.GetProperty("card").GetString());
        Assert.False(watcher.TryGetProperty("break", out _));
        Assert.DoesNotContain("break", File.ReadAllText(file));
    }

    [Fact]
    public async Task A_job_nobody_watches_leaves_no_state_file()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        var file = Path.Combine(_dir, ".dtpipe", "studio", "job.yaml-debug.json");

        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });
        Assert.False(File.Exists(file));

        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "end") } });
        Assert.True(File.Exists(file));

        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = Array.Empty<object>() });
        Assert.False(File.Exists(file));
    }

    [Fact]
    public async Task A_stop_nobody_watches_is_neither_watched_nor_kept()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);

        var (status, stopped) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("b", "end") });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(stopped.GetProperty("report").GetProperty("success").GetBoolean(), stopped.ToString());
        Assert.DoesNotContain("c", Ran(stopped));
        Assert.Equal(0, stopped.GetProperty("view").GetProperty("watch").GetArrayLength());
        var sessions = Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName);
        Assert.Empty(Directory.Exists(sessions) ? Directory.GetDirectories(sessions) : []);
        Assert.False(File.Exists(Path.Combine(_dir, ".dtpipe", "studio", "job.yaml-debug.json")));
    }

    // ---- where a card is read

    private static JsonElement PlaceOf(JsonElement answer, string branch, string at) => answer.GetProperty("view").GetProperty("places").EnumerateArray()
        .First(p => p.GetProperty("branch").GetString() == branch && p.GetProperty("at").GetString() == at);

    private static string? AliasOf(JsonElement answer, string branch, string at) =>
        PlaceOf(answer, branch, at).GetProperty("alias") is { ValueKind: JsonValueKind.String } alias ? alias.GetString() : null;

    [Fact]
    public async Task A_run_with_no_watcher_reads_every_card_in_its_own_branch()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);

        var (_, run) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.True(run.GetProperty("report").GetProperty("success").GetBoolean(), run.ToString());
        foreach (var branch in new[] { "a", "b", "c" })
            foreach (var at in new[] { "read", "end" })
                Assert.Equal(branch, AliasOf(run, branch, at));
        var stages = run.GetProperty("report").GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "b").GetProperty("stages");
        Assert.Equal(3, stages.GetArrayLength());
        Assert.Equal(20, stages[0].GetProperty("rowsSeen").GetInt32());
        Assert.Equal(10, stages[2].GetProperty("rowsSeen").GetInt32());
    }

    [Fact]
    public async Task A_cut_branch_is_read_in_its_pieces_and_a_branch_read_back_says_it_did_not_run()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "read"), At("b", "end") } });

        var (_, cut) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Equal("b__level0", AliasOf(cut, "b", "read"));
        Assert.Equal("b", AliasOf(cut, "b", "end"));
        Assert.Equal("a", AliasOf(cut, "a", "end"));

        // Only the end is held now: b is read back at it, so its read and the branch upstream did not run.
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "end") } });
        var (_, back) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Null(AliasOf(back, "a", "end"));
        Assert.Null(AliasOf(back, "b", "read"));
        Assert.Equal("b", AliasOf(back, "b", "end"));
        Assert.Equal("c", AliasOf(back, "c", "end"));
    }

    [Fact]
    public async Task The_plan_tells_the_digest_of_every_place_without_holding_the_engine_or_writing_a_file()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);

        var (status, plan) = await PostAsync("/api/debug/plan", new { yaml = Chain, rows = 20 });
        var (_, edited) = await PostAsync("/api/debug/plan", new { yaml = Chain.Replace("% 2 == 0", "% 3 == 0"), rows = 20 });

        Assert.Equal(HttpStatusCode.OK, status);
        string? Digest(JsonElement answer, string branch, string at) => PlaceOf(answer, branch, at).GetProperty("digest").GetString();
        Assert.All(new[] { "a", "b", "c" }, branch => Assert.False(string.IsNullOrEmpty(Digest(plan, branch, "end"))));
        Assert.Equal(Digest(plan, "a", "end"), Digest(edited, "a", "end"));
        // The edit is in b's transformers: its read is unchanged, its end and what reads it are not.
        Assert.Equal(Digest(plan, "b", "read"), Digest(edited, "b", "read"));
        Assert.NotEqual(Digest(plan, "b", "end"), Digest(edited, "b", "end"));
        Assert.NotEqual(Digest(plan, "c", "end"), Digest(edited, "c", "end"));
        Assert.Null(AliasOf(plan, "b", "end"));
        Assert.False(Directory.Exists(Path.Combine(_dir, ".dtpipe", "studio")));
    }

    [Fact]
    public async Task A_run_with_no_stop_goes_to_the_end_of_the_job_from_the_levels_it_holds()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { At("b", "end"), At("c", "end") } });
        var (_, atB) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", upTo = At("b", "end") });
        Assert.DoesNotContain("c", Ran(atB));

        var (status, end) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(end.GetProperty("report").GetProperty("success").GetBoolean(), end.ToString());
        Assert.DoesNotContain("a", Ran(end));
        Assert.Contains("c", Ran(end));
        Assert.Equal(KeyOf(atB, "b", "end"), KeyOf(end, "b", "end"));
        Assert.Equal(10, await RowsOfAsync(KeyOf(end, "c", "end")!));
    }

    [Fact]
    public async Task A_watcher_keeps_the_name_of_its_card_beside_the_job()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);

        var (_, set) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", watch = new[] { new { branch = "b", at = "read", card = "src" } } });
        var (_, back) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Equal("src", WatcherOf(set, "b", "read").GetProperty("card").GetString());
        Assert.Equal("src", WatcherOf(back, "b", "read").GetProperty("card").GetString());
    }

    [Fact]
    public async Task A_job_not_saved_yet_keeps_its_state_in_a_draft_that_the_first_save_hands_to_the_job()
    {
        await StateAsync(new { yaml = Chain, rows = 20, draft = Draft, watch = new[] { At("b", "end") } });
        var (status, whole) = await DebugAsync(new { yaml = Chain, rows = 20, draft = Draft, upTo = At("b", "end") });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(File.Exists(Path.Combine(_dir, ".dtpipe", "studio", "drafts", Draft + "-debug.json")));
        var end = KeyOf(whole, "b", "end")!;
        Assert.True(Directory.Exists(KeyDirectory(end)));

        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        var (_, adopted) = await StateAsync(new { yaml = Chain, rows = 20, path = "job.yaml", adopt = Draft });

        Assert.Equal(end, KeyOf(adopted, "b", "end"));
        Assert.False(File.Exists(Path.Combine(_dir, ".dtpipe", "studio", "drafts", Draft + "-debug.json")));
        Assert.True(File.Exists(Path.Combine(_dir, ".dtpipe", "studio", "job.yaml-debug.json")));

        var (neither, _) = await DebugAsync(new { yaml = Chain, rows = 20 });
        var (badDraft, _) = await DebugAsync(new { yaml = Chain, rows = 20, draft = "../x" });
        Assert.Equal(HttpStatusCode.BadRequest, neither);
        Assert.Equal(HttpStatusCode.BadRequest, badDraft);
    }

    [Fact]
    public async Task A_stop_a_branch_runs_whole_at_its_read_is_refused_by_name()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex > 1: ''\n  output: \"null:\"\n";

        var (status, body) = await DebugAsync(new { yaml = job, rows = 5, draft = Draft, upTo = At("b", "read") });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, status);
        Assert.Contains("runs whole", body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_branch_with_a_processor_watched_at_its_end_is_read_back_instead_of_run_again()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a WHERE GenerateIndex < 7\nc:\n  from: b\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: \"null:\"\n";
        await StateAsync(new { yaml = job, rows = 20, draft = Draft, watch = new[] { At("b", "end"), At("c", "end") } });
        var (_, first) = await DebugAsync(new { yaml = job, rows = 20, draft = Draft, upTo = At("b", "end") });
        Assert.True(first.GetProperty("report").GetProperty("success").GetBoolean(), first.ToString());
        Assert.Equal(7, await RowsOfAsync(KeyOf(first, "b", "end")!));

        var (_, next) = await DebugAsync(new { yaml = job, rows = 20, draft = Draft, upTo = At("c", "end") });

        Assert.True(next.GetProperty("report").GetProperty("success").GetBoolean(), next.ToString());
        Assert.DoesNotContain("a", Ran(next));
        Assert.Contains("c", Ran(next));
    }

    [Fact]
    public async Task An_expansion_upstream_arrives_whole_downstream_under_a_bound_on_the_source()
    {
        const string job = """
            a:
              input: generate:100
            s:
              from: a
              provider-options:
                sql:
                  query: SELECT a.* FROM a, range(3)
            c:
              from: s
              transformers:
              - type: compute
                mappings:
                  x:row.GenerateIndex: ''
              output: "null:"
            """;
        await StateAsync(new { yaml = job, rows = 5, draft = Draft, watch = new[] { At("c", "read") } });

        var (status, answer) = await DebugAsync(new { yaml = job, rows = 5, draft = Draft, upTo = At("c", "end") });

        Assert.Equal(HttpStatusCode.OK, status);
        var c0 = answer.GetProperty("report").GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "c__level0");
        Assert.Equal(15, c0.GetProperty("rowsDelivered").GetInt64());
    }

    [Fact]
    public async Task A_debug_run_reads_more_than_a_samples_thousand_rows_and_shows_a_thousand()
    {
        const string job = "a:\n  input: generate:5000\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: \"null:\"\n";

        var (status, answer) = await DebugAsync(new { yaml = job, rows = 3000, draft = Draft });

        Assert.Equal(HttpStatusCode.OK, status);
        var tail = answer.GetProperty("report").GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "a");
        Assert.Equal(3000, tail.GetProperty("rowsDelivered").GetInt64());
        Assert.True(tail.GetProperty("stages").EnumerateArray().All(s => s.GetProperty("sample").GetArrayLength() <= 1000));
    }

    // ---- the page

    [Fact]
    public void The_page_has_no_sample_button_and_no_debug_tab_of_its_own()
    {
        var assembly = typeof(StudioServer).Assembly;
        var page = string.Join('\n', assembly.GetManifestResourceNames().Where(n => n.StartsWith("studio/")).Select(name =>
        {
            using var reader = new StreamReader(assembly.GetManifestResourceStream(name)!);
            return reader.ReadToEnd();
        }));
        Assert.DoesNotContain("d-sample", page);
        Assert.DoesNotContain("d-debug", page);
        Assert.DoesNotContain("Resume here", page);
        Assert.DoesNotContain("name: \"debug\"", page);
    }
}
