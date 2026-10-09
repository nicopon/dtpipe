using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Split;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Jobs;
using DtPipe.Sessions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The debug run: the job cut after each transformer, each level kept in the studio's session, and a later
/// run that starts from a level. The cut is a copy for one sample; the job is never changed.
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

    private async Task<(HttpStatusCode Status, JsonElement Body)> DebugAsync(object body)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"http://127.0.0.1:{_server.Port}/api/debug")
        {
            Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        using var response = await _http.SendAsync(request, Ct);
        return (response.StatusCode, JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement.Clone());
    }

    private static string? KeyOf(JsonElement answer, string branch, int level) => answer.GetProperty("levels").EnumerateArray()
        .First(l => l.GetProperty("branch").GetString() == branch && l.GetProperty("level").GetInt32() == level).GetProperty("key").GetString();

    private async Task<long> RowsOfAsync(string key)
    {
        var store = new CheckpointStore(SessionStore.Resolve(DebugCutter.SessionName, _dir));
        long rows = 0;
        await foreach (var batch in store.ReadAsync(key)) using (batch) rows += batch.Length;
        return rows;
    }

    // ---- the cut

    [Fact]
    public void A_branch_is_cut_after_each_transformer_and_the_last_piece_keeps_its_alias_and_its_output()
    {
        const string job = "a:\n  input: generate:20\n  limit: 8\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex % 2 == 0: ''\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: \"null:\"\n  prefix: p\nz:\n  from: a\n  output: \"null:\"\n";

        var cut = Cutter().Cut(job);

        Assert.Equal(["a", "a", "a"], cut.Levels.Select(l => l.Branch));
        Assert.Equal(["a__level0", "a__level1", "a"], cut.Levels.Select(l => l.Alias));
        var jobs = DtPipe.Configuration.JobFileParser.ParseContent(cut.Yaml, null, interpolate: false);
        Assert.Equal(["a__level0", "a__level1", "a", "z"], jobs.Keys);
        // The head keeps the reader and what bounds it; the tail keeps the writer's side and the alias z reads.
        Assert.Equal(("generate:20", 8, null), (jobs["a__level0"].Input, jobs["a__level0"].Limit, jobs["a__level0"].Output));
        Assert.Equal(("null:", "p", "a__level1"), (jobs["a"].Output, jobs["a"].Prefix, jobs["a"].From));
        Assert.Equal(0, jobs["a"].Limit);
        Assert.Equal("a", jobs["z"].From);
        Assert.All(new[] { "a__level0", "a__level1", "a" }, alias => Assert.Equal(DebugCutter.SessionName, jobs[alias].Session));
        Assert.All(new[] { "a__level0", "a__level1", "a" }, alias => Assert.False(string.IsNullOrEmpty(jobs[alias].Checkpoint)));
    }

    [Fact]
    public void A_branch_with_a_processor_or_no_transformer_stays_whole_and_the_job_is_not_changed()
    {
        const string job = "a:\n  input: generate:20\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT * FROM a\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex > 1: ''\n  output: \"null:\"\n";

        var first = Cutter().Cut(job);
        var jobs = DtPipe.Configuration.JobFileParser.ParseContent(first.Yaml, null, interpolate: false);

        Assert.Empty(first.Levels);
        Assert.Equal(["a", "b"], jobs.Keys);
        Assert.Equal(first.Yaml, Cutter().Cut(job).Yaml);
        Assert.Contains("generate:20", job);
    }

    [Fact]
    public void An_alias_that_holds_the_reserved_mark_is_refused_by_name()
    {
        var error = Assert.Throws<InvalidOperationException>(() => Cutter().Cut("a__level0:\n  input: generate:3\n  output: \"null:\"\n"));
        Assert.Contains("a__level0", error.Message);
    }

    [Fact]
    public void A_resume_drops_the_branch_nothing_reads_any_more_and_names_it_for_the_bindings()
    {
        const string job = "src:\n  input: \"arrow:-\"\na:\n  from: src\n  transformers:\n  - type: filter\n    mappings:\n      row.GenerateIndex > 1: ''\n  output: \"null:\"\n";

        var cut = Cutter().Cut(job, new DebugResume("a", 0, "0123456789abcdef0123456789abcdef"));

        Assert.Contains("src", cut.Dropped);
        Assert.DoesNotContain("src:", cut.Yaml);
        Assert.Contains("from-checkpoint", cut.Yaml);
    }

    // ---- the run

    [Fact]
    public async Task A_debug_run_keeps_every_level_and_a_resume_gives_the_rows_of_the_whole_run()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);

        var (status, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(whole.GetProperty("report").GetProperty("success").GetBoolean(), whole.ToString());
        var held = new List<long>();
        foreach (var level in new[] { 0, 1, 2 }) held.Add(await RowsOfAsync(KeyOf(whole, "b", level)!));
        Assert.Equal([20L, 10L, 10L], held);

        var (resumedStatus, resumed) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", resume = new { branch = "b", level = 1 } });

        Assert.Equal(HttpStatusCode.OK, resumedStatus);
        Assert.True(resumed.GetProperty("report").GetProperty("success").GetBoolean(), resumed.ToString());
        // Level 1 was read back, not recomputed: its key is the one kept, and the downstream rows are the whole run's.
        Assert.Equal(KeyOf(whole, "b", 1), KeyOf(resumed, "b", 1));
        Assert.Equal(await RowsOfAsync(KeyOf(whole, "b", 2)!), await RowsOfAsync(KeyOf(resumed, "b", 2)!));
        var ran = resumed.GetProperty("report").GetProperty("branches").EnumerateArray().Select(b => b.GetProperty("branch").GetString()).ToList();
        Assert.DoesNotContain("a", ran);
        Assert.DoesNotContain("b__level0", ran);
    }

    [Fact]
    public async Task A_resume_is_refused_when_what_produced_the_level_changed_or_its_checkpoint_is_gone()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Chain);
        var (_, whole) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml" });
        var key = KeyOf(whole, "b", 1)!;

        var edited = Chain.Replace("% 2 == 0", "% 3 == 0");
        var (stale, staleBody) = await DebugAsync(new { yaml = edited, rows = 20, path = "job.yaml", resume = new { branch = "b", level = 1 } });
        Assert.Equal(HttpStatusCode.Conflict, stale);
        Assert.Contains("changed", staleBody.GetProperty("error").GetString());

        var upstream = Chain.Replace("generate:20", "generate:30");
        var (moved, _) = await DebugAsync(new { yaml = upstream, rows = 20, path = "job.yaml", resume = new { branch = "b", level = 1 } });
        Assert.Equal(HttpStatusCode.Conflict, moved);

        Directory.Delete(Path.Combine(_dir, ".dtpipe", "sessions", DebugCutter.SessionName, key), recursive: true);
        var (gone, goneBody) = await DebugAsync(new { yaml = Chain, rows = 20, path = "job.yaml", resume = new { branch = "b", level = 1 } });
        Assert.Equal(HttpStatusCode.Conflict, gone);
        Assert.Contains("expired", goneBody.GetProperty("error").GetString());

        var (never, _) = await DebugAsync(new { yaml = Chain, rows = 20, path = "other.yaml", resume = new { branch = "b", level = 1 } });
        Assert.Equal(HttpStatusCode.Conflict, never);
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

        var (status, answer) = await DebugAsync(new { yaml = job, rows = 5 });

        Assert.Equal(HttpStatusCode.OK, status);
        var c0 = answer.GetProperty("report").GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "c__level0");
        Assert.Equal(15, c0.GetProperty("rowsDelivered").GetInt64());
    }

    [Fact]
    public async Task A_debug_run_reads_more_than_a_samples_thousand_rows_and_shows_a_thousand()
    {
        const string job = "a:\n  input: generate:5000\n  transformers:\n  - type: compute\n    mappings:\n      x:row.GenerateIndex: ''\n  output: \"null:\"\n";

        var (status, answer) = await DebugAsync(new { yaml = job, rows = 3000 });

        Assert.Equal(HttpStatusCode.OK, status);
        var tail = answer.GetProperty("report").GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "a");
        Assert.Equal(3000, tail.GetProperty("rowsDelivered").GetInt64());
        Assert.True(tail.GetProperty("stages").EnumerateArray().All(s => s.GetProperty("sample").GetArrayLength() <= 1000));
    }
}
