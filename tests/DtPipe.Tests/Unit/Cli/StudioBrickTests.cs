using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The coordinator's bricks in the studio: a branch the coordinator recognises opens as its brick, a
/// brick card still saves once signed out, and a job with bricks samples on this machine, a source
/// brick's rows coming from its node and a sink brick's target never reached.
/// </summary>
public sealed class StudioBrickTests : IAsyncLifetime
{
    private const string Job = """
        src:
          input: duck:${{NODE_ONLY_DB}}
          provider-options:
            duck:
              query: SELECT id FROM orders
        out:
          from: src
          output: csv:o.csv
        """;

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-bricks-").FullName;
    private readonly StudioFakes _fakes = new();
    private readonly ManualTime _time = new();
    private readonly HttpClient _browser = new(new HttpClientHandler { AllowAutoRedirect = false });
    private ServiceProvider _services = null!;
    private CoordinatorLink _link = null!;
    private StudioServer _studio = null!;
    private Func<string, int, StudioBindings, CancellationToken, Task<string>>? _sampler;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        _fakes.SinkTarget = "csv:" + Path.Combine(_dir, "target.csv");
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        _services = services.BuildServiceProvider();
        var real = StudioEngines.Real(_services);
        _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), _time);
        _studio = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            new SwitchableEngine(real, () => _sampler),
            new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link, lookup => new DesignService(lookup)),
            CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _studio.DisposeAsync();
        _link.Dispose();
        await _fakes.DisposeAsync();
        await _services.DisposeAsync();
        _browser.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<(HttpStatusCode Status, JsonElement Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_studio.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _studio.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _browser.SendAsync(request, Ct);
        var text = await response.Content.ReadAsStringAsync(Ct);
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    private async Task SignInAsync()
    {
        var (_, login) = await CallAsync(HttpMethod.Post, "/api/coordinator/login", new { });
        using var authorize = await _browser.GetAsync(login.GetProperty("url").GetString(), Ct);
        using var callback = await _browser.GetAsync(authorize.Headers.Location, Ct);
        Assert.Equal(HttpStatusCode.OK, callback.StatusCode);
        var (status, bricks) = await CallAsync(HttpMethod.Get, "/api/bricks");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(2, bricks.GetProperty("bricks").GetArrayLength());
    }

    private static string? BrickOf(JsonElement decomposed, string alias) => decomposed.GetProperty("model").GetProperty("steps").EnumerateArray()
        .First(s => s.GetProperty("alias").GetString() == alias).GetProperty("brick").GetString();

    [Fact]
    public async Task A_branch_the_coordinator_recognises_opens_as_its_brick_and_as_a_local_card_otherwise()
    {
        var (_, signedOut) = await CallAsync(HttpMethod.Post, "/api/design/decompose", new { yaml = Job });
        Assert.Null(BrickOf(signedOut, "src"));

        await SignInAsync();
        _fakes.Answers = new() { ["src"] = new Recognition(StudioFakes.SourceKey, StudioFakes.SourceKey, null) };
        var (_, signedIn) = await CallAsync(HttpMethod.Post, "/api/design/decompose", new { yaml = Job });

        Assert.Equal(StudioFakes.SourceKey, BrickOf(signedIn, "src"));
        Assert.Contains("SELECT id FROM orders", _fakes.Recognized);
    }

    [Fact]
    public async Task A_branch_of_this_machine_never_leaves_it_for_the_recognition()
    {
        const string witness = "witness-password-9Zq";
        var job = Job + $"\nmine:\n  input: \"pg:Host=localhost;Username=me;Password={witness};Database=d\"\n  output: csv:m.csv\n"
            + "plain:\n  input: csv:local.csv\n  output: csv:p.csv\n";
        await SignInAsync();
        _fakes.Answers = new() { ["src"] = new Recognition(StudioFakes.SourceKey, StudioFakes.SourceKey, null) };

        var (_, decomposed) = await CallAsync(HttpMethod.Post, "/api/design/decompose", new { yaml = job });

        Assert.DoesNotContain(witness, _fakes.Recognized);
        Assert.DoesNotContain("local.csv", _fakes.Recognized);
        Assert.Contains("NODE_ONLY_DB", _fakes.Recognized);
        Assert.Equal(StudioFakes.SourceKey, BrickOf(decomposed, "src"));
        Assert.Null(BrickOf(decomposed, "mine"));
    }

    [Fact]
    public async Task A_job_with_no_branch_naming_a_node_variable_asks_nothing()
    {
        await SignInAsync();
        _fakes.Recognized = null;

        await CallAsync(HttpMethod.Post, "/api/design/decompose", new { yaml = "plain:\n  input: csv:local.csv\n  output: csv:p.csv\n" });

        Assert.Null(_fakes.Recognized);
    }

    [Fact]
    public async Task A_job_with_a_brick_card_saves_once_its_person_is_signed_out()
    {
        await SignInAsync();
        _fakes.RefuseRefresh = true;
        _time.Now += TimeSpan.FromHours(1);
        var (_, bricks) = await CallAsync(HttpMethod.Get, "/api/bricks");
        Assert.Contains("sign in again", bricks.GetProperty("error").GetString());
        Assert.Equal(CoordinatorLink.SignedOut, _link.Status().State);

        var model = new
        {
            title = "Bricks",
            steps = new object[]
            {
                new { alias = "src", kind = StepKinds.Source, brick = StudioFakes.SourceKey },
                new { alias = "out", kind = StepKinds.Sink, brick = StudioFakes.SinkKey, from = new[] { "src" } },
            },
        };
        var (status, saved) = await CallAsync(HttpMethod.Put, "/api/job", new { path = "bricks.yaml", model });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Contains("SELECT id FROM orders", File.ReadAllText(Path.Combine(_dir, "bricks.yaml")));
        Assert.Contains("target.csv", saved.GetProperty("yaml").GetString());
    }

    private string BrickJob => $$$"""
        src:
          input: duck:${{NODE_ONLY_DB}}
          provider-options:
            duck:
              query: SELECT id FROM orders
        out:
          from: src
          output: {{{_fakes.SinkTarget}}}
        """;

    private static readonly object[] Bricks = [new { branch = "src", key = StudioFakes.SourceKey }, new { branch = "out", key = StudioFakes.SinkKey }];

    [Fact]
    public async Task A_source_brick_samples_the_rows_its_node_sends_and_a_sink_brick_writes_nothing()
    {
        await SignInAsync();

        var (status, report) = await CallAsync(HttpMethod.Post, "/api/sample", new { yaml = BrickJob, rows = 500, bricks = Bricks });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.True(report.GetProperty("success").GetBoolean(), report.ToString());
        var src = report.GetProperty("branches").EnumerateArray().First(b => b.GetProperty("branch").GetString() == "src");
        Assert.Equal(3, src.GetProperty("stages")[0].GetProperty("rowsSeen").GetInt32());
        Assert.Equal(CoordinatorApi.MaxSampleRows, _fakes.SampleRows);
        Assert.False(File.Exists(Path.Combine(_dir, "target.csv")));
    }

    [Fact]
    public async Task A_bricks_sample_file_is_its_owners_only_and_gone_once_the_sample_ends()
    {
        await SignInAsync();
        string? file = null;
        UnixFileMode? mode = null;
        _sampler = (_, _, bindings, _) =>
        {
            file = bindings.Input!.Split('=', 2)[1];
            if (!OperatingSystem.IsWindows()) mode = File.GetUnixFileMode(file);
            Assert.Equal(StudioFakes.Arrow(3), File.ReadAllBytes(file));
            Assert.StartsWith("out=", bindings.Output);
            return Task.FromResult("{}");
        };

        var (status, _) = await CallAsync(HttpMethod.Post, "/api/sample", new { yaml = BrickJob, rows = 3, bricks = Bricks });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.StartsWith(Path.Combine(_dir, ".dtpipe", "studio", "samples"), file);
        if (!OperatingSystem.IsWindows()) Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, mode);
        Assert.False(File.Exists(file));
        Assert.Empty(Directory.EnumerateFiles(Path.Combine(_dir, ".dtpipe", "studio", "samples")));
    }

    [Fact]
    public async Task A_bricks_variables_are_its_nodes_not_this_machines()
    {
        await SignInAsync();

        var (_, all) = await CallAsync(HttpMethod.Post, "/api/variables", new { yaml = BrickJob });
        var (_, mine) = await CallAsync(HttpMethod.Post, "/api/variables", new { yaml = BrickJob, bricks = Bricks });

        Assert.Contains("NODE_ONLY_DB", all.ToString());
        Assert.DoesNotContain("NODE_ONLY_DB", mine.ToString());
    }

    [Fact]
    public async Task A_brick_is_sampled_only_once_signed_in()
    {
        var (status, answer) = await CallAsync(HttpMethod.Post, "/api/sample", new { yaml = BrickJob, rows = 3, bricks = Bricks });
        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("sign in", answer.GetProperty("error").GetString());
    }

    /// <summary>Records the YAML of every branch the model asks about.</summary>
    private sealed class Spy : IBrickLookup
    {
        public readonly List<string> Asked = [];
        public DesignBrick? Find(string key) => null;
        public YamlMappingNode BranchOf(DesignBrick brick) => throw new InvalidOperationException();
        public DesignBrick? Match(YamlMappingNode branch) { Asked.Add(JobYaml.Serialize(branch)); return null; }
        public DesignBrick? Contained(YamlMappingNode branch, string kind) { Asked.Add(JobYaml.Serialize(branch)); return null; }
    }

    [Fact]
    public void Recognition_is_asked_of_each_top_level_branch_as_written()
    {
        // The studio finds the coordinator's answers again by a branch's YAML: the model must ask about
        // the job's own top-level branches, never a piece of one.
        const string job = """
            a:
              input: duck:x.db
              transformers:
                - fake: { id: "random.number" }
              output: csv:o.csv
            b:
              input: csv:in.csv
            """;
        var spy = new Spy();
        new DesignService(spy, new ComponentEndpointRules(
            _services.GetServices<DtPipe.Core.Abstractions.IStreamReaderFactory>(), _services.GetServices<DtPipe.Core.Abstractions.IDataWriterFactory>())).Decompose(job);

        var branches = JobYaml.Branches(JobYaml.Parse(job)).Select(b => JobYaml.Serialize(b.Branch)).ToHashSet();
        Assert.NotEmpty(spy.Asked);
        Assert.All(spy.Asked, asked => Assert.Contains(asked, branches));
        Assert.Equal(3, spy.Asked.Count(a => a == JobYaml.Serialize(JobYaml.Branches(JobYaml.Parse(job)).First().Branch)));
    }
}
