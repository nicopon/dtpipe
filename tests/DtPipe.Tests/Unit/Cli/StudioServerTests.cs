using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The local server of <c>dtpipe studio</c>, on a free port of 127.0.0.1: who it answers, what it
/// lets a page read, and the sample it runs through the same path as the MCP <c>dry-run</c> tool.
/// </summary>
public sealed class StudioServerTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "studio-" + Guid.NewGuid().ToString("N"));
    private readonly HttpClient _http = new();
    private StudioServer _server = null!;
    private StudioSampler _sampler = (_, _, _) => Task.FromResult("{}");
    private readonly InMemorySecretsManager _secrets = new();

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(Path.Combine(_dir, "sub"));
        Directory.CreateDirectory(Path.Combine(_dir, ".hidden"));
        File.WriteAllText(Path.Combine(_dir, "a.yaml"), "a: 1\n");
        File.WriteAllText(Path.Combine(_dir, "sub", "b.yml"), "b: 1\n");
        File.WriteAllText(Path.Combine(_dir, ".hidden", "c.yaml"), "c: 1\n");
        File.WriteAllText(Path.Combine(_dir, "notes.txt"), "not a job\n");
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            (y, r, ct) => _sampler(y, r, ct), new DesignService(IBrickLookup.None),
            [new StudioComponent("csv", "Files")], [new StudioComponent("duck", "Databases")], _secrets), CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _server.DisposeAsync();
        _http.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private HttpRequestMessage Request(HttpMethod method, string path, string? token = "", object? body = null)
    {
        var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_server.Port}{path}");
        if (token is not null) request.Headers.Add(StudioServer.TokenHeader, token.Length == 0 ? _server.Token : token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        return request;
    }

    private async Task<(HttpStatusCode Status, JsonElement Body)> SendAsync(HttpRequestMessage request)
    {
        using var response = await _http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    [Fact]
    public void The_address_carries_the_token_in_its_fragment_only()
    {
        Assert.Equal($"http://127.0.0.1:{_server.Port}/#{_server.Token}", _server.Url);
        Assert.True(_server.Token.Length >= 40);
    }

    [Theory]
    [InlineData("evil.example")]
    [InlineData("127.0.0.1.nip.io")]
    public async Task A_host_name_other_than_the_loopback_is_refused(string host)
    {
        var request = Request(HttpMethod.Get, "/api/jobs");
        request.Headers.Host = $"{host}:{_server.Port}";

        var (status, _) = await SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, status);
    }

    [Fact]
    public async Task A_request_from_another_origin_is_refused_even_with_the_token()
    {
        var request = Request(HttpMethod.Get, "/api/jobs");
        request.Headers.Add("Origin", "http://evil.example");

        var (status, _) = await SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, status);
    }

    [Fact]
    public async Task A_request_the_browser_marks_cross_site_is_refused()
    {
        var request = Request(HttpMethod.Get, "/api/jobs");
        request.Headers.Add("Sec-Fetch-Site", "cross-site");

        Assert.Equal(HttpStatusCode.Forbidden, (await SendAsync(request)).Status);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("not-the-token")]
    public async Task The_api_needs_the_sessions_token(string? token)
    {
        var (status, body) = await SendAsync(Request(HttpMethod.Get, "/api/jobs", token));

        Assert.Equal(HttpStatusCode.Unauthorized, status);
        Assert.Contains("token", body.GetProperty("error").GetString());
    }

    [Fact]
    public async Task Every_answer_forbids_inline_script_and_referrers()
    {
        using var response = await _http.SendAsync(Request(HttpMethod.Get, "/api/jobs"));

        Assert.Contains("script-src 'self'", response.Headers.GetValues("Content-Security-Policy").Single());
        Assert.Equal("no-referrer", response.Headers.GetValues("Referrer-Policy").Single());
        Assert.False(response.Headers.Contains("Access-Control-Allow-Origin"));
    }

    [Fact]
    public async Task The_jobs_are_the_yaml_files_of_the_folder_hidden_ones_excepted()
    {
        var (status, body) = await SendAsync(Request(HttpMethod.Get, "/api/jobs"));

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(["a.yaml", "sub/b.yml"], body.GetProperty("jobs").EnumerateArray().Select(j => j.GetString()));
    }

    [Fact]
    public async Task A_job_of_the_folder_is_read()
    {
        var (status, body) = await SendAsync(Request(HttpMethod.Get, "/api/job?path=sub/b.yml"));

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("b: 1\n", body.GetProperty("yaml").GetString());
    }

    [Theory]
    [InlineData("../a.yaml")]
    [InlineData("sub/../../x.yaml")]
    [InlineData(".hidden/c.yaml")]
    [InlineData("/etc/hosts.yaml")]
    [InlineData("notes.txt")]
    [InlineData("")]
    public async Task A_path_that_leaves_the_folder_or_is_no_job_is_refused(string path)
    {
        var (status, _) = await SendAsync(Request(HttpMethod.Get, "/api/job?path=" + Uri.EscapeDataString(path)));

        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task A_job_reached_through_a_link_is_refused()
    {
        var outside = Path.Combine(Path.GetTempPath(), "studio-outside-" + Guid.NewGuid().ToString("N") + ".yaml");
        File.WriteAllText(outside, "secret: 1\n");
        try
        {
            try { File.CreateSymbolicLink(Path.Combine(_dir, "link.yaml"), outside); }
            catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
            {
                Assert.Skip("This system does not let a test create a symbolic link.");
            }

            var (status, _) = await SendAsync(Request(HttpMethod.Get, "/api/job?path=link.yaml"));
            var (_, list) = await SendAsync(Request(HttpMethod.Get, "/api/jobs"));

            Assert.Equal(HttpStatusCode.BadRequest, status);
            Assert.DoesNotContain("link.yaml", list.GetProperty("jobs").EnumerateArray().Select(j => j.GetString()));
        }
        finally
        {
            File.Delete(outside);
        }
    }

    [Fact]
    public async Task One_sample_runs_at_a_time()
    {
        var release = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _sampler = (_, _, _) => { started.TrySetResult(); return release.Task; };

        var first = SendAsync(Request(HttpMethod.Post, "/api/sample", body: new { yaml = "a: 1", rows = 5 }));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var (second, _) = await SendAsync(Request(HttpMethod.Post, "/api/sample", body: new { yaml = "a: 1", rows = 5 }));
        release.SetResult("{\"success\": true}");

        Assert.Equal(HttpStatusCode.Conflict, second);
        Assert.Equal(HttpStatusCode.OK, (await first).Status);
    }

    [Fact]
    public async Task The_rows_asked_are_bounded()
    {
        var asked = 0;
        _sampler = (_, rows, _) => { asked = rows; return Task.FromResult("{}"); };

        await SendAsync(Request(HttpMethod.Post, "/api/sample", body: new { yaml = "a: 1", rows = 1_000_000 }));

        Assert.Equal(StudioServer.MaxRows, asked);
    }

    [Theory]
    [InlineData("/", "text/html")]
    [InlineData("/studio.js", "text/javascript")]
    [InlineData("/studio.css", "text/css")]
    [InlineData("/dom.js", "text/javascript")]
    [InlineData("/designer.js", "text/javascript")]
    [InlineData("/coordinator.js", "text/javascript")]
    public async Task The_page_is_served_from_the_binary_without_a_token(string path, string type)
    {
        using var response = await _http.SendAsync(Request(HttpMethod.Get, path, token: null));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.StartsWith(type, response.Content.Headers.ContentType!.ToString());
    }

    /// <summary>The policy forbids inline script and style: a page that used either would break, or tempt a looser policy.</summary>
    [Fact]
    public async Task The_page_holds_no_inline_script_nor_style()
    {
        var html = await _http.GetStringAsync($"http://127.0.0.1:{_server.Port}/");

        Assert.Matches("<script[^>]* src=\"/studio.js\"", html);
        Assert.DoesNotMatch("<script(?![^>]* src=)", html);
        Assert.DoesNotContain("<style", html);
        Assert.DoesNotContain("style=", html);
        Assert.DoesNotMatch(" on[a-z]+=", html);
    }

    [Theory]
    [InlineData("GET", "/index.html")]
    [InlineData("POST", "/")]
    [InlineData("GET", "/api/nothing")]
    public async Task Anything_else_is_not_found(string method, string path)
    {
        var (status, _) = await SendAsync(Request(new HttpMethod(method), path));

        Assert.Equal(HttpStatusCode.NotFound, status);
    }

    /// <summary>
    /// Every value the page shows comes from a job or a source: set as HTML, a sampled value would
    /// run as script with the person's access. The page builds nodes and sets text, nothing else.
    /// </summary>
    [Fact]
    public void No_script_of_the_page_writes_html_or_an_inline_style()
    {
        var assembly = typeof(StudioServer).Assembly;
        var scripts = assembly.GetManifestResourceNames().Where(n => n.StartsWith("studio/") && n.EndsWith(".js")).ToList();
        Assert.Contains("studio/designer.js", scripts);
        foreach (var name in scripts)
        {
            using var reader = new StreamReader(assembly.GetManifestResourceStream(name)!);
            var code = reader.ReadToEnd();
            foreach (var sink in new[] { "innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "style=", "\"style\"", "eval(", "new Function" })
                Assert.False(code.Contains(sink, StringComparison.Ordinal), $"{name} uses {sink}");
        }
    }

    [Fact]
    public async Task The_palette_lists_the_components()
    {
        var (status, body) = await SendAsync(Request(HttpMethod.Get, "/api/components"));

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("csv", body.GetProperty("readers")[0].GetProperty("name").GetString());
        Assert.Equal("duck", body.GetProperty("writers")[0].GetProperty("name").GetString());
    }

    [Fact]
    public async Task Cards_are_composed_by_the_server_and_a_job_taken_apart_by_it()
    {
        var cards = new
        {
            title = "Copy",
            steps = new object[]
            {
                new { alias = "a", kind = "branch", yaml = "input: csv:a.csv\n" },
                new { alias = "b", kind = "branch", from = new[] { "a" }, yaml = "from: a\noutput: csv:b.csv\n" },
            },
        };

        var (status, composed) = await SendAsync(Request(HttpMethod.Post, "/api/design/compose", body: cards));

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(0, composed.GetProperty("errors").GetArrayLength());
        var yaml = composed.GetProperty("yaml").GetString()!;
        Assert.StartsWith("# Copy\na:\n", yaml);

        var (again, decomposed) = await SendAsync(Request(HttpMethod.Post, "/api/design/decompose", body: new { yaml }));

        Assert.Equal(HttpStatusCode.OK, again);
        Assert.Equal(["a", "b"], decomposed.GetProperty("model").GetProperty("steps").EnumerateArray().Select(s => s.GetProperty("alias").GetString()));
    }

    [Fact]
    public async Task A_body_the_route_cannot_read_is_refused()
    {
        var request = Request(HttpMethod.Post, "/api/design/compose");
        request.Content = new StringContent("{\"steps\": 3}", Encoding.UTF8, "application/json");

        var (status, body) = await SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("/api/design/compose", body.GetProperty("error").GetString());
    }

    // ── Saving ───────────────────────────────────────────────────────────────

    private const string Copy = "# Copy\nmain:\n  input: csv:a.csv\n  output: csv:b.csv\n";

    private static object Cards(string output, object? layout = null) => new
    {
        title = "Copy",
        steps = new object[] { new { alias = "main", kind = "branch", yaml = $"input: csv:a.csv\noutput: {output}\n" } },
        layout,
    };

    private async Task<JsonElement> OpenAsync(string path)
    {
        var (status, body) = await SendAsync(Request(HttpMethod.Get, $"/api/job?path={Uri.EscapeDataString(path)}"));
        Assert.Equal(HttpStatusCode.OK, status);
        return body;
    }

    private Task<(HttpStatusCode Status, JsonElement Body)> SaveAsync(string path, string? hash, object model, bool acceptErrors = false) =>
        SendAsync(Request(HttpMethod.Put, "/api/job", body: new { path, hash, model, acceptErrors }));

    [Fact]
    public async Task A_save_writes_the_job_the_server_composes_and_keeps_the_layout_outside_git()
    {
        File.WriteAllText(Path.Combine(_dir, "copy.yaml"), Copy);
        var opened = await OpenAsync("copy.yaml");
        Assert.False(opened.GetProperty("rewrites").GetBoolean());

        var (status, saved) = await SaveAsync("copy.yaml", opened.GetProperty("hash").GetString(),
            Cards("csv:c.csv", new { main = new { x = 40, y = 60 } }));

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("# Copy\nmain:\n  input: csv:a.csv\n  output: csv:c.csv\n", File.ReadAllText(Path.Combine(_dir, "copy.yaml")));
        Assert.Equal("*\n", File.ReadAllText(Path.Combine(_dir, ".dtpipe", ".gitignore")));
        Assert.True(File.Exists(Path.Combine(_dir, ".dtpipe", "studio", "copy.yaml-layout.json")));
        var again = await OpenAsync("copy.yaml");
        Assert.Equal(saved.GetProperty("hash").GetString(), again.GetProperty("hash").GetString());
        Assert.Equal(40, again.GetProperty("layout").GetProperty("main").GetProperty("x").GetInt32());
        Assert.DoesNotContain("copy.yaml-layout.json", (await SendAsync(Request(HttpMethod.Get, "/api/jobs"))).Body.ToString());
    }

    [Fact]
    public async Task A_save_over_a_file_changed_since_it_was_opened_is_refused()
    {
        var path = Path.Combine(_dir, "copy.yaml");
        File.WriteAllText(path, Copy);
        var hash = (await OpenAsync("copy.yaml")).GetProperty("hash").GetString();
        File.WriteAllText(path, Copy + "# edited elsewhere\n");

        var (status, body) = await SaveAsync("copy.yaml", hash, Cards("csv:c.csv"));

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("changed on disk", body.GetProperty("error").GetString());
        Assert.EndsWith("# edited elsewhere\n", File.ReadAllText(path));
    }

    [Fact]
    public async Task A_new_job_is_created_once_and_never_over_an_existing_file()
    {
        var (created, _) = await SaveAsync("sub/new.yaml", null, Cards("csv:c.csv"));
        var (again, body) = await SaveAsync("sub/new.yaml", null, Cards("csv:d.csv"));

        Assert.Equal(HttpStatusCode.OK, created);
        Assert.Equal(HttpStatusCode.Conflict, again);
        Assert.Contains("already exists", body.GetProperty("error").GetString());
        Assert.Contains("csv:c.csv", File.ReadAllText(Path.Combine(_dir, "sub", "new.yaml")));
    }

    [Theory]
    [InlineData("nowhere/new.yaml", HttpStatusCode.NotFound)]
    [InlineData(".hidden/new.yaml", HttpStatusCode.BadRequest)]
    [InlineData("../new.yaml", HttpStatusCode.BadRequest)]
    [InlineData("new.txt", HttpStatusCode.BadRequest)]
    [InlineData("sub", HttpStatusCode.BadRequest)]
    public async Task A_job_is_written_only_where_it_could_be_read(string path, HttpStatusCode expected)
    {
        var (status, _) = await SaveAsync(path, null, Cards("csv:c.csv"));

        Assert.Equal(expected, status);
        Assert.False(File.Exists(Path.Combine(_dir, "nowhere", "new.yaml")));
    }

    [Fact]
    public async Task Nothing_is_written_through_a_link()
    {
        var outside = Path.Combine(Path.GetTempPath(), "studio-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outside);
        try
        {
            Directory.CreateSymbolicLink(Path.Combine(_dir, "linked"), outside);
            var (throughFolder, _) = await SaveAsync("linked/x.yaml", null, Cards("csv:c.csv"));
            Assert.Equal(HttpStatusCode.BadRequest, throughFolder);
            Assert.Empty(Directory.EnumerateFileSystemEntries(outside));

            // A clone can carry a .dtpipe that points anywhere: the layout is not kept through it,
            // and the job is not written either.
            Directory.CreateSymbolicLink(Path.Combine(_dir, ".dtpipe"), outside);
            var (throughStore, body) = await SaveAsync("fresh.yaml", null, Cards("csv:c.csv", new { main = new { x = 1, y = 1 } }));
            Assert.Equal(HttpStatusCode.BadRequest, throughStore);
            Assert.Contains("link", body.GetProperty("error").GetString());
            Assert.False(File.Exists(Path.Combine(_dir, "fresh.yaml")));
            Assert.Empty(Directory.EnumerateFileSystemEntries(outside));
        }
        finally
        {
            Directory.Delete(outside, recursive: true);
        }
    }

    [Fact]
    public async Task Cards_that_do_not_compose_are_written_only_when_the_person_accepts_it()
    {
        var broken = new { title = "Broken", steps = new object[] { new { alias = "lonely", kind = "branch", yaml = "input: csv:a.csv\n" } } };

        var (refused, body) = await SaveAsync("broken.yaml", null, broken);
        Assert.Equal(HttpStatusCode.UnprocessableEntity, refused);
        Assert.Contains("leads nowhere", body.GetProperty("error").GetString());
        Assert.False(File.Exists(Path.Combine(_dir, "broken.yaml")));

        var (accepted, _) = await SaveAsync("broken.yaml", null, broken, acceptErrors: true);
        Assert.Equal(HttpStatusCode.OK, accepted);
        Assert.True(File.Exists(Path.Combine(_dir, "broken.yaml")));
    }

    [Fact]
    public async Task Opening_a_job_says_when_saving_would_rewrite_its_text()
    {
        File.WriteAllText(Path.Combine(_dir, "hand.yaml"), "# Hand\nmain:\n  # read the file\n  input: csv:a.csv\n  output: csv:b.csv\n");

        Assert.True((await OpenAsync("hand.yaml")).GetProperty("rewrites").GetBoolean());
    }

    // ── Variables ────────────────────────────────────────────────────────────

    private const string Sentinel = "s3ntinel-never-shown";

    [Fact]
    public async Task Each_variable_is_shown_by_name_with_whether_something_resolves_it_never_a_value()
    {
        _secrets.SetSecret("known", Sentinel);
        Environment.SetEnvironmentVariable("STUDIO_T_SET", Sentinel);
        try
        {
            const string yaml = "main:\n  input: \"csv:${{STUDIO_T_UNSET}}\"\n  output: \"pg:Password=${{ keyring://known }};${{keyring://ghost}};${{STUDIO_T_SET}}\"\n"
                + "  cursor: id\n  state: \"${{cursor://state.json|0}}\"\n";

            var (status, body) = await SendAsync(Request(HttpMethod.Post, "/api/variables", body: new { path = "a.yaml", yaml }));

            Assert.Equal(HttpStatusCode.OK, status);
            var shown = body.GetProperty("variables").EnumerateArray().Select(v =>
                $"{v.GetProperty("kind").GetString()}:{v.GetProperty("name").GetString()}:{v.GetProperty("defined").GetBoolean()}:{v.GetProperty("ready").GetBoolean()}");
            Assert.Equal(["env:STUDIO_T_UNSET:False:False", "keyring:known:True:True", "keyring:ghost:False:False", "env:STUDIO_T_SET:True:True",
                "cursor:state.json|0:True:True"], shown);
            Assert.Equal(["known"], body.GetProperty("aliases").EnumerateArray().Select(a => a.GetString()));
            Assert.DoesNotContain(Sentinel, body.ToString());
        }
        finally
        {
            Environment.SetEnvironmentVariable("STUDIO_T_SET", null);
        }
    }

    [Fact]
    public async Task A_sample_is_refused_before_it_runs_naming_what_nothing_resolves()
    {
        var ran = false;
        _sampler = (_, _, _) => { ran = true; return Task.FromResult("{}"); };

        var (status, body) = await SendAsync(Request(HttpMethod.Post, "/api/sample",
            body: new { yaml = "main:\n  input: \"csv:${{STUDIO_T_MISSING}}\"\n  output: \"csv:${{keyring://ghost}}\"\n", rows = 3 }));

        Assert.Equal(HttpStatusCode.UnprocessableEntity, status);
        Assert.Contains("'STUDIO_T_MISSING'", body.GetProperty("error").GetString());
        Assert.Contains("'ghost'", body.GetProperty("error").GetString());
        Assert.False(ran);
    }

    [Theory]
    [InlineData("keyring")]
    [InlineData("value")]
    public async Task A_launch_value_reaches_the_run_and_leaves_with_it(string how)
    {
        _secrets.SetSecret("pass", Sentinel);
        string? seen = null;
        _sampler = (_, _, _) => { seen = Environment.GetEnvironmentVariable("STUDIO_T_PASS"); return Task.FromResult("{\"success\": true}"); };
        object chosen = how == "keyring" ? new { keyring = "pass" } : new { value = "typed by hand" };

        var (launched, _) = await SendAsync(Request(HttpMethod.Put, "/api/launch",
            body: new { path = "a.yaml", overrides = new Dictionary<string, object> { ["STUDIO_T_PASS"] = chosen } }));
        var (sampled, _) = await SendAsync(Request(HttpMethod.Post, "/api/sample",
            body: new { yaml = "main:\n  input: \"csv:${{STUDIO_T_PASS}}\"\n", rows = 3, path = "a.yaml" }));

        Assert.Equal(HttpStatusCode.NoContent, launched);
        Assert.Equal(HttpStatusCode.OK, sampled);
        Assert.Equal(how == "keyring" ? Sentinel : "typed by hand", seen);
        Assert.Null(Environment.GetEnvironmentVariable("STUDIO_T_PASS"));
        var file = File.ReadAllText(Path.Combine(_dir, ".dtpipe", "studio", "a.yaml-launch.json"));
        Assert.DoesNotContain(Sentinel, file);
        Assert.Contains(how == "keyring" ? "\"pass\"" : "typed by hand", file);
    }

    [Fact]
    public async Task A_launch_value_is_an_alias_or_a_text_never_both()
    {
        var (status, _) = await SendAsync(Request(HttpMethod.Put, "/api/launch",
            body: new { path = "a.yaml", overrides = new Dictionary<string, object> { ["X"] = new { keyring = "a", value = "b" } } }));

        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task Quitting_from_the_page_ends_the_session()
    {
        var (status, _) = await SendAsync(Request(HttpMethod.Post, "/api/quit"));

        Assert.Equal(HttpStatusCode.Accepted, status);
        await _server.Quit.WaitAsync(TimeSpan.FromSeconds(5));
    }
}

/// <summary>
/// The studio's sample is the MCP sample, through the real engine: what each stage saw, and no option
/// of one job carried into the next.
/// </summary>
[Collection("console-serial")]
public sealed class StudioSampleTests
{
    private static DtPipeMcpTools Tools()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        return services.BuildServiceProvider().GetRequiredService<DtPipeMcpTools>();
    }

    private static JsonElement Branch(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("branches").EnumerateArray().Single();

    [Fact]
    public async Task Each_stage_shows_every_row_asked_for()
    {
        var json = await Tools().SampleAsync("main:\n  input: \"generate:30\"\n  output: \"null:\"\n", rows: 25, shownRows: 25, CancellationToken.None);

        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.GetProperty("success").GetBoolean(), json);
        var reader = Branch(json).GetProperty("stages").EnumerateArray().First();
        Assert.Equal(25, reader.GetProperty("sample").GetArrayLength());
    }

    [Fact]
    public async Task Two_samples_in_a_row_share_nothing()
    {
        var tools = Tools();
        const string withCompute =
            "main:\n  input: \"generate:5\"\n  output: \"null:\"\n  transformers:\n    - type: compute\n      mappings:\n        doubled: \"row.GenerateIndex * 2\"\n";
        const string plain = "main:\n  input: \"generate:3\"\n  output: \"null:\"\n";

        var first = await tools.SampleAsync(withCompute, rows: 5, shownRows: 5, CancellationToken.None);
        var second = await tools.SampleAsync(plain, rows: 3, shownRows: 3, CancellationToken.None);

        Assert.Contains("doubled", first);
        Assert.True(JsonDocument.Parse(second).RootElement.GetProperty("success").GetBoolean(), second);
        Assert.DoesNotContain("doubled", second);
        var stages = Branch(second).GetProperty("stages").EnumerateArray().ToList();
        Assert.Single(stages);
        Assert.Equal(3, stages[0].GetProperty("rowsSeen").GetInt64());
        Assert.Equal(3, Branch(second).GetProperty("rowsDelivered").GetInt64());
    }

    [Fact]
    public async Task A_hand_written_job_opens_as_cards_of_dtpipe_s_components_and_samples_as_written()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        var provider = services.BuildServiceProvider();
        var design = new DesignService(IBrickLookup.None, new ComponentEndpointRules(
            provider.GetServices<IStreamReaderFactory>(), provider.GetServices<IDataWriterFactory>()));
        const string job = "main:\n  input: \"generate:4\"\n  limit: 3\n  transformers:\n  - type: compute\n    mappings:\n"
            + "      doubled: \"row.GenerateIndex * 2\"\n  output: \"null:\"\n";

        var cards = design.Decompose(job);

        Assert.Empty(cards.Errors);
        Assert.Equal(["main_source:source", "main:transform", "main_sink:sink"], cards.Model.Steps.Select(c => $"{c.Alias}:{c.Kind}"));
        Assert.Single(JobYaml.Branches(JobYaml.Parse(cards.Yaml)));
        var json = await provider.GetRequiredService<DtPipeMcpTools>().SampleAsync(cards.Yaml, rows: 10, shownRows: 10, CancellationToken.None);
        Assert.True(JsonDocument.Parse(json).RootElement.GetProperty("success").GetBoolean(), json);
        Assert.Contains("doubled", json);
        Assert.Equal(3, Branch(json).GetProperty("rowsDelivered").GetInt64());
    }

    /// <summary>
    /// A keyring value launched into a job reaches the run, never the page: not in the sample's
    /// answer, even when the connection it opens fails and reports why.
    /// </summary>
    [Fact]
    public async Task A_secret_launched_into_a_failing_connection_never_reaches_the_page()
    {
        const string sentinel = "Zq7-sentinel-Xw9";
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        var secrets = new InMemorySecretsManager();
        secrets.SetSecret("pgpass", sentinel);
        services.AddSingleton<ISecretsManager>(secrets);
        var provider = services.BuildServiceProvider();
        var mcp = provider.GetRequiredService<DtPipeMcpTools>();
        var dir = Path.Combine(Path.GetTempPath(), "studio-leak-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        await using var server = await StudioServer.StartAsync(new StudioFolder(dir), 0, new StudioTools(
            (y, r, ct) => mcp.SampleAsync(y, r, shownRows: r, ct), new DesignService(IBrickLookup.None), [], [], secrets), CancellationToken.None);
        using var http = new HttpClient();
        async Task<string> Send(HttpMethod method, string path, object body)
        {
            var request = new HttpRequestMessage(method, $"http://127.0.0.1:{server.Port}{path}")
            {
                Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"),
            };
            request.Headers.Add(StudioServer.TokenHeader, server.Token);
            using var response = await http.SendAsync(request);
            return await response.Content.ReadAsStringAsync();
        }
        try
        {
            await Send(HttpMethod.Put, "/api/launch", new { path = "job.yaml", overrides = new { STUDIO_T_DB_PASS = new { keyring = "pgpass" } } });
            foreach (var password in new[] { "${{STUDIO_T_DB_PASS}}", "${{keyring://pgpass}}" })
            {
                var yaml = $"main:\n  input: \"pg:Host=127.0.0.1;Port=1;Username=u;Password={password};Timeout=2\"\n"
                    + "  provider-options:\n    pg:\n      query: \"SELECT 1\"\n  output: \"null:\"\n";

                var answer = await Send(HttpMethod.Post, "/api/sample", new { yaml, rows = 1, path = "job.yaml" });
                var variables = await Send(HttpMethod.Post, "/api/variables", new { yaml, path = "job.yaml" });

                Assert.Contains("\"success\": false", answer);
                Assert.DoesNotContain(sentinel, answer);
                Assert.DoesNotContain(sentinel, variables);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
