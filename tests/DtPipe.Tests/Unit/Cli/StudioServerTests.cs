using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Studio;
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

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(Path.Combine(_dir, "sub"));
        Directory.CreateDirectory(Path.Combine(_dir, ".hidden"));
        File.WriteAllText(Path.Combine(_dir, "a.yaml"), "a: 1\n");
        File.WriteAllText(Path.Combine(_dir, "sub", "b.yml"), "b: 1\n");
        File.WriteAllText(Path.Combine(_dir, ".hidden", "c.yaml"), "c: 1\n");
        File.WriteAllText(Path.Combine(_dir, "notes.txt"), "not a job\n");
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, (y, r, ct) => _sampler(y, r, ct), CancellationToken.None);
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
}
