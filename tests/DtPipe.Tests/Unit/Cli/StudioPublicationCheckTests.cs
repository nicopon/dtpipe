using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// What a publication would refuse, said by the studio through the runner's own dry run: the real binary under
/// <c>--edges-only</c>, each source brick bound to the rows the coordinator sends for it and each sink brick to a
/// throwaway file. Every refusal a publication is known to make reaches the studio's verdict, and a job the runner
/// takes is said to pass, so a refusal cannot hold for the wrong reason (a binding that never worked).
/// </summary>
public sealed class StudioPublicationCheckTests : IAsyncLifetime
{
    private readonly string _dir = Directory.CreateTempSubdirectory("studio-publication-check-").FullName;
    private readonly StudioFakes _fakes = new();
    private readonly ManualTime _time = new();
    private readonly HttpClient _browser = new(new HttpClientHandler { AllowAutoRedirect = false });
    private ServiceProvider _services = null!;
    private CoordinatorLink _link = null!;
    private StudioServer _studio = null!;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        _services = services.BuildServiceProvider();
        _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), _time);
        _studio = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            StudioEngines.Real(_services), new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link,
            lookup => new DesignService(lookup)), CancellationToken.None);
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
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Get, "/api/bricks")).Status);
    }

    // A source brick, one step, a sink brick: the shape a published pipeline has.
    private static string Job(string source = "brick:n1/orders", string query = "SELECT * FROM src", string sink = "brick:n1/archive") => $"""
        src:
          input: {source}
        q:
          from: src
          provider-options:
            sql:
              query: {query}
        out:
          from: q
          output: {sink}
        """;

    public static TheoryData<string, string, string?> Cases => new()
    {
        { "a job the runner takes", Job(), null },
        { "an unknown provider", Job(sink: "nope:x"), "'out' writes 'nope:x'" },
        { "an unknown brick", Job(source: "brick:n1/nope"), "n1/nope" },
        { "a query on a table that is not its input's alias", Job(query: "SELECT * FROM input"), "Table with name input does not exist" },
        { "a variable the query reads", Job(query: "SELECT * FROM src LIMIT ${{LIMIT}}"), "${{LIMIT}} is refused" },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Every_refusal_a_publication_makes_is_the_studios_publication_verdict(string why, string yaml, string? said)
    {
        await SignInAsync();

        var (status, verdict) = await CallAsync(HttpMethod.Post, "/api/publish/check", new { yaml });

        Assert.True(status == HttpStatusCode.OK, $"{why}: {status} {verdict}");
        var report = verdict.GetProperty("report").GetString();
        Assert.True(verdict.GetProperty("ok").GetBoolean() == (said is null), $"{why}: {report}");
        if (said is not null) Assert.Contains(said, report);
    }

    [Fact]
    public void The_child_is_this_dtpipe_started_through_the_host_that_runs_it()
    {
        // The test host is neither a single-file binary nor the apphost: the child is the dotnet host and this assembly's file.
        var (program, prefix) = PublicationCheck.Self();

        var dll = Assert.Single(prefix);
        Assert.Equal(typeof(PublicationCheck).Assembly.GetName().Name + ".dll", Path.GetFileName(dll));
        Assert.True(File.Exists(dll));
        Assert.Equal("dotnet", Path.GetFileNameWithoutExtension(program));
    }

    [Fact]
    public async Task Signed_out_there_is_no_publication_verdict()
    {
        var (status, body) = await CallAsync(HttpMethod.Post, "/api/publish/check", new { yaml = Job() });

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("Sign in", body.GetProperty("error").GetString());
    }
}
