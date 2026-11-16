using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Stores;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Two coordinators of the product on one database (<see cref="SqlTestBed"/>), each a standby (<c>Coordinator:Standby=passive</c>) on
/// Kestrel at a port of its own, sharing a library remote: the first holds the database, the second waits for it. The second's
/// bootstrap names an identity the first's does not, so the rights tell whether it wrote.
/// </summary>
public sealed class PassiveCoordinatorTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "passive-" + Guid.NewGuid().ToString("N"));
    private readonly string _remote;
    private readonly string _runnerKey;
    private SqlTestBed? _bed;

    public PassiveCoordinatorTests()
    {
        _remote = Path.Combine(_dir, "remote.git");
        Directory.CreateDirectory(_remote);
        Git(_remote, "init", "-q", "--bare", "-b", "main");
        _runnerKey = Path.Combine(_dir, "runner.key");
        File.WriteAllText(_runnerKey, ClearKey.New("runner").Reveal());
        File.WriteAllText(Path.Combine(_dir, "admin.key"), ClearKey.New("boss").Reveal());
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose()
    {
        _bed?.Dispose();
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [Theory]
    [InlineData("postgresql")]
    [InlineData("sqlserver")]
    public async Task A_Second_Coordinator_On_The_Database_Is_Passive_Answers_Its_Probes_Alone_And_Writes_Nothing(string provider)
    {
        _bed = new SqlTestBed(provider, _dir);
        await using var active = await StartedAsync(Build("active", runner: false, out var activePort, out _));
        var passive = Build("passive", runner: true, out var port, out var log);
        var starting = Task.Run(passive.MapCoordinatorServerAsync, Ct);
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };
        await AnswersAsync(http);

        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("/livez", Ct)).StatusCode);
        using var ready = await http.GetAsync("/healthz", Ct);
        Assert.Equal((HttpStatusCode.ServiceUnavailable, PassiveResponder.State), (ready.StatusCode, await ready.Content.ReadAsStringAsync(Ct)));
        using var negotiate = await http.PostAsync("/node-channel/negotiate?negotiateVersion=1", null, Ct);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, negotiate.StatusCode);
        Assert.Equal(TimeSpan.FromSeconds(PassiveResponder.RetryAfterSeconds), negotiate.Headers.RetryAfter?.Delta);
        using var token = await ExchangeAsync(http);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, token.StatusCode);
        Assert.False(starting.IsCompleted);
        // The standby's bootstrap ran nowhere: it read and wrote nothing before holding the database.
        Assert.DoesNotContain("runner", (string)_bed.Read("SELECT doc FROM rights")!);
        // The responder listens before the wait for the database logs its warning: the log is read once it holds it.
        var logging = Stopwatch.StartNew();
        while (!log.Entries.Any(e => e.Contains("passive")) && logging.Elapsed < TimeSpan.FromSeconds(20)) await Task.Delay(50, Ct);
        var logged = string.Join('\n', log.Entries);
        Assert.Contains("passive", logged);
        Assert.DoesNotContain(PasswordOf(provider), logged);

        // Stopped while it waits, it takes nothing, and the active one serves on.
        passive.Lifetime.StopApplication();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => starting.WaitAsync(TimeSpan.FromSeconds(20), Ct));
        await passive.DisposeAsync();
        await Assert.ThrowsAsync<HttpRequestException>(() => http.GetAsync("/livez", Ct));
        using var activeHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{activePort}") };
        Assert.Equal(HttpStatusCode.OK, (await activeHttp.GetAsync("/healthz", Ct)).StatusCode);
    }

    // The active one leaves as a killed process does, as far as the database sees: its session ends. A node's reconnection is stood
    // for by what a node asks first: a token, then the node channel's negotiation.
    [Theory]
    [InlineData("postgresql")]
    [InlineData("sqlserver")]
    public async Task A_Passive_Coordinator_Takes_The_Database_When_The_Active_One_Leaves_And_Serves_A_Node(string provider)
    {
        _bed = new SqlTestBed(provider, _dir);
        var active = await StartedAsync(Build("active", runner: false, out _, out _));
        await using var passive = Build("passive", runner: true, out var port, out var log);
        var starting = Task.Run(passive.MapCoordinatorServerAsync, Ct);
        using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}") };
        await AnswersAsync(http);

        await active.StopAsync(Ct);
        await active.DisposeAsync();
        await starting.WaitAsync(TimeSpan.FromSeconds(60), Ct);
        await passive.StartAsync(Ct);

        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("/healthz", Ct)).StatusCode);
        Assert.Contains(log.Entries, e => e.Contains("leaves the passive state"));
        using var exchanged = await ExchangeAsync(http);
        Assert.Equal(HttpStatusCode.OK, exchanged.StatusCode);
        var bearer = (await exchanged.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("access_token").GetString();
        using var negotiate = new HttpRequestMessage(HttpMethod.Post, "/node-channel/negotiate?negotiateVersion=1");
        negotiate.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        Assert.Equal(HttpStatusCode.OK, (await http.SendAsync(negotiate, Ct)).StatusCode);
    }

    // A coordinator of the product on a free port, its state in a directory of its own and in the bed's database. With runner, its
    // bootstrap names the identity runner beside the administrator.
    private WebApplication Build(string name, bool runner, out int port, out CapturedLog log)
    {
        port = FreePort();
        var url = $"http://127.0.0.1:{port}";
        var settings = new Dictionary<string, string?>
        {
            ["urls"] = url,
            ["Coordinator:StateDirectory"] = Path.Combine(_dir, name),
            ["Coordinator:Initialize"] = "true",
            ["Coordinator:Store"] = _bed!.Dialect() is PostgreSqlDialect ? "postgresql" : "sqlserver",
            ["Coordinator:ConnectionString"] = _bed.ConnectionString,
            ["Coordinator:Standby"] = "passive",
            ["Coordinator:Library:Remote:Url"] = _remote,
            ["Coordinator:PublicUrl"] = url,
            ["Coordinator:Auth:AllowHttpKeys"] = "true",
            ["Coordinator:Bootstrap:AdminKey"] = "@" + Path.Combine(_dir, "admin.key"),
        };
        if (runner)
        {
            settings["Coordinator:Bootstrap:Identities:0:Id"] = "runner";
            settings["Coordinator:Bootstrap:Identities:0:Groups:0"] = "runners";
            settings["Coordinator:Bootstrap:Identities:0:Key"] = "@" + _runnerKey;
        }
        var builder = WebApplication.CreateBuilder();
        builder.Configuration.AddInMemoryCollection(settings);
        log = new CapturedLog();
        builder.Logging.AddProvider(log);
        builder.AddCoordinatorServer();
        return builder.Build();
    }

    private static async Task<WebApplication> StartedAsync(WebApplication app)
    {
        await app.MapCoordinatorServerAsync();
        await app.StartAsync(Ct);
        return app;
    }

    private async Task<HttpResponseMessage> ExchangeAsync(HttpClient http) =>
        await http.PostAsync(KeyIssuer.TokenPath, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = "runner",
            ["client_secret"] = (await File.ReadAllTextAsync(_runnerKey, Ct)).Trim(),
        }), Ct);

    // Until the responder listens.
    private static async Task AnswersAsync(HttpClient http)
    {
        var waited = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                using var _ = await http.GetAsync("/livez", Ct);
                return;
            }
            catch (HttpRequestException) when (waited.Elapsed < TimeSpan.FromSeconds(20))
            {
                await Task.Delay(100, Ct);
            }
        }
    }

    private string PasswordOf(string provider) => provider == "postgresql"
        ? new Npgsql.NpgsqlConnectionStringBuilder(_bed!.ConnectionString).Password!
        : new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(_bed!.ConnectionString).Password;

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static void Git(string dir, params string[] args)
    {
        var psi = new ProcessStartInfo("git") { WorkingDirectory = dir, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in args) psi.ArgumentList.Add(arg);
        using var process = Process.Start(psi)!;
        process.WaitForExit();
        Assert.Equal(0, process.ExitCode);
    }
}
