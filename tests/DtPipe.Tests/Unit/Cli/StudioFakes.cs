using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;
using DtPipe.Cli;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Split;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.DryRun;

namespace DtPipe.Tests.Unit.Cli;

internal sealed class ManualTime : TimeProvider
{
    public DateTimeOffset Now { get; set; } = new(2026, 10, 8, 12, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => Now;
}

/// <summary>
/// A provider that signs "ann" in at once, and a coordinator that accepts the provider's latest access
/// token and offers one source brick and one sink brick, built on the records the real coordinator writes.
/// </summary>
internal sealed class StudioFakes : IAsyncDisposable
{
    public string IdpUrl = "", CoordinatorUrl = "";
    public int Contract = CoordinatorApi.Contract;
    public string? IssuerAnswered;
    public int ExpiresIn = 600;
    public bool RefuseRefresh, RotateRefresh = true;
    public int Refreshes, MeCalls;
    public string? ValidAccess, LastCsrf;
    public readonly List<string> IdpPaths = [];
    public Dictionary<string, Recognition> Answers = [];
    public string? Recognized;
    public int SampleRows, PreviewCalls;
    public readonly Dictionary<string, (string Yaml, string Commit)> Library = [];
    public string? LastSaveBody;
    public LibraryRefused? Refusal;
    public bool Deployed;
    public int Commits;

    public const string SourceKey = "n1/orders", SinkKey = "n1/archive";

    public BrickCatalogView Catalog => new(
        [new CatalogNode("n1", true, "data", "1.0.0+node")],
        [
            new CatalogBrick(SourceKey, "n1", "orders", StepKinds.Source, "Orders", "", new JsonObject { ["input"] = BrickRef.Format("n1", "orders") },
                "aaaaaaaaaaaa", null, null, true),
            new CatalogBrick(SinkKey, "n1", "archive", StepKinds.Sink, "Archive", "", new JsonObject { ["output"] = BrickRef.Format("n1", "archive") },
                "bbbbbbbbbbbb", null, null, true),
        ]);

    /// <summary>Rows <c>id</c> 1..n, as an Arrow IPC stream written by Apache.Arrow.</summary>
    public static byte[] Arrow(int rows)
    {
        var ids = new Int64Array.Builder();
        for (var i = 1; i <= rows; i++) ids.Append(i);
        var schema = new Schema.Builder().Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false)).Build();
        using var batch = new RecordBatch(schema, [ids.Build()], rows);
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }
        return stream.ToArray();
    }
    private string? _challenge, _redirect, _refresh;
    private int _issued;
    private WebApplication? _idp, _coordinator;

    public async Task StartAsync()
    {
        (_idp, IdpUrl) = await ServeAsync(IdpAsync);
        (_coordinator, CoordinatorUrl) = await ServeAsync(CoordinatorAsync);
    }

    private static async Task<(WebApplication, string)> ServeAsync(RequestDelegate handler)
    {
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions());
        builder.WebHost.UseKestrelCore().ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0));
        var app = builder.Build();
        app.Run(handler);
        await app.StartAsync();
        return (app, app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First());
    }

    private async Task IdpAsync(HttpContext context)
    {
        var path = context.Request.Path.Value!;
        IdpPaths.Add(path);
        switch (path)
        {
            case "/.well-known/openid-configuration":
                await context.Response.WriteAsJsonAsync(new Dictionary<string, string>
                {
                    ["issuer"] = IssuerAnswered ?? IdpUrl + "/",
                    ["authorization_endpoint"] = IdpUrl + "/authorize",
                    ["token_endpoint"] = IdpUrl + "/token",
                });
                break;
            case "/authorize":
                var query = context.Request.Query;
                _challenge = query["code_challenge"];
                _redirect = query["redirect_uri"];
                var ok = query["client_id"] == "studio" && query["code_challenge_method"] == "S256" && query["scope"].ToString().Contains("offline_access");
                context.Response.Redirect($"{_redirect}?code={(ok ? "code-1" : "refused")}&state={Uri.EscapeDataString(query["state"]!)}&iss={Uri.EscapeDataString(IdpUrl + "/")}");
                break;
            case "/token":
                var form = await context.Request.ReadFormAsync();
                var granted = form["grant_type"].ToString() switch
                {
                    "authorization_code" => form["code"] == "code-1" && form["redirect_uri"] == _redirect && form["client_id"] == "studio"
                        && Convert.ToBase64String(SHA256.HashData(Encoding.ASCII.GetBytes(form["code_verifier"]!))).TrimEnd('=').Replace('+', '-').Replace('/', '_') == _challenge,
                    "refresh_token" => !RefuseRefresh && form["refresh_token"] == _refresh && ++Refreshes > 0,
                    _ => false,
                };
                if (!granted)
                {
                    context.Response.StatusCode = 400;
                    await context.Response.WriteAsJsonAsync(new { error = "invalid_grant" });
                    return;
                }
                var access = $"access-{++_issued}";
                var refresh = _refresh is null || RotateRefresh ? $"refresh-{_issued}" : null;
                ValidAccess = access;
                _refresh = refresh ?? _refresh;
                await context.Response.WriteAsJsonAsync(new Dictionary<string, object?>
                {
                    ["access_token"] = access, ["refresh_token"] = refresh, ["expires_in"] = ExpiresIn, ["token_type"] = "Bearer",
                });
                break;
            default:
                context.Response.StatusCode = 404;
                break;
        }
    }

    private async Task CoordinatorAsync(HttpContext context)
    {
        switch (context.Request.Path.Value)
        {
            case CoordinatorApi.Metadata:
                await context.Response.WriteAsJsonAsync(new ResourceMetadata(CoordinatorUrl, [IdpUrl], ["header"], Contract, "studio"));
                break;
            case "/api/me":
                MeCalls++;
                LastCsrf = context.Request.Headers[CoordinatorApi.CsrfHeader];
                if (context.Request.Headers.Authorization != $"Bearer {ValidAccess}")
                {
                    context.Response.StatusCode = 401;
                    return;
                }
                await context.Response.WriteAsJsonAsync(new CoordinatorMe("ann", "ann@x.test", ["designer", "reader"]), JsonSerializerOptions.Web);
                break;
            case "/api/catalog":
            await context.Response.WriteAsJsonAsync(Catalog, JsonSerializerOptions.Web);
            break;
        case "/api/bricks/recognize":
            Recognized = (await context.Request.ReadFromJsonAsync<RecognizeRequest>(JsonSerializerOptions.Web))!.Job;
            await context.Response.WriteAsJsonAsync(new RecognizeResponse(Answers), JsonSerializerOptions.Web);
            break;
        case "/api/bricks/n1/orders/preview":
            PreviewCalls++;
            await context.Response.WriteAsJsonAsync(new BrickPreviewView("id\n1\n", [new CatalogColumn("id", "BIGINT", false)]), JsonSerializerOptions.Web);
            break;
        case "/api/bricks/n1/orders/sample":
            SampleRows = int.Parse(context.Request.Query["rows"]!);
            context.Response.ContentType = "application/vnd.apache.arrow.stream";
            await context.Response.Body.WriteAsync(Arrow(Math.Min(SampleRows, 3)));
            break;
        case CoordinatorApi.Prefix + CoordinatorApi.LibraryListRoute:
            await context.Response.WriteAsJsonAsync(
                Library.Select(p => new LibraryItem(p.Key, p.Key, null, p.Value.Commit)).ToArray(), JsonSerializerOptions.Web);
            break;
        case { } library when library.StartsWith("/api/library/", StringComparison.Ordinal) && context.Request.Method == "GET":
            var wanted = library["/api/library/".Length..];
            if (!Library.TryGetValue(wanted, out var held))
            {
                context.Response.StatusCode = 404;
                return;
            }
            await context.Response.WriteAsJsonAsync(new LibraryJob(wanted, held.Yaml, held.Commit), JsonSerializerOptions.Web);
            break;
        case { } library when library.StartsWith("/api/library/", StringComparison.Ordinal):
            var id = library["/api/library/".Length..];
            LastSaveBody = await new StreamReader(context.Request.Body).ReadToEndAsync();
            var save = JsonSerializer.Deserialize<LibrarySave>(LastSaveBody, JsonSerializerOptions.Web)!;
            if (Refusal is not null)
            {
                context.Response.StatusCode = 422;
                await context.Response.WriteAsJsonAsync(Refusal, JsonSerializerOptions.Web);
                return;
            }
            var exists = Library.TryGetValue(id, out var current);
            var ifMatch = context.Request.Headers.IfMatch.ToString();
            if (context.Request.Headers.IfNoneMatch == "*" && exists || ifMatch.Length > 0 && (!exists || ifMatch.Trim('"') != current.Commit))
            {
                context.Response.StatusCode = 412;
                await context.Response.WriteAsJsonAsync(new LibraryConflict("not the version expected", exists ? current.Commit : null), JsonSerializerOptions.Web);
                return;
            }
            Library[id] = (save.Yaml, $"commit-{Library.Count + Commits++}");
            await context.Response.WriteAsJsonAsync(new LibrarySaved(id, Library[id].Commit, Deployed), JsonSerializerOptions.Web);
            break;
        case "/api/elsewhere":
                context.Response.Redirect(IdpUrl + "/stolen");
                break;
            default:
                context.Response.StatusCode = 404;
                break;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_idp is not null) await _idp.DisposeAsync();
        if (_coordinator is not null) await _coordinator.DisposeAsync();
    }
}

/// <summary>
/// A studio engine that answers what a test sets. <see cref="Run"/> reports through <c>failures</c> and its
/// exception comes back as the outcome's fault, as <see cref="JobRunner"/> returns it.
/// </summary>
internal sealed class FakeStudioEngine : IStudioEngine
{
    public Func<string, int, StudioBindings, CancellationToken, Task<string>> Sample { get; init; } = (_, _, _, _) => Task.FromResult("{}");

    public Func<string, StudioBindings, ConcurrentDictionary<string, string>, CancellationToken, Task<int>> Run { get; init; } =
        (_, _, _, _) => Task.FromResult(0);

    public DebugCutter Cutter { get; } = new([], [], []);

    public Task<string> SampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) => Sample(yaml, rows, bindings, ct);

    public Task<string> DebugSampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) => Sample(yaml, rows, bindings, ct);

    public async Task<RunOutcome> RunAsync(string yaml, StudioBindings bindings, CancellationToken ct)
    {
        var failures = new ConcurrentDictionary<string, string>();
        var code = 1;
        Exception? fault = null;
        try { code = await Run(yaml, bindings, failures, ct); }
        catch (Exception ex) { fault = ex; }
        return new RunOutcome(code, fault, failures, new Dictionary<string, SampleReport>(), new Dictionary<string, string>());
    }
}

/// <summary>An engine whose sample a test may replace while the rest stays the real one.</summary>
internal sealed class SwitchableEngine(IStudioEngine inner, Func<Func<string, int, StudioBindings, CancellationToken, Task<string>>?> sample) : IStudioEngine
{
    public DebugCutter Cutter => inner.Cutter;

    public Task<string> SampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        sample() is { } replaced ? replaced(yaml, rows, bindings, ct) : inner.SampleAsync(yaml, rows, bindings, ct);

    public Task<string> DebugSampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        inner.DebugSampleAsync(yaml, rows, bindings, ct);

    public Task<RunOutcome> RunAsync(string yaml, StudioBindings bindings, CancellationToken ct) => inner.RunAsync(yaml, bindings, ct);
}

internal static class StudioEngines
{
    /// <summary>The engine <c>dtpipe studio</c> builds, over the services of a test.</summary>
    public static StudioEngine Real(IServiceProvider services) => new(
        services.GetRequiredService<JobAnalysis>(), services.GetRequiredService<JobRunner>(),
        new DebugCutter(services.GetServices<IStreamReaderFactory>().ToList(), services.GetServices<IDataWriterFactory>().ToList(),
            services.GetServices<IStreamTransformerFactory>().ToList()));
}
