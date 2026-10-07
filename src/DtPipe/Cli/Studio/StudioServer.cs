using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Studio;

/// <summary>Samples a job: its YAML, the source rows to read, and the call's cancellation. Returns the sample as JSON.</summary>
public delegate Task<string> StudioSampler(string yaml, int rows, CancellationToken ct);

/// <summary>A reader or a writer the designer's palette offers, by its component name.</summary>
public sealed record StudioComponent(string Name, string Category);

/// <summary>
/// What the studio's server works with: the sample path, the designer's model (the coordinator's
/// own, with dtpipe's components for endpoints) and the components the palette lists.
/// </summary>
public sealed record StudioTools(
    StudioSampler Sample, DesignService Design, IReadOnlyList<StudioComponent> Readers, IReadOnlyList<StudioComponent> Writers);

/// <summary>
/// The local server behind <c>dtpipe studio</c>. It runs dtpipe with the person's own access, so it
/// is guarded like a shell:
/// <list type="bullet">
/// <item>it listens on 127.0.0.1 only, and answers only a <c>Host</c> naming that address or
/// <c>localhost</c> at its port, so a page served from another name that resolves here (DNS
/// rebinding) is refused;</item>
/// <item>every <c>/api</c> call carries the session's token in <see cref="TokenHeader"/>. The token
/// reaches the page in the URL's fragment, which the browser never sends; no cookie is set, since
/// cookies are shared by every port of a host;</item>
/// <item>a request with another <c>Origin</c>, or that the browser marks cross-site, is refused, and
/// no CORS header is ever sent;</item>
/// <item>the pages run no inline script (<c>Content-Security-Policy</c>) and send no referrer.</item>
/// </list>
/// One sample runs at a time: the sample mode's collector belongs to the whole process.
/// </summary>
public sealed class StudioServer : IAsyncDisposable
{
    public const string TokenHeader = "X-Dtpipe-Studio";
    public const int MaxRows = 1000;

    private const string Policy =
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

    private static readonly Dictionary<string, (string Resource, string Type)> Assets = new(StringComparer.Ordinal)
    {
        ["/"] = ("studio/index.html", "text/html; charset=utf-8"),
        ["/studio.js"] = ("studio/studio.js", "text/javascript; charset=utf-8"),
        ["/dom.js"] = ("studio/dom.js", "text/javascript; charset=utf-8"),
        ["/designer.js"] = ("studio/designer.js", "text/javascript; charset=utf-8"),
        ["/studio.css"] = ("studio/studio.css", "text/css; charset=utf-8"),
    };

    private static readonly JsonSerializerOptions Json = JsonSerializerOptions.Web;

    private readonly WebApplication _app;
    private readonly StudioFolder _folder;
    private readonly StudioTools _tools;
    private readonly StudioLocalFiles _local;
    private readonly byte[] _token;
    private readonly SemaphoreSlim _sampling = new(1, 1);
    private readonly TaskCompletionSource _quit = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private StudioServer(WebApplication app, StudioFolder folder, StudioTools tools, string token)
    {
        _app = app;
        _folder = folder;
        _tools = tools;
        _local = new StudioLocalFiles(folder);
        Token = token;
        _token = Encoding.UTF8.GetBytes(token);
    }

    public string Token { get; }
    public int Port { get; private set; }

    /// <summary>The address to open: the token rides in the fragment, never sent to any server.</summary>
    public string Url => $"http://127.0.0.1:{Port}/#{Token}";

    /// <summary>Completes when the page asks to quit.</summary>
    public Task Quit => _quit.Task;

    /// <param name="port">0 lets the system choose a free port.</param>
    public static async Task<StudioServer> StartAsync(StudioFolder folder, int port, StudioTools tools, CancellationToken ct)
    {
        // The empty builder reads no configuration file, no environment and logs nothing: the folder
        // is the person's, and an appsettings.json in it must not configure this server.
        var builder = WebApplication.CreateEmptyBuilder(new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
        builder.WebHost.UseKestrelCore().ConfigureKestrel(o =>
        {
            o.Listen(IPAddress.Loopback, port);
            o.AddServerHeader = false;
            o.Limits.MaxRequestBodySize = 2_000_000;
        });
        var app = builder.Build();
        var server = new StudioServer(app, folder, tools, Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
            .TrimEnd('=').Replace('+', '-').Replace('/', '_'));
        app.Run(server.HandleAsync);
        await app.StartAsync(ct);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        server.Port = new Uri(address).Port;
        return server;
    }

    private async Task HandleAsync(HttpContext context)
    {
        var request = context.Request;
        var headers = context.Response.Headers;
        headers["Content-Security-Policy"] = Policy;
        headers["Referrer-Policy"] = "no-referrer";
        headers["X-Content-Type-Options"] = "nosniff";
        headers["Cache-Control"] = "no-store";

        try
        {
            Admit(request);
            var path = request.Path.Value ?? "/";
            if (!path.StartsWith("/api/", StringComparison.Ordinal))
            {
                await ServeAssetAsync(context, path);
                return;
            }
            if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(request.Headers[TokenHeader].ToString()), _token))
                throw new StudioRefusal(401, "This session's token is missing or wrong: open the address dtpipe studio printed.");

            switch (request.Method, path)
            {
                case ("GET", "/api/jobs"):
                    await JsonAsync(context, new { folder = Path.GetFileName(_folder.Root), jobs = _folder.Jobs() });
                    break;
                case ("GET", "/api/job"):
                    await OpenAsync(context, request.Query["path"].ToString());
                    break;
                case ("PUT", "/api/job"):
                    await SaveAsync(context);
                    break;
                case ("GET", "/api/components"):
                    await JsonAsync(context, new { readers = _tools.Readers, writers = _tools.Writers });
                    break;
                case ("POST", "/api/design/compose"):
                    await JsonAsync(context, _tools.Design.Compose(await BodyAsync<DesignModel>(context)));
                    break;
                case ("POST", "/api/design/decompose"):
                    var decompose = await BodyAsync<DecomposeRequest>(context);
                    await JsonAsync(context, _tools.Design.Decompose(decompose.Yaml ?? "", decompose.Layout));
                    break;
                case ("POST", "/api/sample"):
                    await SampleAsync(context);
                    break;
                case ("POST", "/api/quit"):
                    context.Response.StatusCode = StatusCodes.Status202Accepted;
                    _quit.TrySetResult();
                    break;
                default:
                    throw new StudioRefusal(404, $"No {request.Method} {path}.");
            }
        }
        catch (StudioRefusal refusal)
        {
            context.Response.StatusCode = refusal.Status;
            await JsonAsync(context, new { error = refusal.Message });
        }
        catch (Exception ex) when (!context.RequestAborted.IsCancellationRequested)
        {
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            await JsonAsync(context, new { error = ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }

    /// <summary>Refuses a request this session's page could not have sent.</summary>
    private void Admit(HttpRequest request)
    {
        var host = request.Host;
        if (host.Port != Port || !(host.Host.Equals("127.0.0.1", StringComparison.Ordinal)
                                   || host.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase)))
            throw new StudioRefusal(403, "dtpipe studio answers only at 127.0.0.1 or localhost, at its own port.");

        var origin = request.Headers.Origin.ToString();
        if (origin.Length > 0 && !origin.Equals($"http://{host.Value}", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(403, "A request from another site is refused.");
        if (request.Headers["Sec-Fetch-Site"].ToString() is { Length: > 0 } site && site is not ("same-origin" or "none"))
            throw new StudioRefusal(403, "A request from another site is refused.");
    }

    /// <summary>
    /// A job, the hash a save must find again, its layout, and whether saving would change its text:
    /// the server writes what it composes, so a file written by hand loses its comments and its order.
    /// </summary>
    private async Task OpenAsync(HttpContext context, string relative)
    {
        var job = _folder.ReadJob(relative);
        var rewrites = _tools.Design.Decompose(job.Text).Yaml != job.Text.Replace("\r\n", "\n");
        await JsonAsync(context, new { path = relative, yaml = job.Text, hash = job.Hash, layout = _local.ReadLayout(relative), rewrites });
    }

    /// <summary>
    /// Writes the job the server composes from the page's cards, never a text the page sends, then
    /// the cards' layout beside the folder's files. A job whose cards do not compose is written only
    /// when the page says the person accepted it.
    /// </summary>
    private async Task SaveAsync(HttpContext context)
    {
        var save = await BodyAsync<SaveRequest>(context);
        if (save.Model is null) throw new StudioRefusal(400, "A save carries the cards as 'model'.");
        var composed = _tools.Design.Compose(save.Model);
        if (!composed.Valid && !save.AcceptErrors)
            throw new StudioRefusal(422, "The cards do not compose into a job: " + string.Join(" ", composed.Errors));
        _folder.ResolveForWrite(save.Path ?? "");
        if (save.Model.Layout is not null) _local.CheckLayout(save.Path!);
        var hash = _folder.WriteJob(save.Path!, composed.Yaml, save.Hash);
        if (save.Model.Layout is { } layout) _local.WriteLayout(save.Path!, layout);
        await JsonAsync(context, new { path = save.Path, hash, yaml = composed.Yaml });
    }

    private async Task SampleAsync(HttpContext context)
    {
        if (!(context.Request.ContentType ?? "").StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(415, "A sample is asked as JSON.");

        string yaml;
        int rows;
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            yaml = body.RootElement.GetProperty("yaml").GetString() ?? "";
            rows = body.RootElement.TryGetProperty("rows", out var r) && r.TryGetInt32(out var n) ? n : 10;
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
        {
            throw new StudioRefusal(400, "A sample is asked as {\"yaml\": \"...\", \"rows\": n}.");
        }

        if (!await _sampling.WaitAsync(0))
            throw new StudioRefusal(409, "A sample is already running: wait for it to finish.");
        try
        {
            // Nobody watches the terminal for a prompt: a run that would ask one fails instead.
            using var _ = NonInteractiveGuard.Suppress();
            var json = await _tools.Sample(yaml, Math.Clamp(rows, 1, MaxRows), context.RequestAborted);
            context.Response.ContentType = "application/json; charset=utf-8";
            await context.Response.WriteAsync(json, context.RequestAborted);
        }
        finally
        {
            _sampling.Release();
        }
    }

    private static async Task ServeAssetAsync(HttpContext context, string path)
    {
        if (context.Request.Method != "GET" || !Assets.TryGetValue(path, out var asset))
            throw new StudioRefusal(404, $"No {context.Request.Method} {path}.");
        await using var stream = typeof(StudioServer).Assembly.GetManifestResourceStream(asset.Resource)
            ?? throw new StudioRefusal(404, $"No {path} in this build.");
        context.Response.ContentType = asset.Type;
        await stream.CopyToAsync(context.Response.Body, context.RequestAborted);
    }

    private static Task JsonAsync(HttpContext context, object value)
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        return context.Response.WriteAsync(JsonSerializer.Serialize(value, Json), context.RequestAborted);
    }

    private static async Task<T> BodyAsync<T>(HttpContext context) where T : class
    {
        if (!(context.Request.ContentType ?? "").StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(415, "The body is JSON.");
        try
        {
            return await JsonSerializer.DeserializeAsync<T>(context.Request.Body, Json, context.RequestAborted)
                ?? throw new StudioRefusal(400, "The body is empty.");
        }
        catch (JsonException ex)
        {
            throw new StudioRefusal(400, $"The body is not what {context.Request.Path} reads: {ex.Message}");
        }
    }

    private sealed record DecomposeRequest(string? Yaml, JsonObject? Layout);

    private sealed record SaveRequest(string? Path, string? Hash, DesignModel? Model, bool AcceptErrors);

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        _sampling.Dispose();
    }
}
