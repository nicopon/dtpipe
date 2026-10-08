using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Samples a job: its YAML, the source rows to read, the <c>arrow:-</c> boundaries to bind and the
/// call's cancellation. Returns the sample as JSON.
/// </summary>
public delegate Task<string> StudioSampler(string yaml, int rows, StudioBindings bindings, CancellationToken ct);

/// <summary>What <c>--bind-input</c> and <c>--bind-output</c> would say: <c>alias=location</c> pairs, comma-separated.</summary>
public sealed record StudioBindings(string? Input, string? Output)
{
    public static readonly StudioBindings None = new(null, null);
}

/// <summary>A reader or a writer the designer's palette offers, by its component name.</summary>
public sealed record StudioComponent(string Name, string Category);

/// <summary>
/// What the studio's server works with: the sample path, the designer's model (the coordinator's
/// own, with dtpipe's components for endpoints), the components the palette lists, the keyring
/// a variable may be launched from, and the coordinator the command line named, if any.
/// <see cref="DesignWith"/> gives the same model over the coordinator's bricks; without it, <see cref="Design"/> alone.
/// </summary>
public sealed record StudioTools(
    StudioSampler Sample, DesignService Design, IReadOnlyList<StudioComponent> Readers, IReadOnlyList<StudioComponent> Writers,
    ISecretsManager Secrets, CoordinatorLink? Coordinator = null, Func<IBrickLookup, DesignService>? DesignWith = null);

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
/// no CORS header is ever sent. Only <see cref="CoordinatorLink.Callback"/> is reached without the
/// token, by a navigation from the identity provider: a GET that answers by checking its single-use
/// <c>state</c>;</item>
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
        ["/coordinator.js"] = ("studio/coordinator.js", "text/javascript; charset=utf-8"),
        ["/studio.css"] = ("studio/studio.css", "text/css; charset=utf-8"),
    };

    private static readonly JsonSerializerOptions Json = JsonSerializerOptions.Web;

    private readonly WebApplication _app;
    private readonly StudioFolder _folder;
    private readonly StudioTools _tools;
    private readonly StudioLocalFiles _local;
    private readonly StudioVariables _variables;
    private readonly byte[] _token;
    private readonly SemaphoreSlim _sampling = new(1, 1);
    private readonly TaskCompletionSource _quit = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private StudioServer(WebApplication app, StudioFolder folder, StudioTools tools, string token)
    {
        _app = app;
        _folder = folder;
        _tools = tools;
        _local = new StudioLocalFiles(folder);
        _variables = new StudioVariables(_local, tools.Secrets);
        _local.PurgeSamples();
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
            var path = request.Path.Value ?? "/";
            if (path == CoordinatorLink.Callback)
            {
                await CallbackAsync(context);
                return;
            }
            Admit(request);
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
                case ("GET", "/api/bricks"):
                    await BricksAsync(context);
                    break;
                case ("POST", "/api/design/compose"):
                    await JsonAsync(context, Design().Compose(await BodyAsync<DesignModel>(context)));
                    break;
                case ("POST", "/api/design/decompose"):
                    var decompose = await BodyAsync<DecomposeRequest>(context);
                    await JsonAsync(context, (await DesignForAsync(decompose.Yaml ?? "", context.RequestAborted)).Decompose(decompose.Yaml ?? "", decompose.Layout));
                    break;
                case ("POST", "/api/variables"):
                    var asked = await BodyAsync<VariablesRequest>(context);
                    // A brick's branch runs on its node, which resolves its variables: they are none of this machine's.
                    var variables = _variables.Of(JobPath(asked.Path), WithoutBricks(asked.Yaml ?? "", asked.Bricks));
                    // The keyring's aliases are listed only for a job with a variable to launch.
                    var aliases = variables.Any(v => v.Kind == "env") ? _variables.Aliases() : [];
                    await JsonAsync(context, new { variables, aliases });
                    break;
                case ("PUT", "/api/launch"):
                    await LaunchAsync(context);
                    break;
                case ("POST", "/api/sample"):
                    await SampleAsync(context);
                    break;
                case ("GET", "/api/coordinator"):
                    await JsonAsync(context, (object?)_tools.Coordinator?.Status() ?? new { url = (string?)null });
                    break;
                case ("POST", "/api/coordinator/login"):
                    var link = _tools.Coordinator ?? throw new StudioRefusal(404, "This studio was opened without --coordinator.");
                    try { await JsonAsync(context, new { url = await link.BeginSignInAsync(Port, context.RequestAborted) }); }
                    catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, ex.Message); }
                    break;
                case ("POST", "/api/coordinator/logout"):
                    _tools.Coordinator?.SignOutNow();
                    context.Response.StatusCode = StatusCodes.Status204NoContent;
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
        AdmitHost(request);

        var origin = request.Headers.Origin.ToString();
        if (origin.Length > 0 && !origin.Equals($"http://{request.Host.Value}", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(403, "A request from another site is refused.");
        if (request.Headers["Sec-Fetch-Site"].ToString() is { Length: > 0 } site && site is not ("same-origin" or "none"))
            throw new StudioRefusal(403, "A request from another site is refused.");
    }

    /// <summary>Refuses a name other than the loopback's, or another port: a page served from a name that resolves here.</summary>
    private void AdmitHost(HttpRequest request)
    {
        var host = request.Host;
        if (host.Port != Port || !(host.Host.Equals("127.0.0.1", StringComparison.Ordinal)
                                   || host.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase)))
            throw new StudioRefusal(403, "dtpipe studio answers only at 127.0.0.1 or localhost, at its own port.");
    }

    /// <summary>
    /// The identity provider sends the browser back here with a code: a cross-site navigation, so no
    /// token and no same-origin check, only the host's. What makes it this studio's is the single-use
    /// <c>state</c> the link checks before exchanging the code. The answer is a line of text, and never
    /// shows a token.
    /// </summary>
    private async Task CallbackAsync(HttpContext context)
    {
        AdmitHost(context.Request);
        if (context.Request.Method != "GET") throw new StudioRefusal(405, "The sign-in comes back as a GET.");
        string message;
        if (_tools.Coordinator is not { } link)
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            message = "This studio was opened without --coordinator.";
        }
        else
        {
            var query = context.Request.Query;
            string? Value(string name) => query[name] is { Count: 1 } v ? v.ToString() : null;
            try
            {
                var who = await link.CompleteSignInAsync(Value("code"), Value("state"), Value("iss"), Value("error"), context.RequestAborted);
                message = $"Signed in to {link.Origin.ToString().TrimEnd('/')} as {who}. You can close this tab and go back to dtpipe studio.";
            }
            catch (CoordinatorLinkException ex)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                message = ex.Message;
            }
        }
        context.Response.ContentType = "text/plain; charset=utf-8";
        await context.Response.WriteAsync(message + "\n", context.RequestAborted);
    }

    /// <summary>
    /// A job, the hash a save must find again, its layout, and whether saving would change its text:
    /// the server writes what it composes, so a file written by hand loses its comments and its order.
    /// </summary>
    private async Task OpenAsync(HttpContext context, string relative)
    {
        var job = _folder.ReadJob(relative);
        var rewrites = (await DesignForAsync(job.Text, context.RequestAborted)).Decompose(job.Text).Yaml != job.Text.Replace("\r\n", "\n");
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
        var composed = Design().Compose(save.Model);
        if (!composed.Valid && !save.AcceptErrors)
            throw new StudioRefusal(422, "The cards do not compose into a job: " + string.Join(" ", composed.Errors));
        _folder.ResolveForWrite(save.Path ?? "");
        if (save.Model.Layout is not null) _local.CheckLayout(save.Path!);
        var hash = _folder.WriteJob(save.Path!, composed.Yaml, save.Hash);
        if (save.Model.Layout is { } layout) _local.WriteLayout(save.Path!, layout);
        await JsonAsync(context, new { path = save.Path, hash, yaml = composed.Yaml });
    }

    /// <summary>A job's path, confined to the folder, or null for none.</summary>
    private string? JobPath(string? relative)
    {
        if (string.IsNullOrEmpty(relative)) return null;
        _folder.ResolveForWrite(relative);
        return relative;
    }

    /// <summary>
    /// Keeps what each variable of a job is launched with: a keyring alias or a typed text. A value
    /// from the keyring is never stored, only its alias.
    /// </summary>
    private async Task LaunchAsync(HttpContext context)
    {
        var launch = await BodyAsync<LaunchRequest>(context);
        var job = JobPath(launch.Path) ?? throw new StudioRefusal(400, "Launch values belong to a job: name its path.");
        var overrides = new Dictionary<string, StudioOverride>(StringComparer.Ordinal);
        foreach (var (name, chosen) in launch.Overrides ?? [])
        {
            if (chosen is null || chosen.Keyring is null == chosen.Value is null)
                throw new StudioRefusal(400, $"'{name}' is launched with either a keyring alias or a text.");
            if (chosen.Value is { Length: > 65_536 })
                throw new StudioRefusal(413, $"The text for '{name}' is too long.");
            overrides[name] = chosen;
        }
        _local.WriteLaunch(job, overrides);
        context.Response.StatusCode = StatusCodes.Status204NoContent;
    }

    private async Task SampleAsync(HttpContext context)
    {
        if (!(context.Request.ContentType ?? "").StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(415, "A sample is asked as JSON.");

        string yaml;
        int rows;
        string? path;
        List<SampledBrick>? bricks;
        try
        {
            using var body = await JsonDocument.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            yaml = body.RootElement.GetProperty("yaml").GetString() ?? "";
            rows = body.RootElement.TryGetProperty("rows", out var r) && r.TryGetInt32(out var n) ? n : 10;
            path = body.RootElement.TryGetProperty("path", out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;
            bricks = body.RootElement.TryGetProperty("bricks", out var b) && b.ValueKind == JsonValueKind.Array
                ? b.Deserialize<List<SampledBrick>>(Json)
                : null;
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
        {
            throw new StudioRefusal(400, "A sample is asked as {\"yaml\": \"...\", \"rows\": n}.");
        }
        rows = Math.Clamp(rows, 1, MaxRows);

        // Each brick's branch is read or written on its node: here, its rows come from the coordinator.
        var sampling = BrickSampling.Of(yaml, bricks, _tools.Coordinator?.Catalog);
        if (sampling.Sources.Count > 0 && _tools.Coordinator?.Status().State != CoordinatorLink.SignedIn)
            throw new StudioRefusal(409, "This job reads a node's brick: sign in to the coordinator to sample it.");

        // Every variable resolves before anything runs, and is named when it would not.
        var launch = _variables.LaunchEnvironment(JobPath(path), sampling.Yaml);

        if (!await _sampling.WaitAsync(0))
            throw new StudioRefusal(409, "A sample is already running: wait for it to finish.");
        // The run reads ${{NAME}} from the environment, through its own resolver: each chosen value is
        // set for this sample only, which no other runs beside, and the environment restored after.
        var previous = launch.Keys.ToDictionary(k => k, Environment.GetEnvironmentVariable, StringComparer.Ordinal);
        var files = new List<string>();
        try
        {
            var inputs = new List<string>();
            foreach (var (alias, brick) in sampling.Sources)
            {
                byte[] arrow;
                try { arrow = await _tools.Coordinator!.SampleAsync(brick.Node, brick.Id, Math.Min(rows, CoordinatorApi.MaxSampleRows), context.RequestAborted); }
                catch (CoordinatorLinkException ex) { throw new StudioRefusal(502, $"'{alias}': {ex.Message}"); }
                files.Add(_local.SampleFile(arrow));
                inputs.Add($"{alias}={Bindable(files[^1])}");
            }
            var outputs = new List<string>();
            foreach (var alias in sampling.Sinks)
            {
                files.Add(_local.SampleFile(null));
                outputs.Add($"{alias}={Bindable(files[^1])}");
            }
            var bindings = new StudioBindings(inputs.Count > 0 ? string.Join(',', inputs) : null, outputs.Count > 0 ? string.Join(',', outputs) : null);

            foreach (var (name, value) in launch) Environment.SetEnvironmentVariable(name, value);
            // Nobody watches the terminal for a prompt: a run that would ask one fails instead.
            using var _ = NonInteractiveGuard.Suppress();
            var json = await _tools.Sample(sampling.Yaml, rows, bindings, context.RequestAborted);
            context.Response.ContentType = "application/json; charset=utf-8";
            await context.Response.WriteAsync(json, context.RequestAborted);
        }
        finally
        {
            foreach (var (name, value) in previous) Environment.SetEnvironmentVariable(name, value);
            foreach (var file in files) File.Delete(file);
            _sampling.Release();
        }
    }

    /// <summary>A location a binding can carry: its pairs are separated by commas.</summary>
    private static string Bindable(string path) => path.Contains(',')
        ? throw new StudioRefusal(400, "The folder's path holds a comma, which a binding cannot carry: open the studio on another folder to sample a brick.")
        : path;

    /// <summary>The designer's model over the catalog read last: what a card names is found, nothing is recognised.</summary>
    private DesignService Design() =>
        _tools.DesignWith is { } with && _tools.Coordinator is { } link ? with(new StudioBricks(link.Catalog)) : _tools.Design;

    /// <summary>
    /// The model for taking <paramref name="yaml"/> apart: the coordinator says what each branch is when a
    /// person is signed in; otherwise no branch is a brick and each opens as a local card.
    /// </summary>
    private async Task<DesignService> DesignForAsync(string yaml, CancellationToken ct)
    {
        if (_tools.DesignWith is not { } with || _tools.Coordinator is not { } link) return _tools.Design;
        string job;
        try { job = JobYaml.Serialize(JobYaml.Parse(yaml)); }
        catch (Exception) { return Design(); }
        var answers = await link.RecognizeAsync(job, ct);
        return with(new StudioBricks(link.Catalog, job, answers));
    }

    /// <summary>The job with each brick's branch replaced as a sample replaces it; as it is when that cannot be done.</summary>
    private string WithoutBricks(string yaml, List<SampledBrick>? bricks)
    {
        try { return BrickSampling.Of(yaml, bricks, _tools.Coordinator?.Catalog).Yaml; }
        catch (Exception ex) when (ex is StudioRefusal or FormatException or YamlDotNet.Core.YamlException) { return yaml; }
    }

    /// <summary>
    /// The coordinator's catalog, read again when a person is signed in, with this dtpipe's version: the
    /// palette's bricks and the versions the page compares. The catalog read last when it cannot be read.
    /// </summary>
    private async Task BricksAsync(HttpContext context)
    {
        var link = _tools.Coordinator;
        string? error = null;
        BrickCatalogView? catalog = null;
        if (link is not null)
        {
            try { catalog = await link.RefreshCatalogAsync(context.RequestAborted); }
            catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException)
            {
                error = ex.Message;
                catalog = link.Catalog;
            }
        }
        await JsonAsync(context, new
        {
            studioVersion = CoordinatorLink.StudioVersion,
            nodes = catalog?.Nodes ?? [],
            bricks = catalog?.Bricks ?? [],
            error,
        });
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

    private sealed record VariablesRequest(string? Path, string? Yaml, List<SampledBrick>? Bricks);

    private sealed record LaunchRequest(string? Path, Dictionary<string, StudioOverride?>? Overrides);

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        _sampling.Dispose();
    }
}
