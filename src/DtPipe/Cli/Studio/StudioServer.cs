using System.Net;
using System.Security.Cryptography;
using System.Text;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Security;
using DtPipe.Cli.Split;
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

/// <summary>What <c>--bind-input</c> and <c>--bind-output</c> would say: <c>alias=location</c> pairs, comma-separated.</summary>
public sealed record StudioBindings(string? Input, string? Output)
{
    public static readonly StudioBindings None = new(null, null);
}

/// <summary>A reader or a writer the designer's palette offers, by its component name.</summary>
public sealed record StudioComponent(string Name, string Category);

/// <summary>
/// What the studio's server works with: the engine, the designer's model (the coordinator's own, with
/// dtpipe's components for endpoints), the components the palette lists, the keyring a variable may be
/// launched from, and the coordinator the studio starts with, if any (<see cref="CoordinatorAtStart"/>: the page may replace it,
/// so the server reads the link in place, never this one).
/// <see cref="DesignWith"/> gives the same model over the coordinator's bricks; without it, <see cref="Design"/> alone.
/// <see cref="Settings"/> keeps the remembered connections and recent lists; without it, the page opens a folder and remembers none.
/// </summary>
public sealed record StudioTools(
    IStudioEngine Engine,
    DesignService Design,
    IReadOnlyList<StudioComponent> Readers,
    IReadOnlyList<StudioComponent> Writers,
    ISecretsManager Secrets,
    CoordinatorLink? CoordinatorAtStart = null,
    Func<IBrickLookup, DesignService>? DesignWith = null,
    StudioImporter? Import = null)
{
    public StudioSettings? Settings { get; init; }
    public StudioAgentSetup? Agent { get; init; }
    public StudioAgentLink? AgentLink { get; init; }
    public CoordinatorConnection? CoordinatorStart { get; init; }
}

/// <summary>
/// The engine as the studio asks it: a sample, a real run, and the cut a preview runs. Each call carries the
/// <c>arrow:-</c> boundaries it binds and its cancellation; a variable is armed by the caller, for the call alone.
/// </summary>
public interface IStudioEngine
{
    /// <summary>Cuts a job at the cards a preview pins.</summary>
    DebugCutter Cutter { get; }

    /// <summary>The sample of a job over <paramref name="rows"/> source rows, as the <c>dry-run</c> tool answers it: JSON.</summary>
    Task<string> SampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct);

    /// <summary>
    /// The sample of a cut job: each source read for up to <see cref="StudioServer.DebugMaxRows"/> rows, and
    /// <see cref="StudioServer.MaxRows"/> of them shown per stage.
    /// </summary>
    Task<string> DebugSampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct);

    /// <summary>
    /// The real run of a saved job, or of its cut, writers of the sink bricks bound to throwaway files. A job that
    /// does not validate or that the SQL safety policy refuses throws, naming why, before anything runs; an engine fault
    /// comes back in <see cref="RunOutcome.Fault"/> beside the failures and the checkpoints the run reached.
    /// </summary>
    Task<RunOutcome> RunAsync(string yaml, StudioBindings bindings, CancellationToken ct);
}

/// <summary>A command line, as the arguments of one process, as the text of the job file it stands for.</summary>
public delegate string StudioImporter(IReadOnlyList<string> args);

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
/// One engine action runs at a time: a sample, a run and a quit refuse each other (409).
/// </summary>
public sealed class StudioServer : IAsyncDisposable
{
    public const string TokenHeader = "X-Dtpipe-Studio";
    public const int MaxRows = 1000;
    public const int DebugMaxRows = 100_000;
    public const int Stopped = 130;

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
        ["/state.js"] = ("studio/state.js", "text/javascript; charset=utf-8"),
        ["/render.js"] = ("studio/render.js", "text/javascript; charset=utf-8"),
        ["/prefs.js"] = ("studio/prefs.js", "text/javascript; charset=utf-8"),
        ["/preview.js"] = ("studio/preview.js", "text/javascript; charset=utf-8"),
        ["/run.js"] = ("studio/run.js", "text/javascript; charset=utf-8"),
        ["/publish.js"] = ("studio/publish.js", "text/javascript; charset=utf-8"),
        ["/dialog.js"] = ("studio/dialog.js", "text/javascript; charset=utf-8"),
        ["/recent-list.js"] = ("studio/recent-list.js", "text/javascript; charset=utf-8"),
        ["/connect-dialogs.js"] = ("studio/connect-dialogs.js", "text/javascript; charset=utf-8"),
        ["/job-dialogs.js"] = ("studio/job-dialogs.js", "text/javascript; charset=utf-8"),
        ["/variables.js"] = ("studio/variables.js", "text/javascript; charset=utf-8"),
        ["/agent.js"] = ("studio/agent.js", "text/javascript; charset=utf-8"),
        ["/studio.css"] = ("studio/studio.css", "text/css; charset=utf-8"),
        ["/favicon.svg"] = ("studio/favicon.svg", "image/svg+xml"),
    };

    private readonly WebApplication _app;
    private readonly WorkspaceSlot _workspace;
    private readonly CoordinatorSlot _coordinator;
    private readonly AgentSlot _agentSlot;
    private readonly EngineSlot _engineSlot = new();
    private readonly StudioRouteTable _routes = new();
    private readonly CoordinatorRoutes _coordinatorRoutes;
    private readonly RunRoutes _runRoutes;
    private readonly byte[] _token;
    private readonly TaskCompletionSource _quit = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private StudioServer(WebApplication app, StudioFolder folder, StudioTools tools, string token)
    {
        _app = app;
        Token = token;
        _token = Encoding.UTF8.GetBytes(token);

        _workspace = new WorkspaceSlot(new StudioWorkspace(folder, tools.Secrets), "argument");

        _coordinator = new CoordinatorSlot(
            tools.CoordinatorAtStart,
            tools.CoordinatorStart ?? (tools.CoordinatorAtStart is { } given
                ? new CoordinatorConnection(given.Origin.ToString().TrimEnd('/'), given.ByKey ? "key" : "browser", "flag", given.ByKey ? "flag" : "none")
                : null));
        if (tools.CoordinatorAtStart is { ByKey: true } remembered && _coordinator.Connection?.Source == "remembered")
            _coordinator.StartSignIn(remembered);

        _agentSlot = new AgentSlot(
            null,
            tools.AgentLink?.Start ?? (tools.Agent is { } started
                ? new AgentConnection(started.Client.ProviderName, started.BaseUrl, started.Model,
                    (int)AgentOptions.DefaultLlmTimeout.TotalSeconds, "flag", "none")
                : null));

        var design = new StudioDesignContext(_workspace, _coordinator, _engineSlot, tools);
        var debug = new DebugRoutes(_workspace, _engineSlot, tools.Engine, design);
        _runRoutes = new RunRoutes(_workspace, _coordinator, _engineSlot, _agentSlot, tools.Engine, design, debug, _token);
        var jobs = new JobRoutes(_workspace, _coordinator, tools, design, debug, _runRoutes);
        _coordinatorRoutes = new CoordinatorRoutes(_workspace, _coordinator, design, () => Port);
        var coordinatorConnection = new CoordinatorConnectionRoutes(_coordinator, _engineSlot, tools.Settings);
        var agent = new AgentRoutes(_workspace, _agentSlot, _engineSlot, design, () => _runRoutes.RunInProgress);
        StudioAgent CreateAgent(StudioAgentSetup setup) => new(setup, new StudioAgentTools(setup.Tools, agent));
        if (tools.Agent is { } initialAgent)
            _agentSlot.SetInitial(CreateAgent(initialAgent));

        var agentConnection = new AgentConnectionRoutes(_agentSlot, tools.AgentLink, tools.Settings, CreateAgent);
        void Opened()
        {
            _workspace.Current.Local.PurgeSamples();
            debug.PurgeDrafts();
        }
        var workspaceRoutes = new WorkspaceRoutes(
            _workspace, _agentSlot, _engineSlot, tools.Settings, tools.Secrets, folder.Root,
            () => _runRoutes.RunInProgress,
            () => { _runRoutes.ClearRun(); Opened(); },
            CreateAgent);

        Opened();

        jobs.Register(_routes);
        debug.Register(_routes);
        _runRoutes.Register(_routes);
        _coordinatorRoutes.Register(_routes);
        coordinatorConnection.Register(_routes);
        agent.Register(_routes);
        agentConnection.Register(_routes);
        workspaceRoutes.Register(_routes);
        _routes.Map("POST", "/api/quit", context =>
        {
            if (_runRoutes.RunInProgress) throw new StudioRefusal(409, "A run is in progress: stop it, or wait for it, before quitting.");
            context.Response.StatusCode = StatusCodes.Status202Accepted;
            _quit.TrySetResult();
            return Task.CompletedTask;
        }, RouteLocks.None);
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
                await _coordinatorRoutes.CallbackAsync(context, AdmitHost);
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

            if (!_routes.TryGet(request.Method, path, out var route))
                throw new StudioRefusal(404, $"No {request.Method} {path}.");

            using var workspace = route.Locks.HasFlag(RouteLocks.Workspace) ? _workspace.Hold() : null;
            using var coordinator = route.Locks.HasFlag(RouteLocks.Coordinator) ? _coordinator.Hold() : null;
            await route.Handle(context);
        }
        catch (StudioRefusal refusal)
        {
            context.Response.StatusCode = refusal.Status;
            await StudioHttp.JsonAsync(context, new { error = ConnectionStringSanitizer.Sanitize(refusal.Message) });
        }
        catch (Exception ex) when (!context.RequestAborted.IsCancellationRequested)
        {
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            await StudioHttp.JsonAsync(context, new { error = StudioHttp.Shown(ex) });
        }
    }

    internal static string Shown(Exception failure) => StudioHttp.Shown(failure);

    internal static string ExpandHome(string path, string? home = null) => WorkspaceRoutes.ExpandHome(path, home);

    internal static string ForRecognition(string yaml) => StudioDesignContext.ForRecognition(yaml);

    internal static string? SecretsRefusal(string yaml) => StudioDesignContext.SecretsRefusal(yaml);

    internal static void RefuseMisusedReferences(string yaml) => StudioDesignContext.RefuseMisusedReferences(yaml);

    internal static void RefuseReferences(string yaml, BrickCatalogView? signedIn = null) => StudioDesignContext.RefuseReferences(yaml, signedIn);

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

    private static async Task ServeAssetAsync(HttpContext context, string path)
    {
        if (context.Request.Method != "GET" || !Assets.TryGetValue(path, out var asset))
            throw new StudioRefusal(404, $"No {context.Request.Method} {path}.");
        await using var stream = typeof(StudioServer).Assembly.GetManifestResourceStream(asset.Resource)
            ?? throw new StudioRefusal(404, $"No {path} in this build.");
        context.Response.ContentType = asset.Type;
        await stream.CopyToAsync(context.Response.Body, context.RequestAborted);
    }

    public async ValueTask DisposeAsync()
    {
        // The agent's waits end first, then the run it may have started; its turn is awaited last.
        var agent = _agentSlot.Current?.DisposeAsync();
        await _runRoutes.StopRunAndWaitAsync();
        if (agent is { } stopping) await stopping;
        await _app.StopAsync();
        await _app.DisposeAsync();
        _agentSlot.Dispose();
        _engineSlot.Dispose();
        _coordinator.Dispose();
    }
}
