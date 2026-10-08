using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

// The coordinator's server. Its settings are the Coordinator configuration section, from the
// environment first (Coordinator__Authority, Coordinator__StateDirectory, …); the pages are served
// from the wwwroot published beside the binary, whatever directory the server starts in.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.Logging.AddFilter("TransportR", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

var server = CoordinatorServerOptions.FromConfiguration(builder.Configuration);
builder.Services.AddSingleton(server);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

// The coordinator as DtPipe.Coordinator ships it, in TransportR's production mode: every client
// is identified by a token of the trusted IdP, its group read from the rights document by its
// subject. The flow matrix is the rights document's too, read live by the hub on every transfer and
// by the planner on every plan.
builder.Services.AddSingleton<RightsStore>();
builder.Services.AddCoordinatorHub(_ => { }).Build();
// A node answers a designer's check with a source brick's first row, larger than SignalR's 32 KB default.
builder.Services.Configure<HubOptions<NodeChannelHub>>(o => o.MaximumReceiveMessageSize = 1024 * 1024);
builder.Services.Replace(ServiceDescriptor.Singleton<IFlowControlService, RightsFlowControl>());
builder.Services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider, RightsIdentityProvider>());
builder.Services.AddTrustedIdp(server);

builder.Services.AddSingleton<LoggingHubProgressMonitor>();
builder.Services.Replace(ServiceDescriptor.Singleton<IHubProgressMonitor, EventProgressMonitor>());

builder.Services.AddSingleton<EventBus>();
builder.Services.AddSingleton<NodeInventory>();
builder.Services.AddSingleton<EnrollmentQueue>();
builder.Services.AddSingleton<NodeRequests>();
builder.Services.AddSingleton<BrickCatalog>();
builder.Services.AddSingleton<PipelineCatalog>();
builder.Services.AddSingleton<PlanBuilder>();
builder.Services.AddSingleton<AutoPlacer>();
builder.Services.AddSingleton(sp => new DesignService(sp.GetRequiredService<BrickCatalog>()));
builder.Services.AddSingleton<DesignValidator>();
builder.Services.AddSingleton<PipelineLibrary>();
builder.Services.AddSingleton<RunJournal>();
builder.Services.AddSingleton<DeploymentManager>();

var app = builder.Build();
app.Services.GetRequiredService<DeploymentManager>();

app.Use(async (context, next) =>
{
    try { await next(context); }
    catch (ConflictException ex)
    {
        context.Response.StatusCode = StatusCodes.Status409Conflict;
        await context.Response.WriteAsJsonAsync(new { error = ex.Message });
    }
});

// A 401 from /api names the document a client reads to learn how to sign in (RFC 9728).
app.Use((context, next) =>
{
    if (context.Request.Path.StartsWithSegments(CoordinatorApi.Prefix))
        context.Response.OnStarting(() =>
        {
            if (context.Response.StatusCode != StatusCodes.Status401Unauthorized) return Task.CompletedTask;
            var metadata = $"resource_metadata=\"{OriginOf(context)}{CoordinatorApi.Metadata}\"";
            var challenge = context.Response.Headers.WWWAuthenticate.ToString();
            context.Response.Headers.WWWAuthenticate = challenge.StartsWith("Bearer ", StringComparison.Ordinal)
                ? $"{challenge}, {metadata}"
                : $"Bearer {metadata}";
            return Task.CompletedTask;
        });
    return next(context);
});
app.UseAuthentication();
// The Lab view's pages exist only in lab mode, like the endpoints they call.
if (!server.LabMode)
    app.Use((context, next) => context.Request.Path.StartsWithSegments("/lab") ? Results.NotFound().ExecuteAsync(context) : next(context));
// A page is opened by a person signed in through the provider, with a role: the Lab view an admin's.
app.Use(async (context, next) =>
{
    if (!TrustedIdp.IsPageNavigation(context.Request) || context.Request.Path.StartsWithSegments("/account"))
    {
        await next(context);
        return;
    }
    if (context.User.Identity?.IsAuthenticated != true)
    {
        await context.ChallengeAsync(TrustedIdp.OidcScheme);
        return;
    }
    // A person with no role can open no page: they are signed out of the coordinator and of the
    // provider, so the next sign-in can be someone else's instead of the same refusal again.
    if (!context.User.IsInRole(CoordinatorRoles.Reader))
    {
        await Results.SignOut(new AuthenticationProperties { RedirectUri = "/account/denied" }, [TrustedIdp.CookieScheme, TrustedIdp.OidcScheme])
            .ExecuteAsync(context);
        return;
    }
    if (context.Request.Path.StartsWithSegments("/lab") && !context.User.IsInRole(CoordinatorRoles.Admin))
    {
        var who = System.Net.WebUtility.HtmlEncode(EmailOf(context) ?? context.User.Identity.Name ?? "This account");
        await AccessPage(StatusCodes.Status403Forbidden, $"{who} is not an admin of this coordinator: the Lab view is for administrators.",
            ("/", "Back to the coordinator"), ("/account/logout", "Sign out")).ExecuteAsync(context);
        return;
    }
    await next(context);
});
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();
app.UseAuthorization();
app.MapDataHub(requireAuth: true);
app.MapHub<NodeChannelHub>(NodeChannel.Path).RequireAuthorization();

// Anonymous: whether the server answers at all, for a container's health check.
app.MapGet("/healthz", () => Results.Ok());
// Anonymous: where a person signs in for dtpipe studio, and the contract this server speaks.
app.MapGet(CoordinatorApi.Metadata, (HttpContext context, CoordinatorServerOptions options) =>
    new ResourceMetadata(OriginOf(context), [options.Authority], ["header"], CoordinatorApi.Contract, options.StudioClientId));

// A person signs in and out through the provider; the pages send them here.
app.MapGet("/account/login", (string? returnUrl) => Results.Challenge(
    new AuthenticationProperties { RedirectUri = returnUrl is { Length: > 0 } url && url.StartsWith('/') && !url.StartsWith("//") ? url : "/" },
    [TrustedIdp.OidcScheme]));
app.MapGet("/account/logout", () => Results.SignOut(new AuthenticationProperties { RedirectUri = "/" }, [TrustedIdp.CookieScheme, TrustedIdp.OidcScheme]));
// Where a person with no role lands once signed out: anonymous, it names nobody.
app.MapGet("/account/denied", () => AccessPage(StatusCodes.Status200OK,
    "That account holds no role on this coordinator, so it has been signed out, here and at the identity provider.",
    ("/", "Sign in with another account")));

// /api serves the pages by the session cookie, and dtpipe studio by a person's bearer token, with
// three cumulative roles: a reader sees everything, a designer also changes pipelines, deploys and
// runs them, an admin also changes the rights and, in lab mode, uses the bench's tools. A request that changes something carries the CSRF
// header. TransportR's own routes under /api (client, stream) are mapped by MapDataHub, not here.
var account = app.MapGroup("/api").RequireAuthorization(TrustedIdp.SignedIn).AddEndpointFilter(RequireCsrfHeader);
account.MapGet(CoordinatorApi.MeRoute, (HttpContext context) => new CoordinatorMe(
    context.User.Identity?.Name, context.User.FindFirst("email")?.Value, [.. CoordinatorRoles.All.Where(context.User.IsInRole)]));
account.MapGet("/info", (CoordinatorServerOptions options) => new { labMode = options.LabMode });

var api = app.MapGroup("/api").RequireAuthorization(CoordinatorRoles.Reader).AddEndpointFilter(RequireCsrfHeader);
var designer = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Designer);
var admin = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Admin);

api.MapGet("/nodes", (NodeInventory inventory) => inventory.Snapshot());
api.MapGet("/flow-matrix", (RightsStore rights) => new { groups = rights.Snapshot().Matrix });
api.MapGet("/bricks", (BrickCatalog bricks) => bricks.List());

// A brick is read by the node that owns it, never by the coordinator.
designer.MapPost("/bricks/{node}/{id}/preview", async (string node, string id, NodeRequests nodes, CancellationToken ct) =>
{
    var preview = await nodes.AskAsync<BrickPreview>(node, NodeChannel.PreviewBrick, [id, 20], TimeSpan.FromSeconds(40), ct);
    return preview.Error is null ? Results.Ok(new { csv = preview.Csv }) : Results.BadRequest(new { error = preview.Error });
});

designer.MapPost("/graph", (GraphRequest request) =>
{
    try { return Results.Ok(JobGraph.Describe(JobYaml.Parse(request.Yaml))); }
    catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
});

// A command line becomes a job through dtpipe's own --export-job, run by a runner: the coordinator runs no dtpipe.
designer.MapPost("/pipelines/from-command", async (CommandRequest request, NodeRequests nodes, CancellationToken ct) =>
{
    try
    {
        var exported = await nodes.AskRunnerAsync<ExportedJob>(NodeChannel.ExportJob, [request.Args], TimeSpan.FromSeconds(40), ct);
        return exported.Yaml is { } yaml ? Results.Ok(new { yaml }) : Results.BadRequest(new { error = exported.Error });
    }
    catch (ConflictException ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
    {
        return Results.BadRequest(new { error = "The runner did not answer in time." });
    }
});


// The Lab view's snapshot: the plan deployed last and its own run, whatever else is in flight.
api.MapGet("/state", (DeploymentManager deployments, RunJournal journal) =>
{
    var plan = deployments.ActivePlan;
    return new { plan, run = deployments.CurrentRun(plan?.PipelineId), history = journal.List(limit: 20) };
});
designer.MapPost("/runs", (RunRequest? request, DeploymentManager deployments) => deployments.Enqueue(request?.PipelineId, request?.Pins));
api.MapGet("/runs", (string? pipeline, RunJournal journal) => journal.List(pipeline));
api.MapGet("/runs/queue", (DeploymentManager deployments) => deployments.Queue);
designer.MapPost("/runs/cancel", (CancelRequest? request, DeploymentManager deployments) =>
{
    deployments.Cancel(request?.RunId);
    return Results.Accepted();
});

api.MapGet("/deployments", (DeploymentManager deployments) => deployments.Deployments);
designer.MapDelete("/deployments/{id}", async (string id, DeploymentManager deployments, CancellationToken ct) =>
{
    await deployments.UndeployAsync(id, ct);
    return Results.Accepted();
});

// The designer: cards to job and back. The page never writes YAML itself.
designer.MapPost("/design/compose", (PageDesignModel model, DesignService design) => PageDesignResult.From(design.Compose(model.ToModel())));
designer.MapPost("/design/decompose", (DecomposeRequest request, DesignService design) => PageDesignResult.From(design.Decompose(request.Yaml, request.Layout)));
designer.MapPost("/design/validate", async (PageDesignModel model, DesignService design, DesignValidator validator, CancellationToken ct) =>
{
    var composed = design.Compose(model.ToModel());
    if (!composed.Valid) return Results.Ok(new { composed.Errors, dryRun = (string?)null, ok = false });
    var check = await validator.ValidateAsync(composed.Yaml, ct);
    return Results.Ok(new { composed.Errors, dryRun = check.Report, ok = check.Ok });
});

// The library.
api.MapGet("/library", async (PipelineLibrary library, CancellationToken ct) => await library.ListAsync(ct));
api.MapGet("/library/{id}", async (string id, PipelineLibrary library, CancellationToken ct) =>
    await library.GetAsync(id, ct) is { } doc ? Results.Ok(doc) : Results.NotFound(new { error = $"'{id}' is not in the library." }));
designer.MapPut(CoordinatorApi.LibraryRoute, async (string id, LibrarySave request, PipelineLibrary library, EventBus bus, CancellationToken ct) =>
{
    var commit = await library.SaveAsync(id, request.Yaml, request.Layout, request.Message ?? "", ct);
    bus.Publish("library", new { id, commit });
    return Results.Ok(new { id, commit });
});
designer.MapDelete("/library/{id}", async (string id, PipelineLibrary library, EventBus bus, CancellationToken ct) =>
{
    await library.DeleteAsync(id, ct);
    bus.Publish("library", new { id, commit = (string?)null });
    return Results.Accepted();
});
api.MapGet("/library/{id}/history", async (string id, PipelineLibrary library, CancellationToken ct) => await library.HistoryAsync(id, ct));
api.MapGet("/library/{id}/at/{commit}", async (string id, string commit, PipelineLibrary library, CancellationToken ct) =>
{
    var (yaml, layout, diff) = await library.AtAsync(id, commit, ct);
    return new { yaml, layout, diff };
});

// Distribution: the library's job placed automatically, previewed or saved as the pipeline's plan.
designer.MapPost("/library/{id}/distribute", async (string id, DistributeRequest? request, PipelineLibrary library, AutoPlacer placer, EventBus bus, CancellationToken ct) =>
{
    var doc = await library.GetAsync(id, ct);
    if (doc is null) return Results.NotFound(new { error = $"'{id}' is not in the library." });
    var plan = await placer.PlanAsync(id, doc.Yaml, ct);
    string? commit = null;
    if (request?.Save == true)
    {
        if (!plan.Deployable) return Results.Conflict(new { error = "This plan cannot be saved: " + string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "")) });
        commit = await library.SavePlanAsync(id, plan, request.Message ?? "", ct);
        bus.Publish("library", new { id, commit });
    }
    return Results.Ok(new { plan, commit, bricks = placer.BrickAliases(doc.Yaml) });
});
// The same placement for a pipeline still being designed: its cards composed, then placed, nothing saved.
designer.MapPost("/design/place", async (PlaceRequest request, DesignService design, AutoPlacer placer, CancellationToken ct) =>
{
    if (!CoordinatorApi.IsPipelineId(request.Id)) return Results.BadRequest(new { error = $"'{request.Id}' is not a pipeline id: lowercase letters, digits and '-'." });
    var composed = design.Compose(request.Model.ToModel());
    if (!composed.Valid) return Results.Ok(new { errors = composed.Errors, plan = (DistributionPlan?)null, bricks = new Dictionary<string, string>() });
    var plan = await placer.PlanAsync(request.Id, composed.Yaml, ct);
    return Results.Ok(new { errors = Array.Empty<string>(), plan, bricks = placer.BrickAliases(composed.Yaml) });
});
designer.MapPost("/library/{id}/deploy", async (string id, PipelineLibrary library, DeploymentManager deployments, CancellationToken ct) =>
{
    var doc = await library.GetAsync(id, ct);
    if (doc?.Plan is null) return Results.Conflict(new { error = $"'{id}' has no saved distributed plan: distribute it first." });
    if (!doc.PlanCurrent) return Results.Conflict(new { error = $"'{id}' changed since its plan was saved: distribute it again." });
    await deployments.DeployAsync(doc.Plan.Plan, "library", ct);
    return Results.Ok(doc.Plan.Plan);
});

// Rights: who may send what to whom. The page edits them; every change is audited and applies at once.
api.MapGet("/rights", (RightsStore rights, NodeInventory inventory, EnrollmentQueue enrollment, CoordinatorServerOptions options) =>
{
    var doc = rights.Snapshot();
    var online = inventory.Snapshot().Where(n => n.Online).Select(n => n.Name).ToHashSet();
    return new
    {
        idp = new { issuer = options.Authority, audience = options.Audience, grant = "client_credentials" },
        identities = doc.Identities.Select(i => new
        {
            i.ClientId, i.Name, i.Description, i.Groups, i.Enabled,
            connected = online.Contains(i.Name) && inventory.ClientIdOf(i.Name) == i.ClientId,
        }),
        groups = rights.Groups(),
        pending = enrollment.List(),
        matrix = doc.Matrix,
        audit = doc.Audit,
    };
});
admin.MapPut("/rights/matrix", (MatrixRequest request, RightsStore rights, EventBus bus, HttpContext http) =>
{
    rights.SetMatrix(request.Matrix, EmailOf(http));
    bus.Publish("rights", new { });
    return Results.Ok();
});
admin.MapPost("/rights/identities", (IdentityRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http) =>
{
    if (!PipelineLibrary.IsValidId(request.ClientId)) throw new ConflictException($"'{request.ClientId}' is not a client id: lowercase letters, digits and '-'.");
    rights.AddIdentity(new Identity(request.ClientId, string.IsNullOrWhiteSpace(request.Name) ? request.ClientId : request.Name, request.Description ?? "",
        request.Groups ?? [], request.Enabled ?? true), EmailOf(http));
    enrollment.Remove(request.ClientId);
    bus.Publish("rights", new { });
    return Results.Ok();
});
admin.MapPut("/rights/identities/{id}", (string id, IdentityRequest request, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
{
    var before = rights.Find(id);
    rights.UpdateIdentity(id, request.Name ?? id, request.Description ?? "", request.Groups ?? [], request.Enabled ?? true, EmailOf(http));
    // A node renamed connects again under its new name; the old one is forgotten once its host is gone.
    if (before is not null && rights.Find(id)!.Name != before.Name) { inventory.Disconnect(id); inventory.Forget(before.Name); }
    bus.Publish("rights", new { });
    return Results.Ok();
});
admin.MapDelete("/rights/identities/{id}", (string id, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
{
    var name = rights.Find(id)?.Name;
    rights.RemoveIdentity(id, EmailOf(http));
    inventory.Disconnect(id);
    if (name is not null) inventory.Forget(name);
    bus.Publish("rights", new { });
    return Results.Accepted();
});
// A host no identity knows waits here: the administrator names it and gives it its groups, or sets it aside.
admin.MapPost("/rights/pending/{subject}/accept", (string subject, AcceptRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http) =>
{
    var pending = enrollment.Find(subject) ?? throw new ConflictException($"'{subject}' does not wait for approval.");
    rights.AddIdentity(new Identity(subject, request.Name ?? "", request.Description ?? pending.Description, request.Groups ?? [], true), EmailOf(http));
    enrollment.Remove(subject);
    bus.Publish("rights", new { });
    return Results.Ok();
});
admin.MapPost("/rights/pending/{subject}/ignore", (string subject, EnrollmentQueue enrollment, EventBus bus) =>
{
    enrollment.Ignore(subject);
    bus.Publish("rights", new { });
    return Results.Ok();
});
// TransportR records a node's group when it connects and when it renews its token: dropping its
// control connection makes its host connect again under its current group. Fragments already
// deployed keep theirs until they are re-armed.
admin.MapPost("/rights/identities/{id}/reconnect", (string id, NodeInventory inventory) =>
    Results.Ok(new { disconnected = inventory.Disconnect(id) }));


if (server.LabMode) admin.MapLabMode();

api.MapGet("/events", async (HttpContext context, EventBus bus, NodeInventory inventory, DeploymentManager deployments) =>
{
    context.Response.Headers.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    var json = context.RequestServices.GetRequiredService<Microsoft.Extensions.Options.IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>>().Value.SerializerOptions;

    async Task WriteAsync(string type, object data)
    {
        await context.Response.WriteAsync($"data: {JsonSerializer.Serialize(new { type, data }, json)}\n\n");
        await context.Response.Body.FlushAsync();
    }

    var (backlog, live, unsubscribe) = bus.Subscribe();
    try
    {
        await WriteAsync("nodes", inventory.Snapshot());
        var plan = deployments.ActivePlan;
        await WriteAsync("state", new { plan, run = deployments.CurrentRun(plan?.PipelineId) });
        await WriteAsync("deployments", deployments.Deployments);
        foreach (var evt in backlog.Where(e => e.Type == "log").TakeLast(150)) await WriteAsync(evt.Type, evt.Data);
        await foreach (var evt in live.ReadAllAsync(context.RequestAborted)) await WriteAsync(evt.Type, evt.Data);
    }
    catch (OperationCanceledException) { }
    finally { unsubscribe(); }
});

app.Run();

static string? EmailOf(HttpContext context) => context.User.FindFirst("email")?.Value;

static string OriginOf(HttpContext context) => $"{context.Request.Scheme}://{context.Request.Host}";

// A refused page navigation: a sentence and its ways out, readable without the pages' scripts.
// The message is HTML already encoded.
static IResult AccessPage(int status, string message, params (string Href, string Label)[] links) => Results.Content(
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    + "<title>dtpipe coordinator</title><link rel=\"stylesheet\" href=\"/styles.css\"></head>"
    + "<body style=\"max-width:40rem;margin:4rem auto;padding:0 1rem;font-family:system-ui,sans-serif;line-height:1.5\">"
    + $"<h1 style=\"font-size:1.3rem\">dtpipe coordinator</h1><p>{message}</p><p>"
    + string.Join(" · ", links.Select(l => $"<a href=\"{l.Href}\">{l.Label}</a>"))
    + "</p></body></html>",
    "text/html; charset=utf-8",
    statusCode: status);

static async ValueTask<object?> RequireCsrfHeader(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
{
    var request = context.HttpContext.Request;
    if (HttpMethods.IsGet(request.Method) || HttpMethods.IsHead(request.Method) || request.Headers.ContainsKey(TrustedIdp.CsrfHeader))
        return await next(context);
    return Results.Problem($"A request that changes something carries the {TrustedIdp.CsrfHeader} header.", statusCode: StatusCodes.Status403Forbidden);
}

internal sealed record GraphRequest(string Yaml);
internal sealed record CommandRequest(string[] Args);
internal sealed record RunRequest(Dictionary<string, string>? Pins, string? PipelineId = null);
internal sealed record CancelRequest(string? RunId);
internal sealed record DecomposeRequest(string Yaml, System.Text.Json.Nodes.JsonObject? Layout = null);
internal sealed record PlaceRequest(string Id, PageDesignModel Model);
internal sealed record DistributeRequest(bool Save = false, string? Message = null);
internal sealed record MatrixRequest(Dictionary<string, List<string>> Matrix);
internal sealed record AcceptRequest(string? Name, string? Description, List<string>? Groups);
internal sealed record IdentityRequest(string ClientId, string? Name, string? Description, List<string>? Groups, bool? Enabled);
