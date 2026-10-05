using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
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
builder.Services.Replace(ServiceDescriptor.Singleton<IFlowControlService, RightsFlowControl>());
builder.Services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider, RightsIdentityProvider>());
builder.Services.AddTrustedIdp(server);

builder.Services.AddSingleton<LoggingHubProgressMonitor>();
builder.Services.Replace(ServiceDescriptor.Singleton<IHubProgressMonitor, EventProgressMonitor>());

builder.Services.AddSingleton<EventBus>();
builder.Services.AddSingleton<NodeInventory>();
builder.Services.AddSingleton<BrickCatalog>();
builder.Services.AddSingleton<DtPipeCli>();
builder.Services.AddSingleton<PipelineCatalog>();
builder.Services.AddSingleton<PlanBuilder>();
builder.Services.AddSingleton<AutoPlacer>();
builder.Services.AddSingleton<DesignService>();
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

// The Lab view's pages exist only in lab mode, like the endpoints they call.
if (!server.LabMode)
    app.Use((context, next) => context.Request.Path.StartsWithSegments("/lab") ? Results.NotFound().ExecuteAsync(context) : next(context));
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();
app.MapDataHub(requireAuth: true);
app.MapHub<NodeChannelHub>(NodeChannel.Path).RequireAuthorization();

var api = app.MapGroup("/api");

api.MapGet("/info", (CoordinatorServerOptions options) => new { labMode = options.LabMode });
api.MapGet("/nodes", (NodeInventory inventory) => inventory.Snapshot());
api.MapGet("/flow-matrix", (RightsStore rights) => new { groups = rights.Snapshot().Matrix });
api.MapGet("/bricks", (BrickCatalog bricks) => bricks.List());

// A brick is read by the node that owns it, never by the coordinator.
api.MapPost("/bricks/{node}/{id}/preview", async (string node, string id, NodeInventory inventory, IHubContext<NodeChannelHub> hub, CancellationToken ct) =>
{
    var connection = inventory.ConnectionOf(node);
    if (connection is null) return Results.Conflict(new { error = $"{node} is offline." });
    using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
    bounded.CancelAfter(TimeSpan.FromSeconds(40));
    var preview = await hub.Clients.Client(connection).InvokeAsync<BrickPreview>(NodeChannel.PreviewBrick, id, 20, bounded.Token);
    return preview.Error is null ? Results.Ok(new { csv = preview.Csv }) : Results.BadRequest(new { error = preview.Error });
});

api.MapPost("/graph", (GraphRequest request) =>
{
    try { return Results.Ok(JobGraph.Describe(JobYaml.Parse(request.Yaml))); }
    catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
});

// A command line becomes a job through dtpipe's own --export-job.
api.MapPost("/pipelines/from-command", async (CommandRequest request, DtPipeCli cli, CoordinatorServerOptions options, CancellationToken ct) =>
{
    var dir = Path.Combine(options.StateDir, "commands", Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(dir);
    var jobPath = Path.Combine(dir, "job.yaml");
    var result = await cli.RunAsync([.. request.Args, "--export-job", jobPath], dir, TimeSpan.FromSeconds(30), ct);
    return result.Succeeded && File.Exists(jobPath)
        ? Results.Ok(new { yaml = await File.ReadAllTextAsync(jobPath, ct) })
        : Results.BadRequest(new { error = result.Diagnostic() });
});


api.MapGet("/state", (DeploymentManager deployments, RunJournal journal) =>
    new { plan = deployments.ActivePlan, run = deployments.CurrentRun, history = journal.List(limit: 20) });
api.MapPost("/runs", (RunRequest? request, DeploymentManager deployments) => deployments.Enqueue(request?.PipelineId, request?.Pins));
api.MapGet("/runs", (string? pipeline, RunJournal journal) => journal.List(pipeline));
api.MapGet("/runs/queue", (DeploymentManager deployments) => deployments.Queue);
api.MapPost("/runs/cancel", (CancelRequest? request, DeploymentManager deployments) =>
{
    deployments.Cancel(request?.RunId);
    return Results.Accepted();
});

api.MapGet("/deployments", (DeploymentManager deployments) => deployments.Deployments);
api.MapDelete("/deployments/{id}", async (string id, DeploymentManager deployments, CancellationToken ct) =>
{
    await deployments.UndeployAsync(id, ct);
    return Results.Accepted();
});

// The designer: cards to job and back. The page never writes YAML itself.
api.MapPost("/design/compose", (DesignModel model, DesignService design) => design.Compose(model));
api.MapPost("/design/decompose", (DecomposeRequest request, DesignService design) => design.Decompose(request.Yaml, request.Layout));
api.MapPost("/design/validate", async (DesignModel model, DesignService design, DtPipeCli cli, CoordinatorServerOptions options, CancellationToken ct) =>
{
    var composed = design.Compose(model);
    if (!composed.Valid) return Results.Ok(new { composed.Errors, dryRun = (string?)null, ok = false });
    // dtpipe itself judges the job: one row per source through the real engine, writers neutralised.
    var dir = Path.Combine(options.StateDir, "validate", Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(dir);
    var jobPath = Path.Combine(dir, "job.yaml");
    await File.WriteAllTextAsync(jobPath, composed.Yaml, ct);
    var result = await cli.RunAsync(["--job", jobPath, "--dry-run", "1", "--no-stats"], dir, TimeSpan.FromSeconds(60), ct);
    return Results.Ok(new { composed.Errors, dryRun = result.Succeeded ? null : result.Diagnostic(8), ok = result.Succeeded });
});

// The library.
api.MapGet("/library", async (PipelineLibrary library, CancellationToken ct) => await library.ListAsync(ct));
api.MapGet("/library/{id}", async (string id, PipelineLibrary library, CancellationToken ct) =>
    await library.GetAsync(id, ct) is { } doc ? Results.Ok(doc) : Results.NotFound(new { error = $"'{id}' is not in the library." }));
api.MapPut("/library/{id}", async (string id, SaveRequest request, PipelineLibrary library, EventBus bus, CancellationToken ct) =>
{
    var commit = await library.SaveAsync(id, request.Yaml, request.Layout, request.Message ?? "", ct);
    bus.Publish("library", new { id, commit });
    return Results.Ok(new { id, commit });
});
api.MapDelete("/library/{id}", async (string id, PipelineLibrary library, EventBus bus, CancellationToken ct) =>
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
api.MapPost("/library/{id}/distribute", async (string id, DistributeRequest? request, PipelineLibrary library, AutoPlacer placer, BrickCatalog bricks, EventBus bus, CancellationToken ct) =>
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
    // Which brick each branch is, for the page to name it.
    var brickOf = new Dictionary<string, string>(StringComparer.Ordinal);
    try
    {
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(doc.Yaml)))
            if (bricks.Match(branch) is { } brick) brickOf[alias] = brick.Key;
    }
    catch (Exception) { }
    return Results.Ok(new { plan, commit, bricks = brickOf });
});
api.MapPost("/library/{id}/deploy", async (string id, PipelineLibrary library, RightsStore rights, BrickCatalog bricks, DeploymentManager deployments, CancellationToken ct) =>
{
    var doc = await library.GetAsync(id, ct);
    if (doc?.Plan is null) return Results.Conflict(new { error = $"'{id}' has no saved distributed plan: distribute it first." });
    if (!doc.PlanCurrent) return Results.Conflict(new { error = $"'{id}' changed since its plan was saved: distribute it again." });
    // A saved plan is checked against the rights as they are now.
    var violations = rights.PolicyViolations(doc.Plan.Plan, doc.Yaml, bricks);
    if (violations.Count > 0) return Results.Conflict(new { error = string.Join(" ", violations) });
    await deployments.DeployAsync(doc.Plan.Plan, "library", ct);
    return Results.Ok(doc.Plan.Plan);
});

// Rights: who may send what to whom. The page edits them; every change is audited and applies at once.
api.MapGet("/rights", (RightsStore rights, NodeInventory inventory, CoordinatorServerOptions options) =>
{
    var doc = rights.Snapshot();
    var online = inventory.Snapshot().Where(n => n.Online).Select(n => n.Name).ToHashSet();
    return new
    {
        idp = new { issuer = options.Authority, audience = options.Audience, grant = "client_credentials" },
        identities = doc.Identities.Select(i => new
        {
            i.ClientId, i.DisplayName, i.Group, i.Node, i.Enabled,
            connected = i.Node is not null && online.Contains(i.Node) && inventory.ClientIdOf(i.Node) == i.ClientId,
        }),
        groups = rights.Groups(),
        matrix = doc.Matrix,
        brickPolicies = doc.BrickPolicies,
        audit = doc.Audit,
    };
});
api.MapPut("/rights/matrix", (MatrixRequest request, RightsStore rights, EventBus bus) =>
{
    rights.SetMatrix(request.Matrix);
    bus.Publish("rights", new { });
    return Results.Ok();
});
api.MapPut("/rights/bricks/{node}/{id}", (string node, string id, PolicyRequest request, RightsStore rights, EventBus bus) =>
{
    rights.SetBrickPolicy($"{node}/{id}", request.Groups);
    bus.Publish("rights", new { });
    return Results.Ok();
});
api.MapPost("/rights/identities", (IdentityRequest request, RightsStore rights, EventBus bus) =>
{
    if (!PipelineLibrary.IsValidId(request.ClientId)) throw new ConflictException($"'{request.ClientId}' is not a client id: lowercase letters, digits and '-'.");
    if (string.IsNullOrWhiteSpace(request.Group)) throw new ConflictException("An identity belongs to one group.");
    rights.AddIdentity(new Identity(request.ClientId, request.DisplayName ?? request.ClientId, request.Group.Trim(),
        string.IsNullOrWhiteSpace(request.Node) ? null : request.Node.Trim(), request.Enabled ?? true));
    bus.Publish("rights", new { });
    return Results.Ok();
});
api.MapPut("/rights/identities/{id}", (string id, IdentityRequest request, RightsStore rights, EventBus bus) =>
{
    if (string.IsNullOrWhiteSpace(request.Group)) throw new ConflictException("An identity belongs to one group.");
    rights.UpdateIdentity(id, request.DisplayName ?? id, request.Group.Trim(), request.Node, request.Enabled ?? true);
    bus.Publish("rights", new { });
    return Results.Ok();
});
api.MapDelete("/rights/identities/{id}", (string id, RightsStore rights, NodeInventory inventory, EventBus bus) =>
{
    rights.RemoveIdentity(id);
    inventory.Disconnect(id);
    bus.Publish("rights", new { });
    return Results.Accepted();
});
// TransportR records a node's group when it connects and when it renews its token: dropping its
// control connection makes its host connect again under its current group. Fragments already
// deployed keep theirs until they are re-armed.
api.MapPost("/rights/identities/{id}/reconnect", (string id, NodeInventory inventory) =>
    Results.Ok(new { disconnected = inventory.Disconnect(id) }));


if (server.LabMode) api.MapLabMode();

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
        await WriteAsync("state", new { plan = deployments.ActivePlan, run = deployments.CurrentRun });
        await WriteAsync("deployments", deployments.Deployments);
        foreach (var evt in backlog.Where(e => e.Type == "log").TakeLast(150)) await WriteAsync(evt.Type, evt.Data);
        await foreach (var evt in live.ReadAllAsync(context.RequestAborted)) await WriteAsync(evt.Type, evt.Data);
    }
    catch (OperationCanceledException) { }
    finally { unsubscribe(); }
});

app.Run();

internal sealed record GraphRequest(string Yaml);
internal sealed record CommandRequest(string[] Args);
internal sealed record RunRequest(Dictionary<string, string>? Pins, string? PipelineId = null);
internal sealed record CancelRequest(string? RunId);
internal sealed record DecomposeRequest(string Yaml, System.Text.Json.Nodes.JsonObject? Layout = null);
internal sealed record SaveRequest(string Yaml, System.Text.Json.Nodes.JsonObject? Layout, string? Message);
internal sealed record DistributeRequest(bool Save = false, string? Message = null);
internal sealed record MatrixRequest(Dictionary<string, List<string>> Matrix);
internal sealed record PolicyRequest(List<string>? Groups);
internal sealed record IdentityRequest(string ClientId, string? DisplayName, string Group, string? Node, bool? Enabled);
