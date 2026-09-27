using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Lab.Contracts;
using DtPipe.Lab.Coordinator;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

var lab = LabOptions.FromConfiguration(new ConfigurationBuilder().AddCommandLine(args).Build());

// The page is served from the lab's sources: a build output carries no wwwroot outside Development.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
    WebRootPath = Path.Combine(lab.LabRoot, "Lab.Coordinator", "wwwroot"),
});
builder.Logging.AddFilter("TransportR", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
var flowMatrix = FlowMatrix.Load(Path.Combine(lab.LabRoot, "flow-matrix.json"));

builder.Services.AddSingleton(lab);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

// The coordinator exactly as DtPipe.Coordinator ships it; only the dev identity and the matrix are
// the lab's. Every pipeline node lands in the runtime group, which may send to itself.
builder.Services
    .AddCoordinatorHub(flow =>
    {
        foreach (var (group, targets) in flowMatrix.Groups)
            flow.Groups[group] = new GroupAccess { CanSendTo = targets.ToList() };
        flow.Groups[LabOptions.RuntimeGroup] = new GroupAccess { CanSendTo = [LabOptions.RuntimeGroup] };
    })
    .UseDevMode(o =>
    {
        o.AllowAnonymous = true;
        o.DefaultGroups = [LabOptions.RuntimeGroup];
    })
    .Build();

builder.Services.AddSingleton<LoggingHubProgressMonitor>();
builder.Services.Replace(ServiceDescriptor.Singleton<IHubProgressMonitor, LabProgressMonitor>());

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
    catch (LabConflictException ex)
    {
        context.Response.StatusCode = StatusCodes.Status409Conflict;
        await context.Response.WriteAsJsonAsync(new { error = ex.Message });
    }
});

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapDataHub(requireAuth: false);
app.MapHub<LabHub>(LabHubMethods.Path);

var api = app.MapGroup("/api");

api.MapGet("/nodes", (NodeInventory inventory) => inventory.Snapshot());
api.MapGet("/flow-matrix", () => flowMatrix);
api.MapGet("/bricks", (BrickCatalog bricks) => bricks.List());

// A brick is read by the node that owns it, never by the coordinator.
api.MapPost("/bricks/{node}/{id}/preview", async (string node, string id, NodeInventory inventory, IHubContext<LabHub> hub, CancellationToken ct) =>
{
    var connection = inventory.ConnectionOf(node);
    if (connection is null) return Results.Conflict(new { error = $"{node} is offline." });
    using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
    bounded.CancelAfter(TimeSpan.FromSeconds(40));
    var preview = await hub.Clients.Client(connection).InvokeAsync<BrickPreview>(LabHubMethods.PreviewBrick, id, 20, bounded.Token);
    return preview.Error is null ? Results.Ok(new { csv = preview.Csv }) : Results.BadRequest(new { error = preview.Error });
});
api.MapGet("/pipelines", (PipelineCatalog catalog) => catalog.List());

api.MapPost("/graph", (GraphRequest request) =>
{
    try { return Results.Ok(JobGraph.Describe(JobYaml.Parse(request.Yaml))); }
    catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
});

// A command line becomes a job through dtpipe's own --export-job.
api.MapPost("/pipelines/from-command", async (CommandRequest request, DtPipeCli cli, LabOptions options, CancellationToken ct) =>
{
    var dir = Path.Combine(options.StateDir, "commands", Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(dir);
    var jobPath = Path.Combine(dir, "job.yaml");
    var result = await cli.RunAsync([.. request.Args, "--export-job", jobPath], dir, TimeSpan.FromSeconds(30), ct);
    return result.Succeeded && File.Exists(jobPath)
        ? Results.Ok(new { yaml = await File.ReadAllTextAsync(jobPath, ct) })
        : Results.BadRequest(new { error = result.Diagnostic() });
});

api.MapPost("/plan", async (PlanRequest request, PlanBuilder planner, CancellationToken ct) =>
    await planner.BuildAsync(request, ct));

api.MapPost("/deploy", async (PlanRequest request, PlanBuilder planner, DeploymentManager deployments, CancellationToken ct) =>
{
    var plan = await planner.BuildAsync(request, ct);
    await deployments.DeployAsync(plan, "lab", ct);
    return plan;
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
api.MapPost("/fragments/{name}/kill", async (string name, DeploymentManager deployments, CancellationToken ct) =>
{
    await deployments.KillAsync(name, ct);
    return Results.Accepted();
});
api.MapPost("/fragments/{name}/variant", async (string name, DeploymentManager deployments, CancellationToken ct) =>
{
    await deployments.DeployVariantAsync(name, ct);
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
api.MapPost("/design/validate", async (DesignModel model, DesignService design, DtPipeCli cli, LabOptions options, CancellationToken ct) =>
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
api.MapPost("/library/{id}/deploy", async (string id, PipelineLibrary library, DeploymentManager deployments, CancellationToken ct) =>
{
    var doc = await library.GetAsync(id, ct);
    if (doc?.Plan is null) return Results.Conflict(new { error = $"'{id}' has no saved distributed plan: distribute it first." });
    if (!doc.PlanCurrent) return Results.Conflict(new { error = $"'{id}' changed since its plan was saved: distribute it again." });
    await deployments.DeployAsync(doc.Plan.Plan, "library", ct);
    return Results.Ok(doc.Plan.Plan);
});

// Reads a node's database through dtpipe, to show what a run wrote. Single-machine shortcut: the
// coordinator opens the node's file directly.
api.MapPost("/query", async (QueryRequest request, NodeInventory inventory, DtPipeCli cli, LabOptions options, CancellationToken ct) =>
{
    var dataset = inventory.Snapshot().SelectMany(n => n.Datasets).FirstOrDefault(d => d.Variable == request.Variable);
    if (dataset is null) return Results.BadRequest(new { error = $"No node hosts {request.Variable}." });
    Directory.CreateDirectory(options.StateDir);
    var result = await cli.RunAsync(
        ["--input", $"{dataset.Engine}:{dataset.Path}", "--query", request.Sql, "--limit", "200", "--output", "csv:-", "--no-stats"],
        options.StateDir, TimeSpan.FromSeconds(30), ct);
    return result.Succeeded
        ? Results.Ok(new { csv = result.Stdout })
        : Results.BadRequest(new { error = result.Diagnostic() });
});

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
internal sealed record QueryRequest(string Variable, string Sql);

/// <summary><c>flow-matrix.json</c>: which node group may send to which. Checked on every plan.</summary>
internal sealed record FlowMatrix(Dictionary<string, string[]> Groups)
{
    public static FlowMatrix Load(string path) =>
        JsonSerializer.Deserialize<FlowMatrix>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidOperationException($"{path}: empty flow matrix.");
}
