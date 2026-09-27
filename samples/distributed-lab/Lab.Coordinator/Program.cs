using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Lab.Contracts;
using DtPipe.Lab.Coordinator;
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
builder.Services.AddSingleton<DtPipeCli>();
builder.Services.AddSingleton<PipelineCatalog>();
builder.Services.AddSingleton<PlanBuilder>();
builder.Services.AddSingleton<LabRunner>();

var app = builder.Build();

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

api.MapPost("/deploy", async (PlanRequest request, PlanBuilder planner, LabRunner runner, CancellationToken ct) =>
{
    var plan = await planner.BuildAsync(request, ct);
    await runner.DeployAsync(plan, ct);
    return plan;
});

api.MapGet("/state", (LabRunner runner) => new { plan = runner.ActivePlan, run = runner.CurrentRun, history = runner.History });
api.MapPost("/runs", (RunRequest? request, LabRunner runner) => runner.StartRun(request?.Pins));
api.MapPost("/runs/cancel", (LabRunner runner) => { runner.Cancel(); return Results.Accepted(); });
api.MapPost("/fragments/{name}/kill", async (string name, LabRunner runner, CancellationToken ct) =>
{
    await runner.KillAsync(name, ct);
    return Results.Accepted();
});
api.MapPost("/fragments/{name}/variant", async (string name, LabRunner runner, CancellationToken ct) =>
{
    await runner.DeployVariantAsync(name, ct);
    return Results.Accepted();
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

api.MapGet("/events", async (HttpContext context, EventBus bus, NodeInventory inventory, LabRunner runner) =>
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
        await WriteAsync("state", new { plan = runner.ActivePlan, run = runner.CurrentRun });
        foreach (var evt in backlog.Where(e => e.Type == "log").TakeLast(150)) await WriteAsync(evt.Type, evt.Data);
        await foreach (var evt in live.ReadAllAsync(context.RequestAborted)) await WriteAsync(evt.Type, evt.Data);
    }
    catch (OperationCanceledException) { }
    finally { unsubscribe(); }
});

app.Run();

internal sealed record GraphRequest(string Yaml);
internal sealed record CommandRequest(string[] Args);
internal sealed record RunRequest(Dictionary<string, string>? Pins);
internal sealed record QueryRequest(string Variable, string Sql);

/// <summary><c>flow-matrix.json</c>: which node group may send to which. Checked on every plan.</summary>
internal sealed record FlowMatrix(Dictionary<string, string[]> Groups)
{
    public static FlowMatrix Load(string path) =>
        JsonSerializer.Deserialize<FlowMatrix>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidOperationException($"{path}: empty flow matrix.");
}
