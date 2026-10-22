using System.Text.Json;
using System.Text.RegularExpressions;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>The runs and the deployments: what is in flight, the queue, a run asked for or cancelled, and the stream the pages watch.</summary>
internal static partial class RunEndpoints
{
    [GeneratedRegex(@"^[A-Za-z][A-Za-z0-9_-]*\z")]
    private static partial Regex AliasPattern();

    private static CursorView ViewOf(CursorRecord c) => new(c.Alias, c.Column, c.Type, c.Value, c.RunId, c.UpdatedAt, c.SetBy);

    public static void MapRuns(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        // The Lab view's snapshot: the plan deployed last and its own run, whatever else is in flight.
        api.MapGet("/state", (DeploymentManager deployments, RunJournal journal) =>
        {
            var plan = deployments.ActivePlan;
            return new { plan, run = deployments.CurrentRun(plan?.PipelineId), history = journal.List(limit: 20) };
        });
        designer.MapPost("/runs", (RunRequest? request, DeploymentManager deployments, HttpContext http) =>
            deployments.Enqueue(request?.PipelineId, TrustedIdp.CallerOf(http)));
        // The cursors of a pipeline: whoever sees the pipeline sees its values; the right to launch it resets or sets one.
        api.MapGet(CoordinatorApi.CursorsRoute, (string id, DeploymentManager deployments) =>
            CoordinatorApi.IsPipelineId(id)
                ? Results.Ok(deployments.Cursors.Of(id).Select(ViewOf).ToList())
                : Results.BadRequest(new { error = $"'{id}' is no pipeline id." }));
        designer.MapDelete(CoordinatorApi.CursorRoute, (string id, string alias, DeploymentManager deployments) =>
            !CoordinatorApi.IsPipelineId(id) || !AliasPattern().IsMatch(alias)
                ? Results.BadRequest(new { error = "Name a pipeline and a branch." })
                : deployments.ResetCursor(id, alias) ? Results.NoContent() : Results.NotFound(new { error = $"'{id}' keeps no cursor for '{alias}'." }));
        designer.MapPut(CoordinatorApi.CursorRoute, (string id, string alias, CursorSet? request, DeploymentManager deployments, HttpContext http) =>
        {
            if (!CoordinatorApi.IsPipelineId(id) || !AliasPattern().IsMatch(alias) || string.IsNullOrEmpty(request?.Value))
                return Results.BadRequest(new { error = "Name a pipeline, a branch and a value: {\"value\": \"…\"}." });
            return Results.Ok(ViewOf(deployments.SetCursor(id, alias, request.Type, request.Value, TrustedIdp.CallerOf(http) ?? "")));
        });
        api.MapGet("/runs", (string? pipeline, RunJournal journal) => journal.List(pipeline));
        api.MapGet("/runs/queue", (DeploymentManager deployments) => deployments.Queue);
        designer.MapPost("/runs/cancel", (CancelRequest? request, DeploymentManager deployments) =>
        {
            deployments.Cancel(request?.RunId);
            return Results.Accepted();
        });

        api.MapGet("/deployments", async (DeploymentManager deployments) => await deployments.DeploymentsAsync());
        designer.MapDelete("/deployments/{id}", async (string id, DeploymentManager deployments, CancellationToken ct) =>
        {
            await deployments.UndeployAsync(id, ct);
            return Results.Accepted();
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
                var plan = deployments.ActivePlan;
                await WriteAsync("state", new { plan, run = deployments.CurrentRun(plan?.PipelineId) });
                await WriteAsync("deployments", await deployments.DeploymentsAsync());
                foreach (var evt in backlog.Where(e => e.Type == "log").TakeLast(150)) await WriteAsync(evt.Type, evt.Data);
                await foreach (var evt in live.ReadAllAsync(context.RequestAborted)) await WriteAsync(evt.Type, evt.Data);
            }
            catch (OperationCanceledException) { }
            finally { unsubscribe(); }
        });
    }
}

internal sealed record RunRequest(string? PipelineId = null);
internal sealed record CancelRequest(string? RunId);
