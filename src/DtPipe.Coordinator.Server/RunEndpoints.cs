using System.Text.Json;

namespace DtPipe.Coordinator.Server;

/// <summary>The runs and the deployments: what is in flight, the queue, a run asked for or cancelled, and the stream the pages watch.</summary>
internal static class RunEndpoints
{
    public static void MapRuns(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        // The Lab view's snapshot: the plan deployed last and its own run, whatever else is in flight.
        api.MapGet("/state", (DeploymentManager deployments, RunJournal journal) =>
        {
            var plan = deployments.ActivePlan;
            return new { plan, run = deployments.CurrentRun(plan?.PipelineId), history = journal.List(limit: 20) };
        });
        designer.MapPost("/runs", (RunRequest? request, DeploymentManager deployments) => deployments.Enqueue(request?.PipelineId));
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
