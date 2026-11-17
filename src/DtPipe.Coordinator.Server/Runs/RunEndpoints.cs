using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Cursors;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server.Runs;

/// <summary>The runs and the deployments: what is in flight, the queue, a run asked for or cancelled, and the stream the pages watch.</summary>
internal static partial class RunEndpoints
{
    [GeneratedRegex(@"^[A-Za-z][A-Za-z0-9_-]*\z")]
    private static partial Regex AliasPattern();

    private const int DefaultPage = 50;
    private const int MaxPage = 500;

    private static CursorView ViewOf(CursorRecord c) => new(c.Alias, c.Column, c.Type, c.Value, c.RunId, c.UpdatedAt, c.SetBy);

    public static void MapRuns(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        // The snapshot of a plan fixed by hand: the plan deployed last and its own run, whatever else is in flight.
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
        // The journal, newest first: a page of `limit` runs (50 when it is not given, at most MaxPage), of one pipeline and of one state
        // when they are named. X-Total-Count is how many runs answer those two filters before the page cuts them, from the page's snapshot.
        api.MapGet("/runs", (string? pipeline, string? state, int? limit, RunJournal journal, HttpContext http) =>
        {
            RunState? only = null;
            if (!string.IsNullOrEmpty(state))
            {
                // A table of the words, not Enum.TryParse, which takes "1" for a value.
                only = Enum.GetValues<RunState>().Cast<RunState?>().FirstOrDefault(s => string.Equals(s!.Value.Word(), state, StringComparison.OrdinalIgnoreCase));
                if (only is null)
                    return Results.BadRequest(new { error = $"'{state}' is no run state: {string.Join(", ", Enum.GetValues<RunState>().Select(s => s.Word()))}." });
            }
            var (runs, total) = journal.Page(pipeline, Math.Clamp(limit ?? DefaultPage, 1, MaxPage), only);
            http.Response.Headers["X-Total-Count"] = total.ToString(CultureInfo.InvariantCulture);
            return Results.Ok(runs);
        });
        // The latest run that is over of each pipeline, whole, however long ago it ended.
        api.MapGet("/runs/last", (RunJournal journal) => journal.Latest());
        api.MapGet("/runs/queue", (DeploymentManager deployments) => deployments.Queue);
        // One run, in flight, queued or journaled; the literal routes above take their own segment before this one.
        api.MapGet("/runs/{id}", (string id, DeploymentManager deployments) =>
            deployments.FindRun(id) is { } run ? Results.Ok(run) : Results.NotFound(new { error = $"'{id}' is no run the coordinator keeps." }));
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

            // `at` is when the event happened, so a page opened later dates the backlog it replays as it was, never as it loaded.
            async Task WriteAsync(string type, object data, DateTimeOffset? at = null)
            {
                await context.Response.WriteAsync($"data: {JsonSerializer.Serialize(new { type, data, at }, json)}\n\n");
                await context.Response.Body.FlushAsync();
            }

            var (backlog, live, unsubscribe) = bus.Subscribe();
            try
            {
                await WriteAsync("nodes", inventory.Snapshot());
                var plan = deployments.ActivePlan;
                await WriteAsync("state", new { plan, run = deployments.CurrentRun(plan?.PipelineId) });
                await WriteAsync("deployments", await deployments.DeploymentsAsync());
                foreach (var evt in backlog.Where(e => e.Type == "log").TakeLast(150)) await WriteAsync(evt.Type, evt.Data, evt.At);
                await foreach (var evt in live.ReadAllAsync(context.RequestAborted)) await WriteAsync(evt.Type, evt.Data, evt.At);
            }
            catch (OperationCanceledException) { }
            finally { unsubscribe(); }
        });
    }
}

internal sealed record RunRequest(string? PipelineId = null);
internal sealed record CancelRequest(string? RunId);
