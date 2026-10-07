using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What only a test bench needs, mapped in <see cref="CoordinatorServerOptions.LabMode"/> alone: the
/// Lab view's scenario catalog and manual planning (cuts and a placement chosen by hand), fault
/// injection, and a query on a node's dataset, read by that node.
/// </summary>
public static class LabModeEndpoints
{
    public static void MapLabMode(this RouteGroupBuilder api)
    {
        api.MapGet("/pipelines", (PipelineCatalog catalog) => catalog.List());

        api.MapPost("/plan", async (PlanRequest request, PlanBuilder planner, CancellationToken ct) =>
            await planner.BuildAsync(request, ct));

        api.MapPost("/deploy", async (PlanRequest request, PlanBuilder planner, DeploymentManager deployments, CancellationToken ct) =>
        {
            var plan = await planner.BuildAsync(request, ct);
            await deployments.DeployAsync(plan, "lab", ct);
            return plan;
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

        // A query on a node's dataset, to show what a run wrote: the node that hosts it reads it, if
        // its own configuration answers lab requests; the server never opens a node's database.
        api.MapPost("/query", async (QueryRequest request, NodeInventory inventory, NodeRequests nodes, CancellationToken ct) =>
        {
            var host = inventory.Snapshot().FirstOrDefault(n => n.Datasets.Any(d => d.Variable == request.Variable && d.Engine.Length > 0));
            if (host is null) return Results.BadRequest(new { error = $"No node announces a dataset at {request.Variable}." });
            var answer = await nodes.AskAsync<DatasetQueryResult>(
                host.Name, NodeChannel.QueryDataset, [request.Variable, request.Sql, 200], TimeSpan.FromSeconds(40), ct);
            return answer.Error is null ? Results.Ok(new { csv = answer.Csv }) : Results.BadRequest(new { error = answer.Error });
        });
    }

    private sealed record QueryRequest(string Variable, string Sql);
}
