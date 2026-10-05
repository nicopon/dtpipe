using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What only a test bench needs, mapped in <see cref="CoordinatorServerOptions.LabMode"/> alone: the
/// Lab view's scenario catalog and manual planning (cuts and a placement chosen by hand), fault
/// injection, and a query on any node's database opened from the server's own process.
/// </summary>
public static class LabModeEndpoints
{
    public static void MapLabMode(this RouteGroupBuilder api)
    {
        api.MapGet("/pipelines", (PipelineCatalog catalog) => catalog.List());

        api.MapPost("/plan", async (PlanRequest request, PlanBuilder planner, RightsStore rights, BrickCatalog bricks, CancellationToken ct) =>
            rights.WithPolicies(await planner.BuildAsync(request, ct), request.Yaml, bricks));

        api.MapPost("/deploy", async (PlanRequest request, PlanBuilder planner, RightsStore rights, BrickCatalog bricks, DeploymentManager deployments, CancellationToken ct) =>
        {
            var plan = rights.WithPolicies(await planner.BuildAsync(request, ct), request.Yaml, bricks);
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

        // Reads a node's database through dtpipe, to show what a run wrote: the server opens the path
        // the node announced, so it works only where both share a file system.
        api.MapPost("/query", async (QueryRequest request, NodeInventory inventory, DtPipeCli cli, CoordinatorServerOptions options, CancellationToken ct) =>
        {
            var dataset = inventory.Snapshot().SelectMany(n => n.Datasets).FirstOrDefault(d => d.Variable == request.Variable && d.Path.Length > 0);
            if (dataset is null) return Results.BadRequest(new { error = $"No node announces a database at {request.Variable}." });
            Directory.CreateDirectory(options.StateDir);
            var result = await cli.RunAsync(
                ["--input", $"{dataset.Engine}:{dataset.Path}", "--query", request.Sql, "--limit", "200", "--output", "csv:-", "--no-stats"],
                options.StateDir, TimeSpan.FromSeconds(30), ct);
            return result.Succeeded
                ? Results.Ok(new { csv = result.Stdout })
                : Results.BadRequest(new { error = result.Diagnostic() });
        });
    }

    private sealed record QueryRequest(string Variable, string Sql);
}
