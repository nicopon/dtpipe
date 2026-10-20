using DtPipe.Coordinator.Contracts;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What only a test bench needs, mapped in <see cref="CoordinatorServerOptions.LabMode"/> alone: the
/// Lab view's scenario catalog and manual planning (cuts and a placement chosen by hand, the routes that plan in
/// <see cref="PlanMode.Manual"/>), and fault injection.
/// </summary>
public static class LabModeEndpoints
{
    public static void MapLabMode(this RouteGroupBuilder api)
    {
        api.MapGet("/pipelines", (PipelineCatalog catalog) => catalog.List());

        api.MapPost("/plan", async (PlanRequest request, PlanBuilder planner, CancellationToken ct) =>
            await planner.BuildAsync(request, PlanMode.Manual, ct));

        api.MapPost("/deploy", async (PlanRequest request, PlanBuilder planner, DeploymentManager deployments, CancellationToken ct) =>
        {
            var plan = await planner.BuildAsync(request, PlanMode.Manual, ct);
            await deployments.DeployAsync(plan, "lab", ct);
            return plan;
        });

        api.MapPost("/fragments/{name}/kill", async (string name, DeploymentManager deployments, CancellationToken ct) =>
        {
            var killed = await deployments.KillAsync(name, ct);
            return Results.Ok(new { killed });
        });
    }
}
