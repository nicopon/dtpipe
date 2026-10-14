using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>The library: the pipelines, their history, their automatic distribution, and the deployment of a saved plan.</summary>
internal static class LibraryEndpoints
{
    public static void MapLibrary(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        api.MapGet("/library", async (PipelineLibrary library, CancellationToken ct) => await library.ListAsync(ct));
        api.MapGet("/library/{id}", async (string id, PipelineLibrary library, CancellationToken ct) =>
            await library.GetAsync(id, ct) is { } doc ? Results.Ok(doc) : Results.NotFound(new { error = $"'{id}' is not in the library." }));
        // A save may say what it expects in place (If-None-Match: * for a new pipeline, If-Match: "<commit>"
        // for the version it read): 412 otherwise, with the commit in place. Without either, it overwrites.
        // Every save is checked first, the studio's and the pages' alike: a job the check refuses is neither
        // written nor committed, and with no runner or no brick node online the check cannot run, so it refuses.
        designer.MapPut(CoordinatorApi.LibraryRoute, async (string id, LibrarySave request, HttpRequest http, PipelineLibrary library,
            DesignValidator validator, DeploymentManager deployments, EventBus bus, CancellationToken ct) =>
        {
            // The check samples each brick on the node the placement names: the one this save carries over the one kept.
            var placed = new Dictionary<string, string>((await library.GetAsync(id, ct))?.Placement ?? new Dictionary<string, string>());
            foreach (var (alias, node) in request.Placement ?? new Dictionary<string, string>()) placed[alias] = node;
            DesignCheck check;
            try { check = await validator.ValidateAsync(request.Yaml, placed, ct); }
            catch (Exception ex) when (ex is not OperationCanceledException) { check = new DesignCheck(false, $"The job is not readable: {ex.Message}"); }
            if (!check.Ok)
                return Results.Json(new LibraryRefused("The designer's check refused this job: nothing was saved.", check.Report),
                    statusCode: StatusCodes.Status422UnprocessableEntity);
            if (request.Placement?.Any(p => string.IsNullOrWhiteSpace(p.Key) || string.IsNullOrWhiteSpace(p.Value)) == true)
                return Results.BadRequest(new { error = "A placement names a node for each branch: no empty alias, no empty node." });
            var absent = http.Headers.IfNoneMatch.ToString().Trim() == "*";
            var expected = http.Headers.IfMatch.ToString().Trim() is { Length: > 0 } tag ? (tag.StartsWith("W/") ? tag[2..] : tag).Trim('"') : null;
            var precondition = absent || expected is not null ? new SavePrecondition(absent, expected) : null;
            SaveOutcome saved;
            try { saved = await library.SaveAsync(id, request.Yaml, request.Message ?? "", ct, precondition, request.Placement); }
            catch (PreconditionFailedException ex)
            {
                return Results.Json(new LibraryConflict(ex.Message, ex.Commit), statusCode: StatusCodes.Status412PreconditionFailed);
            }
            bus.Publish("library", new { id, commit = saved.Commit });
            // A deployment runs the text it was planned from: only the saved plan, still current, is this text.
            var deployed = deployments.DeployedPlanId(id) is { } deployedPlan
                && (saved.PlanStale || saved.PlanId is null || deployedPlan != saved.PlanId);
            var problems = (await library.GetAsync(id, ct))?.PlacementProblems.Select(p => p.Message).ToList() ?? [];
            return Results.Ok(new LibrarySaved(id, saved.Commit, saved.PlanStale, deployed, problems));
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
            var (yaml, diff) = await library.AtAsync(id, commit, ct);
            return new { yaml, diff };
        });

        // The node of each brick branch of a job, kept beside it. What it names is saved as given, an absent node included:
        // the pipeline then lists it as a problem, which a deployment refuses.
        designer.MapPut("/library/{id}/placement", async (string id, PlacementRequest request, PipelineLibrary library, EventBus bus, CancellationToken ct) =>
        {
            var placement = request.Placement ?? new Dictionary<string, string>();
            if (placement.Any(p => string.IsNullOrWhiteSpace(p.Key) || string.IsNullOrWhiteSpace(p.Value)))
                return Results.BadRequest(new { error = "A placement names a node for each branch: no empty alias, no empty node." });
            var saved = await library.SavePlacementAsync(id, placement, request.Message ?? "", ct);
            bus.Publish("library", new { id, commit = saved.Commit });
            var problems = (await library.GetAsync(id, ct))?.PlacementProblems ?? [];
            return Results.Ok(new { id, commit = saved.Commit, redistribute = saved.PlanStale, problems });
        });

        // Distribution: the library's job placed automatically, previewed or saved as the pipeline's plan.
        designer.MapPost("/library/{id}/distribute", async (string id, DistributeRequest? request, PipelineLibrary library, AutoPlacer placer, EventBus bus, CancellationToken ct) =>
        {
            var doc = await library.GetAsync(id, ct);
            if (doc is null) return Results.NotFound(new { error = $"'{id}' is not in the library." });
            var plan = await placer.PlanAsync(id, doc.Yaml, doc.Placement, ct);
            string? commit = null;
            if (request?.Save == true)
            {
                if (!plan.Deployable) return Results.Conflict(new { error = "This plan cannot be saved: " + string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "")) });
                commit = await library.SavePlanAsync(id, plan, request.Message ?? "", ct);
                bus.Publish("library", new { id, commit });
            }
            return Results.Ok(new { plan, commit, bricks = placer.BrickAliases(doc.Yaml, doc.Placement), offers = placer.Offers(doc.Yaml) });
        });
        designer.MapPost("/library/{id}/deploy", async (string id, PipelineLibrary library, DeploymentManager deployments, CancellationToken ct) =>
        {
            var doc = await library.GetAsync(id, ct);
            if (doc?.Plan is null) return Results.Conflict(new { error = $"'{id}' has no saved distributed plan: distribute it first." });
            if (doc.PlacementProblems.Where(p => p.Blocking).Select(p => p.Message).ToList() is { Count: > 0 } refused)
                return Results.Conflict(new { error = $"The placement of '{id}' does not hold: " + string.Join(" ", refused) });
            if (!doc.PlanCurrent)
                return Results.Conflict(new
                {
                    error = doc.PlanStaleCause switch
                    {
                        PlanStaleCauses.Job => $"'{id}' changed since its plan was saved: distribute it again.",
                        PlanStaleCauses.Placement => $"The placement of '{id}' changed since its plan was saved: distribute it again.",
                        _ => $"A node of '{id}' holds other groups than its plan names, or has no enabled identity: distribute it again.",
                    },
                });
            await deployments.DeployAsync(doc.Plan.Plan, "library", ct);
            return Results.Ok(doc.Plan.Plan);
        });
    }
}

internal sealed record DistributeRequest(bool Save = false, string? Message = null);

internal sealed record PlacementRequest(IReadOnlyDictionary<string, string>? Placement, string? Message = null);
