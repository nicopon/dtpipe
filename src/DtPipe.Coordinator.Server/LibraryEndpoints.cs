using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>The library: the pipelines, their history, the plan a launch would make now, and their deployment.</summary>
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
            if (Refusal(request.Bindings) is { } malformed) return Results.BadRequest(new { error = malformed });
            // The check samples each brick on the node its binding names: the one this save carries over the one kept. What a check
            // pins is written with them; a pin the request brings is not read.
            var held = (await library.GetAsync(id, ct))?.Bindings ?? new Dictionary<string, Binding>();
            var referenced = BrickReferences.In(request.Yaml).Keys.ToHashSet(StringComparer.Ordinal);
            var bound = BrickReferences.Expand(request.Yaml, Bindings.Merge(held, request.Bindings));
            var (refused, check) = await CheckedAsync(validator, request.Yaml, bound, ct);
            if (refused is not null) return refused;
            // The entries to write: those the request names, those the check re-pinned, which have a mode other than strict, and the
            // referenced bricks, which every mode pins at a version.
            var pinned = Bindings.Pinned(bound, check!.Pins, referenced);
            var written = pinned.Where(b => request.Bindings?.ContainsKey(b.Key) == true || !Bindings.IsStrict(b.Value) || referenced.Contains(b.Key))
                .ToDictionary(b => b.Key, b => b.Value, StringComparer.Ordinal);
            var absent = http.Headers.IfNoneMatch.ToString().Trim() == "*";
            var expected = http.Headers.IfMatch.ToString().Trim() is { Length: > 0 } tag ? (tag.StartsWith("W/") ? tag[2..] : tag).Trim('"') : null;
            var precondition = absent || expected is not null ? new SavePrecondition(absent, expected) : null;
            SaveOutcome saved;
            try { saved = await library.SaveAsync(id, request.Yaml, request.Message ?? "", ct, precondition, written); }
            catch (PreconditionFailedException ex)
            {
                return Results.Json(new LibraryConflict(ex.Message, ex.Commit), statusCode: StatusCodes.Status412PreconditionFailed);
            }
            bus.Publish("library", new { id, commit = saved.Commit });
            // A deployment runs the job and the bindings it was armed with: it is another version unless the library holds, by
            // content, the same ones (a commit that put the old text back is the same job).
            var deployed = deployments.ArmedOf(id) is { } armed
                && (armed.JobSha != PipelineLibrary.Sha(request.Yaml) || armed.BindingsSha != Bindings.Sha(saved.Bindings));
            var problems = (await library.GetAsync(id, ct))?.BindingProblems.Select(p => p.Message).ToList() ?? [];
            return Results.Ok(new LibrarySaved(id, saved.Commit, deployed, problems));
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

        // The binding of each brick branch of a job, kept beside it. What it names is saved as given, an absent node included:
        // the pipeline then lists it as a problem, which a deployment refuses. Like a save, it is checked first.
        designer.MapPut("/library/{id}/bindings", async (string id, BindingsRequest request, PipelineLibrary library,
            DesignValidator validator, EventBus bus, CancellationToken ct) =>
        {
            var bindings = request.Bindings ?? new Dictionary<string, Binding>();
            if (Refusal(bindings) is { } malformed) return Results.BadRequest(new { error = malformed });
            var doc = await library.GetAsync(id, ct) ?? throw new ConflictException($"'{id}' is not in the library.");
            var referenced = BrickReferences.In(doc.Yaml).Keys.ToHashSet(StringComparer.Ordinal);
            var stripped = BrickReferences.Expand(doc.Yaml, bindings.ToDictionary(b => b.Key, b => b.Value with { Pin = null }, StringComparer.Ordinal));
            var (refused, check) = await CheckedAsync(validator, doc.Yaml, stripped, ct);
            if (refused is not null) return refused;
            var saved = await library.SaveBindingsAsync(id, Bindings.Pinned(stripped, check!.Pins, referenced), request.Message ?? "", ct);
            bus.Publish("library", new { id, commit = saved.Commit });
            var problems = (await library.GetAsync(id, ct))?.BindingProblems ?? [];
            return Results.Ok(new { id, commit = saved.Commit, problems });
        });

        // The plan a launch would make now, from the job and the bindings the library holds: nothing is written. The runner it names
        // is the one a launch would pick at this moment.
        api.MapGet("/library/{id}/plan", async (string id, PipelineLibrary library, AutoPlacer placer, DeploymentManager deployments, CancellationToken ct) =>
        {
            var doc = await library.GetAsync(id, ct);
            if (doc is null) return Results.NotFound(new { error = $"'{id}' is not in the library." });
            var launch = await deployments.PlanLaunchAsync(id, doc.Yaml, doc.Bindings, ct);
            return Results.Ok(new { plan = launch.Plan, armable = LibraryArming.Armable(launch.Plan.Deployable, doc.BindingProblems), runner = launch.Runner, bricks = placer.BrickAliases(doc.Yaml), offers = placer.Offers(doc.Yaml) });
        });
        // Arms the pipeline at the commit the library holds, with the bindings it holds. A reference no check has pinned is checked and
        // pinned first, in a commit of its own (LibraryArming). The plan a launch would make now must hold and every node must answer
        // for its fragments; a refused arming is not kept.
        designer.MapPost("/library/{id}/deploy", async (string id, HttpContext http, PipelineLibrary library, LibraryArming arming, CancellationToken ct) =>
        {
            var doc = await library.GetAsync(id, ct);
            if (doc is null) return Results.NotFound(new { error = $"'{id}' is not in the library." });
            if (doc.Commit is null) return Results.Conflict(new { error = $"'{id}' has no commit to arm." });
            return Results.Ok(await arming.ArmAsync(doc, TrustedIdp.CallerOf(http), ct));
        });
    }

    /// <summary>Why bindings cannot be written at all: an empty alias or node, or a mode that is none of the four. Null when they can.</summary>
    private static string? Refusal(IReadOnlyDictionary<string, Binding>? bindings)
    {
        if (bindings is null) return null;
        if (bindings.Any(b => string.IsNullOrWhiteSpace(b.Key) || string.IsNullOrWhiteSpace(b.Value?.Node)))
            return "A binding names a node for each branch: no empty alias, no empty node.";
        return bindings.Where(b => b.Value.Mode is not null && !BindingModes.All.Contains(b.Value.Mode))
            .Select(b => $"'{b.Key}' is bound in mode '{b.Value.Mode}': the modes are {string.Join(", ", BindingModes.All)}.").FirstOrDefault();
    }

    /// <summary>The designer's check of <paramref name="yaml"/> under <paramref name="bindings"/>: the 422 that refuses it, or the check that passed.</summary>
    private static async Task<(IResult? Refused, DesignCheck? Check)> CheckedAsync(
        DesignValidator validator, string yaml, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        DesignCheck check;
        try { check = await validator.ValidateAsync(yaml, bindings, ct); }
        catch (Exception ex) when (ex is not OperationCanceledException) { check = new DesignCheck(false, $"The job is not readable: {ex.Message}"); }
        return check.Ok ? (null, check)
            : (Results.Json(new LibraryRefused("The designer's check refused this job: nothing was saved.", check.Report),
                statusCode: StatusCodes.Status422UnprocessableEntity), null);
    }
}

internal sealed record BindingsRequest(IReadOnlyDictionary<string, Binding>? Bindings, string? Message = null);
