using System.Text.RegularExpressions;
using System.Text.Json.Serialization;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// What a scheduler asks (<c>/service</c>, a launcher's token): start the run of an armed pipeline its identity lists, read the
/// state of the runs it can start, and cancel one it started. Outside <c>/api</c>, like <c>/health</c>: a service's token is bearer only, so no cookie and no
/// CSRF header apply, and the contract <c>dtpipe studio</c> reads is untouched. The coordinator answers the run's id and is asked
/// again for its state; it calls nothing back.
/// </summary>
internal static partial class ServiceEndpoints
{
    /// <summary>The longest key a scheduler may send; the key is written to the journal and shown on the pages.</summary>
    private const int MaxKeyLength = 128;

    [GeneratedRegex("^[A-Za-z0-9._:-]+$")]
    private static partial Regex KeyPattern();

    public static void MapService(this IEndpointRouteBuilder app)
    {
        var service = app.MapGroup("/service").RequireAuthorization(TrustedIdp.Launcher).AddEndpointFilter(NoStore);

        // The checks run in this order so that a refusal reveals nothing: what is malformed, then what the identity may name at all
        // (before the coordinator says whether the pipeline exists or is armed), then the key, then the queue's own refusals (409).
        service.MapPost("/runs", (ServiceRunRequest? request, HttpContext http, RightsStore rights, DeploymentManager deployments) =>
        {
            if (string.IsNullOrWhiteSpace(request?.PipelineId))
                return Results.BadRequest(new { error = "Name the pipeline to launch: {\"pipelineId\": \"…\"}." });
            var pipelineId = request.PipelineId.Trim();
            string? key = null;
            if (http.Request.Headers.TryGetValue("Idempotency-Key", out var sent))
            {
                key = sent.ToString();
                if (key.Length is 0 or > MaxKeyLength || !KeyPattern().IsMatch(key))
                    return Results.BadRequest(new { error = $"Idempotency-Key is 1 to {MaxKeyLength} letters, digits, '.', '_', ':' or '-'." });
            }
            var caller = TrustedIdp.SubjectOf(http.User)!;
            if (rights.Find(caller) is not { } identity || !identity.Launches(pipelineId))
                return Results.Json(new { error = $"'{caller}' may not launch '{pipelineId}'." }, statusCode: StatusCodes.Status403Forbidden);

            var launched = deployments.Launch(pipelineId, caller, key);
            var body = ServiceRun.Of(launched.Run);
            return launched.Replayed ? Results.Ok(body) : Results.Json(body, statusCode: StatusCodes.Status202Accepted);
        });

        // A run outside the identity's list is not told apart from a run that does not exist.
        service.MapGet("/runs/{id}", (string id, HttpContext http, RightsStore rights, DeploymentManager deployments) =>
            ReadableRun(id, http, rights, deployments) is { } run
                ? Results.Ok(ServiceRun.Of(run))
                : Results.NotFound(new { error = $"'{id}' is not a run this identity may read." }));

        // A launcher cancels the runs it started, not another's: a run another identity or a person started is told apart from none by
        // nothing. A run already over answers its state (200); otherwise the cancellation is asked (202) and the state read again.
        service.MapPost("/runs/{id}/cancel", (string id, HttpContext http, RightsStore rights, DeploymentManager deployments) =>
        {
            if (ReadableRun(id, http, rights, deployments) is not { } run || run.LaunchedBy != TrustedIdp.SubjectOf(http.User))
                return Results.NotFound(new { error = $"'{id}' is not a run this identity started." });
            if (run.IsOver) return Results.Ok(ServiceRun.Of(run));
            deployments.Cancel(id);
            return Results.Json(ServiceRun.Of(deployments.FindRun(id) ?? run), statusCode: StatusCodes.Status202Accepted);
        });
    }

    private static RunView? ReadableRun(string id, HttpContext http, RightsStore rights, DeploymentManager deployments) =>
        deployments.FindRun(id) is { PipelineId: { } pipelineId } run && rights.Find(TrustedIdp.SubjectOf(http.User)) is { } identity
            && identity.Launches(pipelineId) ? run : null;

    // A scheduler reads state now: no intermediary keeps an answer.
    private static async ValueTask<object?> NoStore(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        context.HttpContext.Response.Headers.CacheControl = "no-store";
        return await next(context);
    }
}

internal sealed record ServiceRunRequest(string? PipelineId);

/// <summary>
/// What a scheduler reads of a run: its id and state, and whether its sinks may be partial (<see cref="RunView.SinkMayBePartial"/>),
/// never the plan, the nodes or the replicas.
/// </summary>
internal sealed record ServiceRun(
    string RunId, string PipelineId, [property: JsonConverter(typeof(WireEnumConverter<RunState>))] RunState State,
    DateTimeOffset? QueuedAt, DateTimeOffset? StartedAt, DateTimeOffset? FinishedAt, string? Description, string? Refusal, string? LaunchedBy,
    bool SinkMayBePartial)
{
    public static ServiceRun Of(RunView run) => new(
        run.RunId, run.PipelineId!, run.State, run.QueuedAt, run.State == RunState.Queued ? null : run.StartedAt, run.FinishedAt,
        run.Description, run.Refusal, run.LaunchedBy, run.SinkMayBePartial);
}
