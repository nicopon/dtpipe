namespace DtPipe.Coordinator.Server;

/// <summary>
/// What a supervisor asks (<c>/health</c>, a monitor's token, read only) and the pages read the same way (<c>/api/health</c>, a reader's
/// session): both answer from <see cref="PipelineHealth"/>, and HTTP 200 whenever the caller is admitted, the status being in the body.
/// </summary>
internal static class HealthEndpoints
{
    public static void MapHealth(this IEndpointRouteBuilder app, RouteGroupBuilder api)
    {
        var monitor = app.MapGroup("/health").RequireAuthorization(TrustedIdp.Monitor).AddEndpointFilter(NoStore);
        monitor.MapGet("/pipelines", async (PipelineHealth health, CancellationToken ct) => await health.ReadAsync(ct));
        monitor.MapGet("/pipelines/{id}", async (string id, PipelineHealth health, CancellationToken ct) => await health.ReadAsync(id, ct));

        api.MapGet("/health", async (PipelineHealth health, CancellationToken ct) => await health.ReadAsync(ct));
    }

    // A supervisor reads the state now: no intermediary keeps an answer for it.
    private static async ValueTask<object?> NoStore(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        context.HttpContext.Response.Headers.CacheControl = "no-store";
        return await next(context);
    }
}
