using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>Slot leases a route must hold for the duration of the request.</summary>
[Flags]
internal enum RouteLocks
{
    None = 0,
    Workspace = 1,
    Coordinator = 2,
    Both = Workspace | Coordinator,
}

internal readonly record struct StudioRoute(RouteLocks Locks, Func<HttpContext, Task> Handle);

/// <summary>
/// Declarative routing table for studio HTTP endpoints. Each route declares its lock requirements alongside its handler;
/// routes default to holding <see cref="RouteLocks.Workspace"/> so a newly added route cannot drop the workspace lease by omission.
/// </summary>
internal sealed class StudioRouteTable
{
    private readonly Dictionary<(string Method, string Path), StudioRoute> _routes = new();

    public void Map(string method, string path, Func<HttpContext, Task> handle, RouteLocks locks = RouteLocks.Workspace) =>
        _routes[(method, path)] = new StudioRoute(locks, handle);

    public bool TryGet(string method, string path, out StudioRoute route) =>
        _routes.TryGetValue((method, path), out route);
}

/// <summary>Shared HTTP serialization, refusal formatting, and error helpers for studio route handlers.</summary>
internal static class StudioHttp
{
    public const string NoCoordinator = "No coordinator is connected to this studio: connect one from the page.";

    public static JsonSerializerOptions Json => JsonSerializerOptions.Web;

    /// <summary>A failure as a person reads it: the cause alone (<see cref="ShownFailure"/>), without a secret.</summary>
    public static string Shown(Exception failure) => ConnectionStringSanitizer.Sanitize(ShownFailure.Of(failure));

    public static string NotAPipelineId(string? id) => $"'{id}' is not a pipeline id: lowercase letters, digits and '-'.";

    public static Task JsonAsync(HttpContext context, object value)
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        return context.Response.WriteAsync(JsonSerializer.Serialize(value, Json), context.RequestAborted);
    }

    public static async Task<T> BodyAsync<T>(HttpContext context) where T : class
    {
        if (!(context.Request.ContentType ?? "").StartsWith("application/json", StringComparison.OrdinalIgnoreCase))
            throw new StudioRefusal(415, "The body is JSON.");
        try
        {
            return await JsonSerializer.DeserializeAsync<T>(context.Request.Body, Json, context.RequestAborted)
                ?? throw new StudioRefusal(400, "The body is empty.");
        }
        catch (JsonException ex)
        {
            throw new StudioRefusal(400, $"The body is not what {context.Request.Path} reads: {ex.Message}");
        }
    }
}
