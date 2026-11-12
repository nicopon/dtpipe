using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server.Nodes;

/// <summary>
/// The nodes and the bricks they declare: what the pages and <c>dtpipe studio</c> read, and the two reads a brick's own node answers.
/// </summary>
internal static class NodeEndpoints
{
    /// <summary>The category of the log that names who read the rows of which brick.</summary>
    public const string ReadsLog = "DtPipe.Coordinator.Server.BrickReads";

    private const int PreviewRows = 20;

    public static void MapNodes(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        api.MapGet("/nodes", (NodeInventory inventory) => inventory.Snapshot());
        api.MapGet("/bricks", (BrickCatalog bricks) => bricks.List());

        // What dtpipe studio reads of the bricks: the catalog, and what each branch of a job is. A job names a brick by
        // reference (BrickRef), so recognising a branch is reading its reference against the catalog; the studio only
        // reads the answer back.
        api.MapGet(CoordinatorApi.CatalogRoute, (NodeInventory inventory, BrickCatalog bricks) => new BrickCatalogView(
            [.. inventory.Snapshot().Select(n => new CatalogNode(n.Name, n.Online, n.Role == NodeRole.Runner ? "runner" : "data", n.DtpipeVersion))],
            bricks.Catalog()));
        api.MapPost(CoordinatorApi.RecognizeRoute, (RecognizeRequest request, BrickCatalog bricks) =>
        {
            YamlDotNet.RepresentationModel.YamlMappingNode job;
            try { job = JobYaml.Parse(request.Job); }
            catch (Exception ex) { return Results.BadRequest(new { error = $"The job is not readable: {ex.Message}" }); }
            var answers = new Dictionary<string, Recognition>(StringComparer.Ordinal);
            foreach (var (alias, branch) in JobYaml.Branches(job))
            {
                var offered = BrickRef.Of(alias, branch) is { } reference && bricks.Find(reference.Key) is { } brick
                    && brick.Kind == (reference.IsSink ? BrickKind.Sink : BrickKind.Source) ? brick : null;
                answers[alias] = new Recognition(offered?.Key);
            }
            return Results.Ok(new RecognizeResponse(answers));
        });

        // A brick is read by the node that owns it, never by the coordinator. Reading the rows of any brick is the designer's
        // right, whatever groups the flow matrix gives its node: rows reach the person through the control plane, never a
        // transfer, and each read is logged under the caller's name.
        designer.MapPost(CoordinatorApi.SampleRoute, async (string node, string id, int? rows, NodeRequests nodes, HttpContext http, ILoggerFactory loggers, CancellationToken ct) =>
        {
            var bound = TimeSpan.FromSeconds(40);
            var limit = Math.Clamp(rows ?? CoordinatorApi.MaxSampleRows, 1, CoordinatorApi.MaxSampleRows);
            Logged(loggers, http, "a sample", node, id, limit);
            try
            {
                var sample = await nodes.AskAsync<BrickSample>(node, NodeChannel.SampleBrick, [id, limit], bound, ct);
                return sample.Arrow is { } arrow ? Results.Bytes(arrow, "application/vnd.apache.arrow.stream") : Results.BadRequest(new { error = sample.Error });
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return Results.Json(new { error = $"{node} did not answer within {bound.TotalSeconds:F0}s." }, statusCode: StatusCodes.Status504GatewayTimeout);
            }
        });
        designer.MapPost(CoordinatorApi.PreviewRoute, async (string node, string id, NodeRequests nodes, HttpContext http, ILoggerFactory loggers, CancellationToken ct) =>
        {
            Logged(loggers, http, "a preview", node, id, PreviewRows);
            var preview = await nodes.AskAsync<BrickPreview>(node, NodeChannel.PreviewBrick, [id, PreviewRows], TimeSpan.FromSeconds(40), ct);
            return preview.Error is null
                ? Results.Ok(new BrickPreviewView(preview.Csv, preview.Columns?.Select(c => new CatalogColumn(c.Name, c.Type, c.IsNullable)).ToList()))
                : Results.BadRequest(new { error = preview.Error });
        });
    }

    private static void Logged(ILoggerFactory loggers, HttpContext http, string read, string node, string brick, int rows) =>
        loggers.CreateLogger(ReadsLog).LogInformation("{Caller} asked {Node} for {Read} of brick {Brick}, up to {Rows} rows",
            TrustedIdp.CallerOf(http), node, read, brick, rows);
}
