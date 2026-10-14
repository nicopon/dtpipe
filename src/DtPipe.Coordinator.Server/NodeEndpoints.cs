using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The nodes and the bricks they declare: what the pages and <c>dtpipe studio</c> read, and the two reads a brick's own node answers.
/// </summary>
internal static class NodeEndpoints
{
    public static void MapNodes(this RouteGroupBuilder api, RouteGroupBuilder designer)
    {
        api.MapGet("/nodes", (NodeInventory inventory) => inventory.Snapshot());
        api.MapGet("/bricks", (BrickCatalog bricks) => bricks.List());

        // What dtpipe studio reads of the bricks: the catalog, and what each branch of a job is. Recognition
        // is the coordinator's, by content (BrickCatalog); the studio only reads the answer back.
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
                var whole = bricks.MatchAll(branch);
                answers[alias] = new Recognition(whole.FirstOrDefault()?.Key, bricks.Contained(branch, BrickKind.Source)?.Key, bricks.Contained(branch, BrickKind.Sink)?.Key,
                    whole.Count == 0 ? null : [.. whole.Select(b => b.Node).Distinct()]);
            }
            return Results.Ok(new RecognizeResponse(answers));
        });

        // A brick is read by the node that owns it, never by the coordinator.
        designer.MapPost(CoordinatorApi.SampleRoute, async (string node, string id, int? rows, NodeRequests nodes, CancellationToken ct) =>
        {
            var bound = TimeSpan.FromSeconds(40);
            try
            {
                var sample = await nodes.AskAsync<BrickSample>(node, NodeChannel.SampleBrick, [id, Math.Clamp(rows ?? CoordinatorApi.MaxSampleRows, 1, CoordinatorApi.MaxSampleRows)], bound, ct);
                return sample.Arrow is { } arrow ? Results.Bytes(arrow, "application/vnd.apache.arrow.stream") : Results.BadRequest(new { error = sample.Error });
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return Results.Json(new { error = $"{node} did not answer within {bound.TotalSeconds:F0}s." }, statusCode: StatusCodes.Status504GatewayTimeout);
            }
        });
        designer.MapPost("/bricks/{node}/{id}/preview", async (string node, string id, NodeRequests nodes, CancellationToken ct) =>
        {
            var preview = await nodes.AskAsync<BrickPreview>(node, NodeChannel.PreviewBrick, [id, 20], TimeSpan.FromSeconds(40), ct);
            return preview.Error is null ? Results.Ok(new { csv = preview.Csv, columns = preview.Columns }) : Results.BadRequest(new { error = preview.Error });
        });
    }
}
