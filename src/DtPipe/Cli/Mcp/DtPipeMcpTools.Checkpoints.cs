using System.ComponentModel;
using System.Text.Json;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Sessions;
using ModelContextProtocol.Server;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// Reading the session store.
///
/// There is deliberately no create-checkpoint tool: materialising is an effect of running a
/// pipeline, not an action of its own. A second way to trigger the same thing is the
/// duplication this cycle removes, reintroduced at the protocol layer.
/// </summary>
public partial class DtPipeMcpTools
{
    private const int MaxCheckpointRows = 1000;

    [McpServerTool(Name = "list-checkpoints")]
    [Description("List the materialised checkpoints in a session. A checkpoint is named by a hash of what produces it — the connection, query, transformers and sampling parameters — so an unchanged pipeline prefix reuses the same one.")]
    public string ListCheckpoints(
        [Description("Session name; omit to use the one the working directory resolves to")] string? session = null)
    {
        try
        {
            var store = SessionStore.Resolve(session, WorkingDirectoryOverride);
            var checkpoints = new CheckpointStore(store);

            return JsonSerializer.Serialize(new
            {
                success = true,
                session = store.Identity.Name,
                origin = store.Identity.Origin.ToString(),
                store = store.SessionPath,
                // An empty session is a normal state, not an error: nothing has been
                // materialised yet. Returning a fault here would teach a model to treat a fresh
                // working directory as a failure.
                checkpoints = checkpoints.List()
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return Failure("list-checkpoints", ex);
        }
    }

    [McpServerTool(Name = "read-checkpoint")]
    [Description("Read rows back from a materialised checkpoint, without touching the original source.")]
    public async Task<string> ReadCheckpoint(
        [Description("The checkpoint key, as reported by list-checkpoints")] string checkpointKey,
        [Description("Rows to return (default 20, max 1000)")] int rows = 20,
        [Description("Session name; omit to use the one the working directory resolves to")] string? session = null,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(checkpointKey))
            return JsonSerializer.Serialize(new { success = false, error = "checkpointKey cannot be empty." });

        try
        {
            var store = SessionStore.Resolve(session, WorkingDirectoryOverride);
            var checkpoints = new CheckpointStore(store);

            if (!checkpoints.Contains(checkpointKey))
            {
                return JsonSerializer.Serialize(new
                {
                    success = false,
                    error = $"No checkpoint '{checkpointKey}' in session '{store.Identity.Name}'.",
                    available = checkpoints.List()
                }, new JsonSerializerOptions { WriteIndented = true });
            }

            var limit = Math.Clamp(rows, 1, MaxCheckpointRows);
            var collected = new List<List<string?>>();
            List<object> columns = new();

            await foreach (var batch in checkpoints.ReadAsync(checkpointKey, ct))
            {
                using (batch)
                {
                    if (columns.Count == 0)
                    {
                        columns = batch.Schema.FieldsList
                            .Select(f => (object)new { f.Name, type = f.DataType.Name, f.IsNullable })
                            .ToList();
                    }

                    var take = Math.Min(limit - collected.Count, batch.Length);
                    for (var i = 0; i < take; i++)
                    {
                        var row = new List<string?>(batch.Schema.FieldsList.Count);
                        for (var c = 0; c < batch.Schema.FieldsList.Count; c++)
                            row.Add(ArrowTypeMapper.GetValueForField(batch.Column(c), batch.Schema.FieldsList[c], i)?.ToString());
                        collected.Add(row);
                    }
                }

                if (collected.Count >= limit) break;
            }

            return JsonSerializer.Serialize(new
            {
                success = true,
                session = store.Identity.Name,
                checkpoint = checkpointKey,
                columns,
                rows = collected
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return Failure("read-checkpoint", ex);
        }
    }

    /// <summary>
    /// A structured failure, never a bare exception: the connection string is sanitised the way
    /// every other tool in this server does it, because an error message is an output too.
    /// </summary>
    private static string Failure(string stage, Exception ex)
        => JsonSerializer.Serialize(new
        {
            success = false,
            stage,
            error = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message)
        }, new JsonSerializerOptions { WriteIndented = true });
}
