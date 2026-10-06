using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The bricks this node offers. It reads its own databases through the dtpipe binary, with the
/// environment its fragments inherit: a source's schema comes from <c>dtpipe inspect</c>, a preview
/// from a bounded read. The coordinator never opens a brick's database for this.
/// </summary>
internal sealed class BrickHost(string node, IReadOnlyList<BrickDeclaration> declared, DtPipeCommand dtpipe, ILogger logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public IReadOnlyList<BrickInfo> Bricks { get; private set; } = [];

    /// <summary>What every announced brick is, for <see cref="BrickRules"/>; set once with <see cref="Bricks"/>.</summary>
    public IReadOnlyList<BrickForm> Forms { get; private set; } = [];

    public async Task InspectAsync(CancellationToken ct)
    {
        var bricks = new List<BrickInfo>();
        foreach (var brick in declared)
        {
            // A brick this node would refuse to run must not be offered: the designer would place it here.
            if (BrickRules.DeclaredRefusal(brick.Id, brick.BranchJson) is { } refusal)
            {
                logger.LogWarning("Brick {Brick} is not offered: {Reason}", brick.Id, refusal);
                continue;
            }
            IReadOnlyList<BrickColumn>? schema = null;
            string? error = null;
            if (brick.Kind == BrickKind.Source)
            {
                try
                {
                    var (input, query) = SourceRead(brick);
                    var result = await dtpipe.RunAsync(["inspect", "--input", input, .. query is null ? Array.Empty<string>() : ["--query", query], "--format", "json"], ct);
                    if (result.Succeeded) schema = JsonSerializer.Deserialize<List<BrickColumn>>(result.Stdout, Json);
                    else error = result.Diagnostic;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    error = ex.Message;
                }
                if (error is not null) logger.LogWarning("Brick {Brick}: schema not inspected: {Error}", brick.Id, error);
            }
            bricks.Add(new BrickInfo(brick.Id, brick.Kind, brick.Title, brick.Description ?? "", brick.BranchJson, schema, error));
        }
        Bricks = bricks;
        Forms = bricks.Select(b => BrickRules.Declared(b.Kind, b.Branch)).ToList();
    }

    /// <summary>A source's first rows, or what a sink's table holds now.</summary>
    public async Task<BrickPreview> PreviewAsync(string id, int rows)
    {
        var brick = declared.FirstOrDefault(b => b.Id == id);
        if (brick is null) return new BrickPreview(null, $"{node} offers no brick '{id}'.");
        try
        {
            string input;
            string? query;
            if (brick.Kind == BrickKind.Source) (input, query) = SourceRead(brick);
            else
            {
                var branch = Branch(brick);
                input = Scalar(branch, "output") ?? throw new InvalidOperationException("The sink names no output.");
                var table = ProviderOption(branch, "-writer", "table") ?? throw new InvalidOperationException("The sink names no table.");
                query = $"SELECT * FROM {table}";
            }
            var result = await dtpipe.RunAsync(
                ["--input", input, .. query is null ? Array.Empty<string>() : ["--query", query], "--limit", rows.ToString(), "--output", "csv:-", "--no-stats"],
                CancellationToken.None);
            return result.Succeeded ? new BrickPreview(result.Stdout, null) : new BrickPreview(null, result.Diagnostic);
        }
        catch (Exception ex)
        {
            return new BrickPreview(null, ex.Message);
        }
    }

    private static (string Input, string? Query) SourceRead(BrickDeclaration brick)
    {
        var branch = Branch(brick);
        return (Scalar(branch, "input") ?? throw new InvalidOperationException("The source names no input."),
                ProviderOption(branch, "-reader", "query"));
    }

    private static JsonElement Branch(BrickDeclaration brick) => JsonDocument.Parse(brick.BranchJson).RootElement.Clone();

    private static string? Scalar(JsonElement branch, string key) =>
        branch.TryGetProperty(key, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    /// <summary>A setting of the first provider-options component whose name ends with <paramref name="suffix"/>.</summary>
    private static string? ProviderOption(JsonElement branch, string suffix, string key)
    {
        if (!branch.TryGetProperty("provider-options", out var components) || components.ValueKind != JsonValueKind.Object) return null;
        foreach (var component in components.EnumerateObject())
        {
            if (component.Name.EndsWith(suffix, StringComparison.Ordinal) && component.Value.ValueKind == JsonValueKind.Object
                && component.Value.TryGetProperty(key, out var value))
                return value.ToString();
        }
        return null;
    }
}
