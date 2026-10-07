using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Microsoft.Extensions.Logging;
using YamlDotNet.RepresentationModel;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The bricks this node offers. It reads its own databases through the dtpipe binary, with the
/// environment its fragments inherit: a source's schema comes from <c>dtpipe inspect</c>, a preview
/// and a sample from a bounded read, a sink's verdict on a contract from <c>dtpipe contract check</c>.
/// The coordinator never opens a brick's database for this.
/// </summary>
internal sealed class BrickHost(string node, IReadOnlyList<BrickDeclaration> declared, DtPipeCommand dtpipe, ILogger logger)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public IReadOnlyList<BrickInfo> Bricks { get; private set; } = [];

    /// <summary>The branch of the announced brick <paramref name="id"/> as its owner declared it, or <c>null</c> when none of that kind is offered.</summary>
    public YamlMappingNode? DeclaredBranch(string id, BrickKind kind) =>
        Bricks.FirstOrDefault(b => b.Id == id && b.Kind == kind) is { } brick ? (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(brick.Branch)) : null;

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

    /// <summary>
    /// A source brick's first rows, read by the brick's own branch sent to <c>arrow:-</c>, exactly as
    /// its fragment sends the runner: the same reader and writer, so the coordinator checks a job
    /// against the schema the runner will receive. A real read bounded by <c>--limit</c>, like a
    /// preview, not a sample run: the source-side guarantees of <c>--dry-run</c> do not apply.
    /// </summary>
    public async Task<BrickSample> SampleAsync(string id, int rows)
    {
        var brick = Bricks.FirstOrDefault(b => b.Id == id && b.Kind == BrickKind.Source);
        if (brick is null) return new BrickSample(null, $"{node} offers no source brick '{id}'.");
        try
        {
            var result = await RunBranchAsync(brick, "output", new Dictionary<string, string>(),
                job => ["--job", job, "--limit", rows.ToString(), "--no-stats"]);
            if (!result.Succeeded) return new BrickSample(null, result.Diagnostic);
            return result.Output.Length > MaxSampleBytes
                ? new BrickSample(null, $"{node}: the first {rows} row(s) of '{id}' take {result.Output.Length:N0} bytes, over the {MaxSampleBytes:N0} a sample may.")
                : new BrickSample(result.Output, null);
        }
        catch (Exception ex)
        {
            return new BrickSample(null, ex.Message);
        }
    }

    /// <summary>
    /// Whether a sink brick's target accepts a contract: <c>dtpipe contract check</c> on the brick fed
    /// from <c>arrow:-</c>, which inspects the target and writes nothing.
    /// </summary>
    public async Task<SinkCheck> CheckSinkAsync(string id, string contractJson)
    {
        var brick = Bricks.FirstOrDefault(b => b.Id == id && b.Kind == BrickKind.Sink);
        if (brick is null) return new SinkCheck(false, $"{node} offers no sink brick '{id}'.");
        try
        {
            var result = await RunBranchAsync(brick, "input", new Dictionary<string, string> { ["contract.json"] = contractJson },
                job => ["contract", "check", "--job", job, "--contract", Path.Combine(Path.GetDirectoryName(job)!, "contract.json")]);
            return new SinkCheck(result.Succeeded, Findings(result.Stderr) ?? result.Tail(12));
        }
        catch (Exception ex)
        {
            return new SinkCheck(false, ex.Message);
        }
    }

    /// <summary>dtpipe's findings, from its first <c>error:</c> or <c>warning:</c> line on, without the table above them.</summary>
    private static string? Findings(string stderr)
    {
        var lines = stderr.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).ToList();
        var first = lines.FindIndex(l => l.StartsWith("error:", StringComparison.Ordinal) || l.StartsWith("warning:", StringComparison.Ordinal));
        return first < 0 ? null : string.Join('\n', lines.Skip(first));
    }

    // A sample crosses the node channel as one message.
    private const int MaxSampleBytes = 256 * 1024;

    /// <summary>
    /// Runs dtpipe on a job holding the brick's branch alone, its <paramref name="endpoint"/> set to
    /// <c>arrow:-</c>, in a directory of its own with <paramref name="files"/> beside the job.
    /// </summary>
    private async Task<DtPipeCommandResult> RunBranchAsync(
        BrickInfo brick, string endpoint, IReadOnlyDictionary<string, string> files, Func<string, string[]> args)
    {
        var dir = Path.Combine(dtpipe.WorkingDirectory, "bricks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var branch = (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(brick.Branch));
            JobYaml.Set(branch, endpoint, BrickRules.ArrowEndpoint);
            var job = Path.Combine(dir, "job.yaml");
            await File.WriteAllTextAsync(job, JobYaml.Serialize(new YamlMappingNode { { "brick", branch } }));
            foreach (var (name, content) in files) await File.WriteAllTextAsync(Path.Combine(dir, name), content);
            return await dtpipe.RunAsync(args(job), CancellationToken.None);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
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
