using System.Diagnostics;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The bricks this node offers. It reads its own databases through the dtpipe binary, with the
/// environment its fragments inherit: a source's schema comes from <c>dtpipe inspect</c>, a preview
/// from a bounded read. The coordinator never opens a brick's database for this.
/// </summary>
internal sealed class BrickHost(
    string node, IReadOnlyList<BrickDeclaration> declared, string dtpipe, string workingDirectory,
    IReadOnlyDictionary<string, string> environment, ILogger logger)
{
    private static readonly TimeSpan CommandTimeout = TimeSpan.FromSeconds(30);
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
                    var result = await RunAsync(["inspect", "--input", input, .. query is null ? Array.Empty<string>() : ["--query", query], "--format", "json"], ct);
                    if (result.ExitCode == 0) schema = JsonSerializer.Deserialize<List<BrickColumn>>(result.Stdout, Json);
                    else error = LastLines(result.Stderr);
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
            var result = await RunAsync(
                ["--input", input, .. query is null ? Array.Empty<string>() : ["--query", query], "--limit", rows.ToString(), "--output", "csv:-", "--no-stats"],
                CancellationToken.None);
            return result.ExitCode == 0 ? new BrickPreview(result.Stdout, null) : new BrickPreview(null, LastLines(result.Stderr));
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

    private async Task<(int ExitCode, string Stdout, string Stderr)> RunAsync(IEnumerable<string> args, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = dtpipe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = workingDirectory,
        };
        foreach (var (name, value) in environment) psi.Environment[name] = value;
        foreach (var arg in args) psi.ArgumentList.Add(arg);
        psi.Environment["NO_COLOR"] = "1";
        psi.Environment["DTPIPE_NO_TUI"] = "1";

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("dtpipe did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync(ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(CommandTimeout);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch { }
            throw new TimeoutException($"dtpipe did not answer within {CommandTimeout.TotalSeconds:F0}s.");
        }
        return (process.ExitCode, await stdout, await stderr);
    }

    private static string LastLines(string text) =>
        string.Join('\n', text.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).TakeLast(4));
}
