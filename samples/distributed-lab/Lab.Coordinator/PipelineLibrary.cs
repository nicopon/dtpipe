using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace DtPipe.Lab.Coordinator;

/// <summary>A distributed plan as the library keeps it, with the hash of the job it was computed from.</summary>
public sealed record SavedPlan(string PipelineSha, DateTimeOffset SavedAt, LabPlan Plan);

public sealed record LibraryEntry(
    string Id, string Title, string? Description, string? Commit, DateTimeOffset? UpdatedAt, string? Message,
    bool Distributed, bool PlanCurrent);

public sealed record LibraryDocument(string Id, string Yaml, JsonObject? Layout, SavedPlan? Plan, bool PlanCurrent, string? Commit);

public sealed record LibraryCommit(string Hash, DateTimeOffset Date, string Message);

/// <summary>
/// The pipelines people keep, in a git repository of their own under <c>.state/library</c>, driven
/// through the <c>git</c> command line. One directory per pipeline: <c>pipeline.yaml</c> (the
/// logical job, runnable whole), <c>layout.json</c> (where the designer drew each card) and
/// <c>distributed/</c> (the last plan saved, with each fragment's job). Every save is a commit.
/// The repository starts from the job files under <c>library-seed/</c>.
/// </summary>
public sealed partial class PipelineLibrary
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _root;
    private readonly string _seedDir;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _ready;

    public PipelineLibrary(LabOptions options)
    {
        _root = Path.Combine(options.StateDir, "library");
        _seedDir = Path.Combine(options.LabRoot, "library-seed");
    }

    public async Task<IReadOnlyList<LibraryEntry>> ListAsync(CancellationToken ct) => await Locked(async () =>
    {
        var dir = Path.Combine(_root, "pipelines");
        var entries = new List<LibraryEntry>();
        foreach (var path in Directory.Exists(dir) ? Directory.GetDirectories(dir).Order(StringComparer.Ordinal).ToArray() : [])
        {
            var id = Path.GetFileName(path);
            var yamlPath = Path.Combine(path, "pipeline.yaml");
            if (!File.Exists(yamlPath)) continue;
            var yaml = await File.ReadAllTextAsync(yamlPath, ct);
            var (title, description, _) = DesignService.ReadHeader(yaml);
            var plan = ReadPlan(id);
            var last = (await HistoryCoreAsync(id, 1, ct)).FirstOrDefault();
            entries.Add(new LibraryEntry(id, title, description, last?.Hash, last?.Date, last?.Message, plan is not null,
                plan is not null && plan.PipelineSha == Sha(yaml)));
        }
        return entries;
    }, ct);

    public async Task<LibraryDocument?> GetAsync(string id, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) return null;
        var yaml = await File.ReadAllTextAsync(yamlPath, ct);
        var layoutPath = Path.Combine(dir, "layout.json");
        var layout = File.Exists(layoutPath) ? JsonNode.Parse(await File.ReadAllTextAsync(layoutPath, ct)) as JsonObject : null;
        var plan = ReadPlan(id);
        var last = (await HistoryCoreAsync(id, 1, ct)).FirstOrDefault();
        return new LibraryDocument(id, yaml, layout, plan, plan is not null && plan.PipelineSha == Sha(yaml), last?.Hash);
    }, ct);

    public async Task<string?> SaveAsync(string id, string yaml, JsonObject? layout, string message, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        Directory.CreateDirectory(dir);
        await File.WriteAllTextAsync(Path.Combine(dir, "pipeline.yaml"), yaml, ct);
        var layoutPath = Path.Combine(dir, "layout.json");
        if (layout is not null) await File.WriteAllTextAsync(layoutPath, layout.ToJsonString(Json), ct);
        return await CommitAsync($"pipelines/{id}", Message(message, $"Save {id}"), ct);
    }, ct);

    public async Task<string?> SavePlanAsync(string id, LabPlan plan, string message, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) throw new LabConflictException($"'{id}' is not in the library.");
        var distributed = Path.Combine(dir, "distributed");
        if (Directory.Exists(distributed)) Directory.Delete(distributed, recursive: true);
        Directory.CreateDirectory(distributed);
        var saved = new SavedPlan(Sha(await File.ReadAllTextAsync(yamlPath, ct)), DateTimeOffset.UtcNow, plan);
        await File.WriteAllTextAsync(Path.Combine(distributed, "plan.json"), JsonSerializer.Serialize(saved, Json), ct);
        foreach (var fragment in plan.Fragments)
            await File.WriteAllTextAsync(Path.Combine(distributed, fragment.Node + ".yaml"), fragment.Yaml, ct);
        return await CommitAsync($"pipelines/{id}", Message(message, $"Distribute {id}"), ct);
    }, ct);

    public async Task DeleteAsync(string id, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        if (!Directory.Exists(dir)) throw new LabConflictException($"'{id}' is not in the library.");
        Directory.Delete(dir, recursive: true);
        await CommitAsync($"pipelines/{id}", $"Delete {id}", ct);
        return true;
    }, ct);

    public async Task<IReadOnlyList<LibraryCommit>> HistoryAsync(string id, CancellationToken ct) =>
        await Locked(() => HistoryCoreAsync(id, 100, ct), ct);

    /// <summary>The job, its layout and the change a commit made to the pipeline's directory.</summary>
    public async Task<(string Yaml, JsonObject? Layout, string Diff)> AtAsync(string id, string commit, CancellationToken ct) => await Locked(async () =>
    {
        if (!CommitPattern().IsMatch(commit)) throw new LabConflictException($"'{commit}' is not a commit.");
        var yaml = await GitAsync(["show", $"{commit}:pipelines/{id}/pipeline.yaml"], ct);
        var layout = await TryGitAsync(["show", $"{commit}:pipelines/{id}/layout.json"], ct);
        var diff = await GitAsync(["show", "--format=", commit, "--", $"pipelines/{id}"], ct);
        return (yaml, layout is null ? null : JsonNode.Parse(layout) as JsonObject, diff);
    }, ct);

    private SavedPlan? ReadPlan(string id)
    {
        var path = Path.Combine(PipelineDir(id), "distributed", "plan.json");
        if (!File.Exists(path)) return null;
        try { return JsonSerializer.Deserialize<SavedPlan>(File.ReadAllText(path), Json); }
        catch (JsonException) { return null; }
    }

    private async Task<IReadOnlyList<LibraryCommit>> HistoryCoreAsync(string id, int limit, CancellationToken ct)
    {
        // An empty repository has no HEAD yet: no history rather than an error.
        var log = await TryGitAsync(["log", $"-{limit}", "--format=%H%x1f%aI%x1f%s", "--", $"pipelines/{id}"], ct) ?? "";
        return log.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split('\x1f'))
            .Where(p => p.Length == 3)
            .Select(p => new LibraryCommit(p[0], DateTimeOffset.Parse(p[1]), p[2]))
            .ToList();
    }

    private async Task<string?> CommitAsync(string path, string message, CancellationToken ct)
    {
        await GitAsync(["add", "-A", "--", path], ct);
        var staged = await GitAsync(["diff", "--cached", "--name-only"], ct);
        if (staged.Trim().Length > 0) await GitAsync(["commit", "-q", "-m", message], ct);
        return (await TryGitAsync(["rev-parse", "HEAD"], ct))?.Trim();
    }

    private async Task<T> Locked<T>(Func<Task<T>> action, CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (!_ready) await InitializeAsync(ct);
            return await action();
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        if (!Directory.Exists(Path.Combine(_root, ".git")))
        {
            Directory.CreateDirectory(_root);
            await GitAsync(["init", "-q", "-b", "main"], ct);
            foreach (var seed in Directory.Exists(_seedDir) ? Directory.GetFiles(_seedDir, "*.yaml") : [])
            {
                var dir = PipelineDir(Path.GetFileNameWithoutExtension(seed));
                Directory.CreateDirectory(dir);
                File.Copy(seed, Path.Combine(dir, "pipeline.yaml"));
            }
            await CommitAsync("pipelines", "Start the library from library-seed/", ct);
        }
        _ready = true;
    }

    private string PipelineDir(string id)
    {
        if (!IdPattern().IsMatch(id)) throw new LabConflictException($"'{id}' is not a pipeline id: lowercase letters, digits and '-'.");
        return Path.Combine(_root, "pipelines", id);
    }

    public static bool IsValidId(string id) => IdPattern().IsMatch(id);

    public static string Sha(string text) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    private static string Message(string? message, string fallback) =>
        string.IsNullOrWhiteSpace(message) ? fallback : message.Trim();

    private async Task<string?> TryGitAsync(string[] args, CancellationToken ct)
    {
        try { return await GitAsync(args, ct); }
        catch (InvalidOperationException) { return null; }
    }

    private async Task<string> GitAsync(string[] args, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "git",
            WorkingDirectory = _root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        // A fixed identity, and nothing from the dtpipe repository that encloses .state/.
        foreach (var arg in new[] { "-c", "user.name=lab", "-c", "user.email=lab@localhost", "-c", "commit.gpgsign=false", "-c", "core.quotepath=false" })
            psi.ArgumentList.Add(arg);
        foreach (var arg in args) psi.ArgumentList.Add(arg);
        psi.Environment.Remove("GIT_DIR");
        psi.Environment.Remove("GIT_WORK_TREE");
        psi.Environment["GIT_CEILING_DIRECTORIES"] = Path.GetDirectoryName(_root)!;

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("git did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync(ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"git {string.Join(' ', args)}: {(await stderr).Trim()}");
        return await stdout;
    }

    [GeneratedRegex("^[a-z0-9][a-z0-9-]{0,62}$")]
    private static partial Regex IdPattern();

    [GeneratedRegex("^[0-9a-f]{7,40}$")]
    private static partial Regex CommitPattern();
}
