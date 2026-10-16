using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// A distributed plan as the library keeps it, with the hash of the job it was computed from and that of the bindings
/// (<see cref="Bindings.Sha"/>: null for none).
/// </summary>
public sealed record SavedPlan(string PipelineSha, DateTimeOffset SavedAt, DistributionPlan Plan, string? BindingsSha = null);

/// <summary>
/// <see cref="PlanStaleCause"/> says why a saved plan is not current: <see cref="PlanStaleCauses.Job"/>,
/// <see cref="PlanStaleCauses.Bindings"/> or <see cref="PlanStaleCauses.Groups"/>; null when there is no plan or it is current.
/// <see cref="BindingProblems"/>: why the pipeline's bindings do not hold now, empty when they do.
/// </summary>
public sealed record LibraryEntry(
    string Id, string Title, string? Description, string? Commit, DateTimeOffset? UpdatedAt, string? Message,
    bool Distributed, bool PlanCurrent, IReadOnlyList<BindingProblem> BindingProblems, string? PlanStaleCause = null);

/// <summary>
/// <see cref="Commit"/> is the last commit of the job file, the version a later <c>If-Match</c> names.
/// <see cref="PlanStaleCause"/> and <see cref="BindingProblems"/> are those of <see cref="LibraryEntry"/>.
/// <see cref="Bindings"/> binds each brick branch to a node and a brick, by alias.
/// </summary>
public sealed record LibraryDocument(
    string Id, string Yaml, SavedPlan? Plan, bool PlanCurrent, string? Commit, IReadOnlyDictionary<string, Binding> Bindings,
    IReadOnlyList<BindingProblem> BindingProblems, string? PlanStaleCause = null);

/// <summary>Why a saved plan is not the one a new distribution would make.</summary>
public static class PlanStaleCauses
{
    /// <summary>The job changed after the plan was saved.</summary>
    public const string Job = "job";

    /// <summary>The bindings of the job's bricks changed after the plan was saved.</summary>
    public const string Bindings = "bindings";

    /// <summary>A node the plan places a fragment on holds other groups now, or has no enabled identity.</summary>
    public const string Groups = "groups";
}

public sealed record LibraryCommit(string Hash, DateTimeOffset Date, string Message);

/// <summary>
/// What a save must find in place: no pipeline (<see cref="Absent"/>), or the pipeline's job at its last
/// commit <see cref="Commit"/>. A plan saved since does not change the job's version: only an edit does.
/// </summary>
public sealed record SavePrecondition(bool Absent, string? Commit);

/// <summary>
/// A save's outcome. <see cref="Commit"/> is the last commit of the pipeline's job, which an unchanged
/// text leaves as it was. <see cref="Changed"/>: the text differs from the one before. <see cref="PlanStale"/>:
/// a plan is saved for the pipeline and no longer matches its text. <see cref="PlanId"/>: that plan's id.
/// </summary>
public sealed record SaveOutcome(string? Commit, bool Changed, bool PlanStale, string? PlanId);

/// <summary>A save whose <see cref="SavePrecondition"/> does not hold, with the pipeline's commit in place, if any.</summary>
public sealed class PreconditionFailedException(string message, string? commit) : Exception(message)
{
    public string? Commit { get; } = commit;
}

/// <summary>
/// The pipelines people keep, in a git repository of their own under <c>library/</c> in the state directory, driven
/// through the <c>git</c> command line. One directory per pipeline: <c>pipeline.yaml</c> (the
/// logical job, runnable whole), <c>bindings.json</c> (the node and brick of each brick branch, see <see cref="Bindings"/>) and
/// <c>distributed/</c> (the last plan saved, with each fragment's job). Every save is a commit.
/// The repository starts from the job files under <c>library-seed/</c>, each with the <c>.bindings.json</c> beside it, if any.
/// </summary>
public sealed partial class PipelineLibrary
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _root;
    private readonly string? _seedDir;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _ready;

    private readonly Func<string, IReadOnlyList<string>?> _groupsOfNode;
    private readonly Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyList<BindingProblem>> _bindingProblems;
    private readonly Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyDictionary<string, Binding>> _resolveBricks;

    /// <param name="options">Where the library lives.</param>
    /// <param name="groupsOfNode">The groups the rights give a node now, null for a node with no identity or a disabled one.</param>
    /// <param name="bindingProblems">Why bindings do not hold for a job now; with none given, nothing is wrong.</param>
    /// <param name="resolveBricks">The bindings with the brick each one lacks filled in from the job; with none given, they are kept as they are.</param>
    public PipelineLibrary(CoordinatorServerOptions options, Func<string, IReadOnlyList<string>?> groupsOfNode,
        Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyList<BindingProblem>>? bindingProblems = null,
        Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyDictionary<string, Binding>>? resolveBricks = null)
    {
        _root = Path.Combine(options.StateDir, "library");
        _seedDir = options.LibrarySeed;
        _groupsOfNode = groupsOfNode;
        _bindingProblems = bindingProblems ?? ((_, _) => []);
        _resolveBricks = resolveBricks ?? ((_, bindings) => bindings);
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
            var bindings = ReadBindings(id);
            var last = (await HistoryCoreAsync(id, 1, ct)).FirstOrDefault();
            var cause = StaleCause(plan, yaml, bindings);
            entries.Add(new LibraryEntry(id, title, description, await JobCommitAsync(id, ct), last?.Date, last?.Message, plan is not null,
                plan is not null && cause is null, _bindingProblems(yaml, bindings), cause));
        }
        return entries;
    }, ct);

    public async Task<LibraryDocument?> GetAsync(string id, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) return null;
        var yaml = await File.ReadAllTextAsync(yamlPath, ct);
        var plan = ReadPlan(id);
        var bindings = ReadBindings(id);
        var cause = StaleCause(plan, yaml, bindings);
        return new LibraryDocument(id, yaml, plan, plan is not null && cause is null, await JobCommitAsync(id, ct), bindings,
            _bindingProblems(yaml, bindings), cause);
    }, ct);

    /// <summary>
    /// Writes the job and commits
    /// it. A <paramref name="precondition"/> is checked under the library's lock, so no other save
    /// slips between the check and the write. The <paramref name="bindings"/> are set into the ones the library keeps, entry by
    /// entry, in the same commit; null leaves them as they are. Every binding the save does not carry a brick for has it
    /// resolved from the job it now holds, under the same lock, so a binding never keeps the brick of a text that was edited.
    /// </summary>
    public async Task<SaveOutcome> SaveAsync(string id, string yaml, string message, CancellationToken ct,
        SavePrecondition? precondition = null, IReadOnlyDictionary<string, Binding>? bindings = null) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        var before = File.Exists(yamlPath) ? await File.ReadAllTextAsync(yamlPath, ct) : null;
        if (precondition is not null)
        {
            var current = before is null ? null : await JobCommitAsync(id, ct);
            if (precondition.Absent && before is not null)
                throw new PreconditionFailedException($"'{id}' is already in the library.", current);
            if (precondition.Commit is { } expected && (before is null || current != expected))
                throw new PreconditionFailedException(before is null
                    ? $"'{id}' is not in the library."
                    : $"'{id}' changed since {Short(expected)}: the library holds {Short(current)}.", current);
        }
        Directory.CreateDirectory(dir);
        await File.WriteAllTextAsync(yamlPath, yaml, ct);
        var kept = ReadBindings(id);
        var merged = kept.ToDictionary(b => b.Key, b => b.Value with { Brick = null }, StringComparer.Ordinal);
        foreach (var (alias, binding) in bindings ?? new Dictionary<string, Binding>()) merged[alias] = binding;
        var resolved = new Dictionary<string, Binding>(_resolveBricks(yaml, merged), StringComparer.Ordinal);
        foreach (var (alias, old) in kept)
            if (resolved.TryGetValue(alias, out var binding) && binding.Brick is null) resolved[alias] = binding with { Brick = old.Brick };
        if (Bindings.Serialize(resolved) != Bindings.Serialize(kept) && (resolved.Count > 0 || kept.Count > 0))
        {
            kept = resolved;
            await WriteBindingsAsync(dir, kept, ct);
        }
        await CommitAsync($"pipelines/{id}", Message(message, $"Save {id}"), ct);
        var plan = ReadPlan(id);
        return new SaveOutcome(await JobCommitAsync(id, ct), before != yaml,
            plan is not null && (plan.PipelineSha != Sha(yaml) || plan.BindingsSha != Bindings.Sha(kept)), plan?.Plan.Id);
    }, ct);

    /// <summary>
    /// Why a saved plan is not the one a new distribution would make, or null when it is (or there is none): the job changed
    /// since the plan was made (<see cref="PlanStaleCauses.Job"/>), its bindings changed (<see cref="PlanStaleCauses.Bindings"/>),
    /// or a node the plan places a fragment on holds, now, other groups than the plan names, compared as sets, or has no
    /// enabled identity (<see cref="PlanStaleCauses.Groups"/>).
    /// </summary>
    private string? StaleCause(SavedPlan? plan, string yaml, IReadOnlyDictionary<string, Binding> bindings) =>
        plan is null ? null
        : plan.PipelineSha != Sha(yaml) ? PlanStaleCauses.Job
        : plan.BindingsSha != Bindings.Sha(bindings) ? PlanStaleCauses.Bindings
        : plan.Plan.Fragments.All(f => _groupsOfNode(f.Node) is { } now && now.ToHashSet(StringComparer.Ordinal).SetEquals(f.Groups)) ? null
        : PlanStaleCauses.Groups;

    private static string Short(string? commit) => commit is null ? "nothing" : commit[..Math.Min(8, commit.Length)];

    public async Task<string?> SavePlanAsync(string id, DistributionPlan plan, string message, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) throw new ConflictException($"'{id}' is not in the library.");
        var distributed = Path.Combine(dir, "distributed");
        if (Directory.Exists(distributed)) Directory.Delete(distributed, recursive: true);
        Directory.CreateDirectory(distributed);
        var saved = new SavedPlan(Sha(await File.ReadAllTextAsync(yamlPath, ct)), DateTimeOffset.UtcNow, plan, Bindings.Sha(ReadBindings(id)));
        await File.WriteAllTextAsync(Path.Combine(distributed, "plan.json"), JsonSerializer.Serialize(saved, Json), ct);
        foreach (var fragment in plan.Fragments)
            await File.WriteAllTextAsync(Path.Combine(distributed, fragment.Node + ".yaml"), fragment.Yaml, ct);
        return await CommitAsync($"pipelines/{id}", Message(message, $"Distribute {id}"), ct);
    }, ct);

    /// <summary>The outcome of a bindings save: the commit it made, and whether the plan saved for the pipeline no longer matches it.</summary>
    public sealed record BindingsSaved(string? Commit, bool PlanStale);

    /// <summary>
    /// Replaces the bindings of the pipeline's job and commits them, each lacking brick resolved from the job as the library holds it.
    /// What they name is kept as given, a node that is absent included: it is listed as a problem of the pipeline, not refused, so a
    /// binding can be written before its node exists.
    /// </summary>
    public async Task<BindingsSaved> SaveBindingsAsync(string id, IReadOnlyDictionary<string, Binding> bindings, string message, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) throw new ConflictException($"'{id}' is not in the library.");
        var resolved = _resolveBricks(await File.ReadAllTextAsync(yamlPath, ct), bindings);
        await WriteBindingsAsync(dir, resolved, ct);
        var commit = await CommitAsync($"pipelines/{id}", Message(message, $"Bind {id}"), ct);
        var plan = ReadPlan(id);
        return new BindingsSaved(commit, plan is not null && plan.BindingsSha != Bindings.Sha(resolved));
    }, ct);

    private static async Task WriteBindingsAsync(string dir, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        var path = Path.Combine(dir, BindingsFile);
        if (bindings.Count == 0) File.Delete(path);
        else await File.WriteAllTextAsync(path, Bindings.Serialize(bindings), ct);
    }

    public async Task DeleteAsync(string id, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        if (!Directory.Exists(dir)) throw new ConflictException($"'{id}' is not in the library.");
        Directory.Delete(dir, recursive: true);
        await CommitAsync($"pipelines/{id}", $"Delete {id}", ct);
        return true;
    }, ct);

    public async Task<IReadOnlyList<LibraryCommit>> HistoryAsync(string id, CancellationToken ct) =>
        await Locked(() => HistoryCoreAsync(id, 100, ct), ct);

    /// <summary>The job and the change a commit made to the pipeline's directory.</summary>
    public async Task<(string Yaml, string Diff)> AtAsync(string id, string commit, CancellationToken ct) => await Locked(async () =>
    {
        if (!CommitPattern().IsMatch(commit)) throw new ConflictException($"'{commit}' is not a commit.");
        var yaml = await GitAsync(["show", $"{commit}:pipelines/{id}/pipeline.yaml"], ct);
        var diff = await GitAsync(["show", "--format=", commit, "--", $"pipelines/{id}"], ct);
        return (yaml, diff);
    }, ct);

    private const string BindingsFile = "bindings.json";

    private IReadOnlyDictionary<string, Binding> ReadBindings(string id)
    {
        var path = Path.Combine(PipelineDir(id), BindingsFile);
        if (!File.Exists(path)) return new Dictionary<string, Binding>();
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, Binding>>(File.ReadAllText(path), Json) ?? new Dictionary<string, Binding>();
        }
        catch (JsonException) { return new Dictionary<string, Binding>(); }
    }

    private SavedPlan? ReadPlan(string id)
    {
        var path = Path.Combine(PipelineDir(id), "distributed", "plan.json");
        if (!File.Exists(path)) return null;
        try
        {
            return JsonSerializer.Deserialize<SavedPlan>(File.ReadAllText(path), Json);
        }
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

    /// <summary>The last commit that changed the pipeline's job, whatever was saved beside it since.</summary>
    private async Task<string?> JobCommitAsync(string id, CancellationToken ct) =>
        (await TryGitAsync(["log", "-1", "--format=%H", "--", $"pipelines/{id}/pipeline.yaml"], ct))?.Trim() is { Length: > 0 } hash ? hash : null;

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
            var seeds = _seedDir is not null && Directory.Exists(_seedDir) ? Directory.GetFiles(_seedDir, "*.yaml") : [];
            foreach (var seed in seeds)
            {
                var dir = PipelineDir(Path.GetFileNameWithoutExtension(seed));
                Directory.CreateDirectory(dir);
                File.Copy(seed, Path.Combine(dir, "pipeline.yaml"));
                var bindings = Path.ChangeExtension(seed, ".bindings.json");
                if (File.Exists(bindings)) File.Copy(bindings, Path.Combine(dir, BindingsFile));
            }
            // An empty library has nothing to commit: its first save is its first commit.
            if (seeds.Length > 0) await CommitAsync("pipelines", $"Start the library from {Path.GetFileName(_seedDir)}/", ct);
        }
        await RemoveLayoutsAsync(ct);
        _ready = true;
    }

    /// <summary>
    /// A layout.json is where a coordinator's designer drew each card; no designer remains, so a library
    /// that still tracks any drops them all in one commit. Idempotent: with none tracked, it commits nothing.
    /// </summary>
    private async Task RemoveLayoutsAsync(CancellationToken ct)
    {
        var tracked = (await TryGitAsync(["ls-files", "-z", "--", ":(glob)pipelines/*/layout.json"], ct) ?? "")
            .Split('\0', StringSplitOptions.RemoveEmptyEntries);
        if (tracked.Length == 0) return;
        await GitAsync(["rm", "-q", "--", .. tracked], ct);
        await GitAsync(["commit", "-q", "-m", "Remove the designer's layouts"], ct);
    }

    private string PipelineDir(string id)
    {
        if (!CoordinatorApi.IsPipelineId(id)) throw new ConflictException($"'{id}' is not a pipeline id: lowercase letters, digits and '-'.");
        return Path.Combine(_root, "pipelines", id);
    }

    /// <summary>The library's ids, which the rights reuse for client ids and node names: <see cref="CoordinatorApi.IsPipelineId"/>.</summary>
    public static bool IsValidId(string id) => CoordinatorApi.IsPipelineId(id);

    public static string Sha(string text) => ContentHash.Of(text);

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
        // A fixed identity, and nothing from any repository that encloses the state directory.
        foreach (var arg in new[] { "-c", "user.name=dtpipe-coordinator", "-c", "user.email=coordinator@localhost", "-c", "commit.gpgsign=false", "-c", "core.quotepath=false" })
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

    [GeneratedRegex("^[0-9a-f]{7,40}$")]
    private static partial Regex CommitPattern();
}
