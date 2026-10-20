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
/// <see cref="Commit"/> is the last commit of the job file, the version a later <c>If-Match</c> names.
/// <see cref="BindingProblems"/>: why the pipeline's bindings do not hold now, empty when they do.
/// </summary>
public sealed record LibraryEntry(
    string Id, string Title, string? Description, string? Commit, DateTimeOffset? UpdatedAt, string? Message,
    IReadOnlyList<BindingProblem> BindingProblems);

/// <summary>
/// <see cref="Commit"/> and <see cref="BindingProblems"/> are those of <see cref="LibraryEntry"/>.
/// <see cref="Bindings"/> binds each brick branch to a node and a brick, by alias.
/// </summary>
public sealed record LibraryDocument(
    string Id, string Yaml, string? Commit, IReadOnlyDictionary<string, Binding> Bindings, IReadOnlyList<BindingProblem> BindingProblems);

public sealed record LibraryCommit(string Hash, DateTimeOffset Date, string Message);

/// <summary>
/// What a save must find in place: no pipeline (<see cref="Absent"/>), or the pipeline's job at its last
/// commit <see cref="Commit"/>. Bindings saved since do not change the job's version: only an edit does.
/// </summary>
public sealed record SavePrecondition(bool Absent, string? Commit);

/// <summary>
/// A save's outcome. <see cref="Commit"/> is the last commit of the pipeline's job, which an unchanged
/// text leaves as it was. <see cref="Changed"/>: the text differs from the one before. <see cref="Bindings"/>: the bindings
/// the library holds after the save.
/// </summary>
public sealed record SaveOutcome(string? Commit, bool Changed, IReadOnlyDictionary<string, Binding> Bindings);

/// <summary>A save whose <see cref="SavePrecondition"/> does not hold, with the pipeline's commit in place, if any.</summary>
public sealed class PreconditionFailedException(string message, string? commit) : Exception(message)
{
    public string? Commit { get; } = commit;
}

/// <summary>
/// The pipelines people keep, in a git repository of their own under <c>library/</c> in the state directory, driven
/// through the <c>git</c> command line. One directory per pipeline: <c>pipeline.yaml</c> (the
/// logical job, runnable whole) and <c>bindings.json</c> (the node and brick of each brick branch, see <see cref="Bindings"/>).
/// Every save is a commit.
/// The repository starts from the job files under <c>library-seed/</c>, each with the <c>.bindings.json</c> beside it, if any.
/// With a <see cref="LibraryRemoteSettings"/> the remote is the authority: the library is read from it when it opens, every write
/// first aligns the library on it and ends with a push, and a write whose push fails is undone, on disk and in history, so the
/// library never holds what its remote does not. Without one, the state directory holds the only copy.
/// </summary>
public sealed partial class PipelineLibrary : IJobSource, ILibraryHeads
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>Where a fetch puts the remote's <c>main</c>: a private ref, so nothing about the remote is written to the repository's configuration.</summary>
    private const string TrackingRef = "refs/remotes/remote/main";

    private static readonly CancellationToken NoCancel = CancellationToken.None;

    private readonly string _root;
    private readonly string? _seedDir;
    private readonly LibraryRemoteSettings? _remote;
    private readonly bool _mayBegin;
    private readonly ILogger? _log;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private volatile bool _ready;

    private readonly Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyList<BindingProblem>> _bindingProblems;
    private readonly Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyDictionary<string, Binding>> _resolveBricks;

    /// <param name="options">Where the library lives.</param>
    /// <param name="bindingProblems">Why bindings do not hold for a job now; with none given, nothing is wrong.</param>
    /// <param name="resolveBricks">The bindings with the brick each one lacks filled in from the job; with none given, they are kept as they are.</param>
    /// <param name="logger">Where the remote's own words go, after the token is taken out: they are never in a response.</param>
    public PipelineLibrary(CoordinatorServerOptions options,
        Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyList<BindingProblem>>? bindingProblems = null,
        Func<string, IReadOnlyDictionary<string, Binding>, IReadOnlyDictionary<string, Binding>>? resolveBricks = null,
        ILogger? logger = null)
    {
        _root = options.LibraryDirectory ?? Path.Combine(options.StateDir, "library");
        _seedDir = options.LibrarySeed;
        (_remote, _mayBegin, _log) = (options.LibraryRemote, options.Initialize, logger);
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
            var bindings = ReadBindings(id);
            var last = (await HistoryCoreAsync(id, 1, ct)).FirstOrDefault();
            entries.Add(new LibraryEntry(id, title, description, await JobCommitAsync(id, ct), last?.Date, last?.Message,
                _bindingProblems(yaml, bindings)));
        }
        return entries;
    }, ct);

    public async Task<LibraryDocument?> GetAsync(string id, CancellationToken ct) => await Locked(async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) return null;
        var yaml = await File.ReadAllTextAsync(yamlPath, ct);
        var bindings = ReadBindings(id);
        return new LibraryDocument(id, yaml, await JobCommitAsync(id, ct), bindings, _bindingProblems(yaml, bindings));
    }, ct);

    /// <summary>
    /// Writes the job and commits
    /// it. A <paramref name="precondition"/> is checked under the library's lock, so no other save
    /// slips between the check and the write. The <paramref name="bindings"/> are set into the ones the library keeps, entry by
    /// entry, in the same commit (an entry with no mode keeps the library's); null leaves them as they are. Every binding the save does not carry a brick for has it
    /// resolved from the job it now holds, under the same lock, so a binding never keeps the brick of a text that was edited.
    /// </summary>
    public async Task<SaveOutcome> SaveAsync(string id, string yaml, string message, CancellationToken ct,
        SavePrecondition? precondition = null, IReadOnlyDictionary<string, Binding>? bindings = null) => await Written(id, async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        var before = File.Exists(yamlPath) ? await File.ReadAllTextAsync(yamlPath, NoCancel) : null;
        if (precondition is not null)
        {
            var current = before is null ? null : await JobCommitAsync(id, NoCancel);
            if (precondition.Absent && before is not null)
                throw new PreconditionFailedException($"'{id}' is already in the library.", current);
            if (precondition.Commit is { } expected && (before is null || current != expected))
                throw new PreconditionFailedException(before is null
                    ? $"'{id}' is not in the library."
                    : $"'{id}' changed since {Short(expected)}: the library holds {Short(current)}.", current);
        }
        Directory.CreateDirectory(dir);
        await File.WriteAllTextAsync(yamlPath, yaml, NoCancel);
        var kept = ReadBindings(id);
        // A strict binding re-reads its brick from the text it now holds; one in another mode keeps its brick by id.
        var merged = kept.ToDictionary(b => b.Key, b => Bindings.IsStrict(b.Value) ? b.Value with { Brick = null } : b.Value, StringComparer.Ordinal);
        foreach (var (alias, binding) in bindings ?? new Dictionary<string, Binding>())
            merged[alias] = binding with { Mode = binding.Mode ?? kept.GetValueOrDefault(alias)?.Mode };
        var resolved = new Dictionary<string, Binding>(_resolveBricks(yaml, merged), StringComparer.Ordinal);
        foreach (var (alias, old) in kept)
            if (resolved.TryGetValue(alias, out var binding) && binding.Brick is null) resolved[alias] = binding with { Brick = old.Brick };
        if (Bindings.Serialize(resolved) != Bindings.Serialize(kept) && (resolved.Count > 0 || kept.Count > 0))
        {
            kept = resolved;
            await WriteBindingsAsync(dir, kept, NoCancel);
        }
        await CommitAsync($"pipelines/{id}", Message(message, $"Save {id}"), NoCancel);
        return new SaveOutcome(await JobCommitAsync(id, NoCancel), before != yaml, kept);
    }, ct);

    private static string Short(string? commit) => commit is null ? "nothing" : commit[..Math.Min(8, commit.Length)];

    /// <summary>The outcome of a bindings save: the commit it made.</summary>
    public sealed record BindingsSaved(string? Commit);

    /// <summary>
    /// Replaces the bindings of the pipeline's job and commits them, each lacking brick resolved from the job as the library holds it.
    /// What they name is kept as given, a node that is absent included: it is listed as a problem of the pipeline, not refused, so a
    /// binding can be written before its node exists.
    /// </summary>
    public async Task<BindingsSaved> SaveBindingsAsync(string id, IReadOnlyDictionary<string, Binding> bindings, string message, CancellationToken ct) => await Written(id, async () =>
    {
        var dir = PipelineDir(id);
        var yamlPath = Path.Combine(dir, "pipeline.yaml");
        if (!File.Exists(yamlPath)) throw new ConflictException($"'{id}' is not in the library.");
        var resolved = _resolveBricks(await File.ReadAllTextAsync(yamlPath, NoCancel), bindings);
        await WriteBindingsAsync(dir, resolved, NoCancel);
        return new BindingsSaved(await CommitAsync($"pipelines/{id}", Message(message, $"Bind {id}"), NoCancel));
    }, ct);

    private static async Task WriteBindingsAsync(string dir, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        var path = Path.Combine(dir, BindingsFile);
        if (bindings.Count == 0) File.Delete(path);
        else await File.WriteAllTextAsync(path, Bindings.Serialize(bindings), ct);
    }

    public async Task DeleteAsync(string id, CancellationToken ct) => await Written(id, async () =>
    {
        var dir = PipelineDir(id);
        if (!Directory.Exists(dir)) throw new ConflictException($"'{id}' is not in the library.");
        Directory.Delete(dir, recursive: true);
        await CommitAsync($"pipelines/{id}", $"Delete {id}", NoCancel);
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

    /// <summary>
    /// The job as a commit holds it: a commit is immutable, so what is read here never changes, and a library that is open is read
    /// without its lock. A launch asks for this, and so does not wait for a save, nor for the remote a save is pushing to.
    /// </summary>
    public async Task<string?> JobAtAsync(string id, string commit, CancellationToken ct)
    {
        if (!CommitPattern().IsMatch(commit)) return null;
        if (!_ready) await Locked(() => Task.FromResult(true), ct);
        return await TryGitAsync(["show", $"{commit}:pipelines/{PipelineName(id)}/pipeline.yaml"], ct);
    }

    /// <summary>
    /// Opens the library now instead of at its first use: with a remote, reads it from there. A remote that cannot be
    /// reached, one that has diverged from the library, or one that holds nothing to begin from fails here.
    /// </summary>
    public async Task OpenAsync(CancellationToken ct) => await Locked(() => Task.FromResult(true), ct);

    /// <summary>What the library holds of a pipeline now, by content; null when it holds none.</summary>
    public async Task<LibraryHead?> HeadAsync(string id, CancellationToken ct) =>
        await GetAsync(id, ct) is { } doc ? new LibraryHead(doc.Commit, Sha(doc.Yaml), Bindings.Sha(doc.Bindings)) : null;

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

    /// <summary>
    /// A write to the pipeline <paramref name="id"/>, under the lock: aligned on the remote first (so a remote that cannot be
    /// reached refuses it before anything is written), then <paramref name="write"/>, then a push when it committed. Whatever
    /// fails from the write on puts the files and the history back as they were. The write and everything after run on no
    /// token: a caller that hangs up cannot leave a commit behind that nothing would undo.
    /// </summary>
    private async Task<T> Written<T>(string id, Func<Task<T>> write, CancellationToken ct)
    {
        var pathspec = $"pipelines/{PipelineName(id)}";
        await _lock.WaitAsync(ct);
        try
        {
            if (!_ready) await InitializeAsync(ct);
            if (_remote is not null) await SyncAsync(ct);
            var before = await CurrentCommitAsync();
            T result;
            try
            {
                result = await write();
                if (_remote is not null && await CurrentCommitAsync() != before) await PushAsync();
            }
            catch
            {
                await UndoAsync(before!, pathspec);
                throw;
            }
            return result;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Puts the library back at <paramref name="commit"/> with nothing of <paramref name="pathspec"/> left over (a pipeline that
    /// was new has no commit to go back to). A library that cannot be put back is opened again, and so aligned on its remote, by the next call.
    /// </summary>
    private async Task UndoAsync(string commit, string pathspec)
    {
        try
        {
            await GitAsync(["reset", "-q", "--hard", commit], NoCancel);
            await GitAsync(["clean", "-fdq", "--", pathspec], NoCancel);
        }
        catch (InvalidOperationException ex)
        {
            if (_remote is not null) _ready = false;
            _log?.LogError(ex, "The library could not be put back after a write that failed: {Reason}", ex.Message);
        }
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        Directory.CreateDirectory(_root);
        if (!Directory.Exists(Path.Combine(_root, ".git"))) await GitAsync(["init", "-q", "-b", "main"], ct);
        var remoteHead = _remote is null ? null : await SyncAsync(ct);
        if (await CurrentCommitAsync() is null)
        {
            if (_remote is not null && remoteHead is null && !_mayBegin)
                throw new InvalidOperationException(
                    $"The library's remote ({_remote.Url}) holds no library and this coordinator has none: set Coordinator:Initialize to true to begin one there.");
            var seeds = _seedDir is not null && Directory.Exists(_seedDir) ? Directory.GetFiles(_seedDir, "*.yaml") : [];
            foreach (var seed in seeds)
            {
                var dir = PipelineDir(Path.GetFileNameWithoutExtension(seed));
                Directory.CreateDirectory(dir);
                File.Copy(seed, Path.Combine(dir, "pipeline.yaml"));
                var bindings = Path.ChangeExtension(seed, ".bindings.json");
                if (File.Exists(bindings)) File.Copy(bindings, Path.Combine(dir, BindingsFile));
            }
            // The first commit is always there, so a write has a commit to go back to.
            if (seeds.Length > 0) await CommitAsync("pipelines", $"Start the library from {Path.GetFileName(_seedDir)}/", ct);
            else await GitAsync(["commit", "-q", "--allow-empty", "-m", "Start the library"], ct);
        }
        else
        {
            // A start after a stop in the middle of a write finds files nothing committed: they are not the library.
            await GitAsync(["reset", "-q", "--hard"], ct);
            await GitAsync(["clean", "-fdq", "--", "pipelines"], ct);
        }
        if (_remote is not null && await CurrentCommitAsync() != remoteHead) await PushAsync();
        _ready = true;
    }

    private async Task<string?> CurrentCommitAsync() =>
        (await TryGitAsync(["rev-parse", "--verify", "-q", "HEAD"], NoCancel))?.Trim() is { Length: > 0 } hash ? hash : null;

    /// <summary>
    /// Fetches the remote's <c>main</c> and puts the library on it: the remote is the authority. A library behind it moves forward, and
    /// one that holds commits the remote does not (a stop between a commit and its push, or a remote put back) drops them, each named in
    /// the log; a history that has parted from the remote's is not chosen between. Returns the remote's commit, null when the remote has none.
    /// </summary>
    private async Task<string?> SyncAsync(CancellationToken ct)
    {
        var remote = await FetchAsync(ct);
        var local = await CurrentCommitAsync();
        if (remote is null || remote == local) return remote;
        if (local is not null && !await IsAncestorAsync(local, remote))
        {
            if (!await IsAncestorAsync(remote, local))
                throw new StoreUnavailableException(
                    $"The library ({Short(local)}) and its remote ({_remote!.Url}, {Short(remote)}) have each commits the other lacks: " +
                    "restore one from the other by hand (removing library/ from the state directory takes the remote's).");
            _log?.LogWarning("The library is ahead of its remote and follows it, dropping: {Dropped}",
                (await GitAsync(["log", "--format=%h %s", $"{remote}..{local}"], NoCancel)).Trim().Replace('\n', ';'));
        }
        await GitAsync(["reset", "-q", "--hard", remote], NoCancel);
        return remote;
    }

    private async Task<bool> IsAncestorAsync(string ancestor, string descendant) =>
        (await RunAsync(["merge-base", "--is-ancestor", ancestor, descendant], NoCancel)).ExitCode == 0;

    /// <summary>The remote's <c>main</c>, fetched; null when the remote has no such branch. A remote that cannot be asked refuses.</summary>
    private async Task<string?> FetchAsync(CancellationToken ct)
    {
        var remote = _remote!;
        // A fetch killed in the middle may leave its lock; nothing else uses this ref, and this lock is held.
        var stale = Path.Combine(_root, ".git", "refs", "remotes", "remote", "main.lock");
        if (File.Exists(stale)) File.Delete(stale);
        var fetch = await RunAsync(["fetch", "-q", "--no-tags", remote.Url, $"+refs/heads/main:{TrackingRef}"], ct, remote: true);
        if (fetch.ExitCode == 0) return (await GitAsync(["rev-parse", "--verify", TrackingRef], NoCancel)).Trim();
        // A remote with no main fails the fetch too: ls-remote tells an absent branch (exit 2) from a remote that cannot be asked.
        var probe = fetch.TimedOut ? fetch : await RunAsync(["ls-remote", "--exit-code", remote.Url, "refs/heads/main"], ct, remote: true);
        if (probe.ExitCode == 2)
        {
            await TryGitAsync(["update-ref", "-d", TrackingRef], NoCancel);
            return null;
        }
        Report("fetch", probe.TimedOut ? fetch : probe);
        throw new StoreUnavailableException(probe.TimedOut
            ? $"The library's remote ({remote.Url}) did not answer within {remote.Timeout.TotalSeconds:0.#} s, so nothing was saved."
            : $"The library's remote ({remote.Url}) could not be reached, or refused the credentials, so nothing was saved. The coordinator's log has git's reason.");
    }

    /// <summary>Pushes the library's <c>main</c> to the remote, never forcing: a remote that has moved refuses.</summary>
    private async Task PushAsync()
    {
        var remote = _remote!;
        var push = await RunAsync(["push", "--porcelain", remote.Url, "HEAD:refs/heads/main"], NoCancel, remote: true);
        if (push.ExitCode == 0) return;
        Report("push", push);
        // A line starting with '!' is the remote answering about the ref: a hook, a right or a history it refuses. Nothing was kept there.
        if (push.Output.Split('\n').Any(line => line.StartsWith('!')))
            throw new StoreUnavailableException($"The library's remote ({remote.Url}) refused the save, so nothing was saved. The coordinator's log has the remote's reason.");
        throw new StoreUnavailableException(
            (push.TimedOut
                ? $"The library's remote ({remote.Url}) did not answer within {remote.Timeout.TotalSeconds:0.#} s"
                : $"The library's remote ({remote.Url}) could not be reached, or refused the credentials")
            + ": the save is undone here, but it may have reached the remote, and then shows here at the next save or start. The coordinator's log has git's reason.");
    }

    /// <summary>What git said about a call to the remote, for the operator's log with the token taken out.</summary>
    private void Report(string operation, GitResult result) =>
        _log?.LogWarning("The library's remote refused or failed a {Operation} (exit {Exit}{Timeout}): {Detail}",
            operation, result.ExitCode, result.TimedOut ? ", timed out" : "", Scrub(result.Error.Trim() + " " + result.Output.Trim()));

    private string Scrub(string text)
    {
        var remote = _remote!;
        if (remote.Token is { } token)
            text = text.Replace(token, "***").Replace(Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{remote.User}:{token}")), "***");
        return text.Length > 1000 ? text[..1000] + "…" : text;
    }

    private string PipelineDir(string id) => Path.Combine(_root, "pipelines", PipelineName(id));

    private static string PipelineName(string id) => CoordinatorApi.IsPipelineId(id)
        ? id
        : throw new ConflictException($"'{id}' is not a pipeline id: lowercase letters, digits and '-'.");

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
        var result = await RunAsync(args, ct);
        if (result.ExitCode != 0)
            throw new InvalidOperationException($"git {string.Join(' ', args)}: {result.Error.Trim()}");
        return result.Output;
    }

    private sealed record GitResult(int ExitCode, string Output, string Error, bool TimedOut = false);

    /// <summary>
    /// Runs git. A call to the <paramref name="remote"/> is bounded by its timeout, which ends the process and the ones it started, and
    /// carries the token; its words are the caller's to report, never an exception's. A local call is not cut: a git ended in the middle
    /// would leave its locks.
    /// </summary>
    private async Task<GitResult> RunAsync(string[] args, CancellationToken ct, bool remote = false)
    {
        using var process = Process.Start(Prepare(args, remote)) ?? throw new InvalidOperationException("git did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        if (!remote)
        {
            await process.WaitForExitAsync(ct);
            return new GitResult(process.ExitCode, await stdout, await stderr);
        }
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(_remote!.Timeout);
        try
        {
            await process.WaitForExitAsync(limit.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            await process.WaitForExitAsync(NoCancel);
            // A process the kill missed may still hold the pipes: what git said so far is enough.
            await Task.WhenAny(Task.WhenAll(stdout, stderr), Task.Delay(TimeSpan.FromSeconds(2), NoCancel));
            if (ct.IsCancellationRequested) throw;
            return new GitResult(-1, stdout.IsCompletedSuccessfully ? stdout.Result : "", stderr.IsCompletedSuccessfully ? stderr.Result : "", TimedOut: true);
        }
        return new GitResult(process.ExitCode, await stdout, await stderr);
    }

    /// <summary>The git process for <paramref name="args"/>: this is the one place the token enters, in the environment of a call to the remote and nowhere else.</summary>
    internal ProcessStartInfo Prepare(string[] args, bool remote)
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
        // No maintenance: git would detach one after each commit, writing into the repository beyond the call.
        // The library is this server's own: git refuses a repository that another user owns, which a mounted volume often is.
        foreach (var arg in new[] { "-c", $"safe.directory={_root}", "-c", "user.name=dtpipe-coordinator", "-c", "user.email=coordinator@localhost", "-c", "commit.gpgsign=false", "-c", "core.quotepath=false", "-c", "gc.auto=0", "-c", "maintenance.auto=false" })
            psi.ArgumentList.Add(arg);
        if (remote)
            // Never a prompt, never a helper's stored credential, and a transfer that stalls ends on its own before the timeout does.
            foreach (var arg in new[] { "-c", "credential.helper=", "-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=10" })
                psi.ArgumentList.Add(arg);
        foreach (var arg in args) psi.ArgumentList.Add(arg);
        psi.Environment.Remove("GIT_DIR");
        psi.Environment.Remove("GIT_WORK_TREE");
        psi.Environment["GIT_CEILING_DIRECTORIES"] = Path.GetDirectoryName(_root)!;
        // The coordinator's own settings (the token, the Redis connection string, the pages' client secret) are in its environment, which a child
        // inherits: git, and a remote's hooks when the remote is a local path, have no use for them.
        foreach (var name in psi.Environment.Keys.Where(k => k.StartsWith("Coordinator__", StringComparison.OrdinalIgnoreCase)).ToList())
            psi.Environment.Remove(name);
        if (!remote) return psi;
        // What traces the wire would show the header.
        foreach (var name in new[] { "GIT_TRACE", "GIT_TRACE_CURL", "GIT_TRACE_PACKET", "GIT_CURL_VERBOSE", "GIT_ASKPASS", "SSH_ASKPASS" })
            psi.Environment.Remove(name);
        psi.Environment["GIT_TERMINAL_PROMPT"] = "0";
        if (_remote!.Token is { } token)
        {
            // In the environment of this process alone: not in its arguments, which `ps` lists, and not in the repository's configuration.
            // Scoped to the remote's URL, so a redirect elsewhere is not sent it.
            psi.Environment.Remove("GIT_CONFIG_PARAMETERS");
            psi.Environment["GIT_CONFIG_COUNT"] = "1";
            psi.Environment["GIT_CONFIG_KEY_0"] = $"http.{_remote.Url}.extraHeader";
            psi.Environment["GIT_CONFIG_VALUE_0"] =
                "Authorization: Basic " + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{_remote.User}:{token}"));
        }
        return psi;
    }

    [GeneratedRegex("^[0-9a-f]{7,40}$")]
    private static partial Regex CommitPattern();
}
