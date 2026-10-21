using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Split;
using DtPipe.Configuration;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using DtPipe.Sessions;
using Microsoft.AspNetCore.Http;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The studio's real run of the job saved in the folder. One engine action runs at a time: a run holds the
/// same slot as a sample (409 otherwise). What runs is the saved file, read again and checked against the
/// hash the page opened; the page confirms what the run would write first, and the run is refused when the
/// job or the values it launches with are no longer what was confirmed.
/// </summary>
public sealed partial class StudioServer
{
    /// <summary>The verdict of a run its person stopped.</summary>
    public const int Stopped = 130;

    private readonly object _runLock = new();
    private RunState? _run;

    private sealed class RunState(string path, CancellationTokenSource cts)
    {
        public string Path { get; } = path;
        public CancellationTokenSource Cts { get; } = cts;
        public ConcurrentDictionary<string, string> Failures { get; } = new(StringComparer.Ordinal);
        public bool Running { get; set; } = true;
        public int? Code { get; set; }
        public bool StopRequested { get; set; }
        public string? Error { get; set; }
        public DateTimeOffset Started { get; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? Finished { get; set; }
        public Task Task { get; set; } = Task.CompletedTask;

        /// <summary>What a staged run holds when it ends (<c>DebugView</c>), for the page to redraw its cards.</summary>
        public object? View { get; set; }
    }

    private sealed record RunRequest(string? Path, string? Hash, List<SampledBrick>? Bricks, string? Digest, bool? Debug = null, bool? Review = null);

    private sealed record RunWrite(string Alias, bool Throwaway, string? Output, string? Block);

    private sealed record RunPlan(
        string Path, BrickSampling Sampling, IReadOnlyDictionary<string, string> Launch, IReadOnlyList<RunWrite> Writes, string Digest,
        StagedRun? Staged = null);

    /// <summary>
    /// A real run cut at the cards the person watches: the job to run, the levels it may read back, and what it
    /// tells the person before they confirm. <see cref="Drop"/> are the checkpoints the run no longer relies on. A
    /// <see cref="Review"/> stops every branch that writes just before its writer: <see cref="Holds"/> are those branches' ends,
    /// kept for the run and never pinned.
    /// </summary>
    private sealed record StagedRun(
        DebugPlan Plan, DebugState State, bool Review, List<DebugWatch> Holds, List<DebugWatch> Watch, List<HeldLevel> Keep, HashSet<string> Drop,
        DebugCut Cut, object Stages, object Held, int Discards, string Signature);

    private bool RunInProgress
    {
        get { lock (_runLock) return _run is { Running: true }; }
    }

    /// <summary>
    /// What a run of the saved job would do, or the refusal that says why it cannot: the job as saved
    /// (its hash is the page's), no source brick (its rows would recreate a real target), every variable
    /// resolved. The digest binds the confirmation to this text and these values, by name.
    /// </summary>
    private RunPlan PlanRun(RunRequest asked)
    {
        var path = JobPath(asked.Path) ?? throw new StudioRefusal(400, "Run a job saved in the folder: name its path.");
        var job = _folder.ReadJob(path);
        if (asked.Hash is null || asked.Hash != job.Hash)
            throw new StudioRefusal(409, $"{path} is not the job saved last: save it, then run.");
        var sampling = BrickSampling.Of(job.Text, asked.Bricks, _tools.Coordinator?.Catalog);
        if (sampling.Sources.Count > 0)
            throw new StudioRefusal(409, "A run is refused here for a job that reads a brick: "
                + string.Join(", ", sampling.Sources.Select(s => $"'{s.Alias}' ({s.Brick.Key})"))
                + ". Its rows would recreate a real target on this machine. Sample it, or run it on its node.");
        RefuseReferences(sampling.Yaml);
        var launch = _variables.LaunchEnvironment(path, sampling.Yaml);
        var staged = asked.Debug == true || asked.Review == true;
        // A suspended run has its rows; any run but its continuation would write the job again and leave them to be written twice.
        if (!staged && LoadDebug(DebugKey(path, null)).Real)
            throw new StudioRefusal(409, "A real run is suspended before its targets are written: write it or abandon it from the sink cards first.");

        var writes = new List<RunWrite>();
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(sampling.Yaml)))
        {
            if (JobYaml.Scalar(branch, "output") is not { } output) continue;
            if (sampling.Sinks.Contains(alias)) { writes.Add(new RunWrite(alias, true, null, null)); continue; }
            // The writer's block as the file holds it, its options neither read nor judged, but for the
            // connection string, which is redacted as one before the block is text.
            var block = new YamlMappingNode { { "output", ConnectionStringSanitizer.Redact(output) } };
            foreach (var key in BranchSides.Write)
                if (branch.Children.TryGetValue(new YamlScalarNode(key), out var value)) block.Add(key, value);
            if (branch.Children.TryGetValue(new YamlScalarNode("provider-options"), out var options)) block.Add("provider-options", options);
            writes.Add(new RunWrite(alias, false, ConnectionStringSanitizer.Redact(output), ConnectionStringSanitizer.Sanitize(JobYaml.Serialize(block))));
        }

        var stagedRun = staged ? Stage(asked, path, job.Text, sampling, launch) : null;

        var material = new StringBuilder(job.Text).Append('\0');
        foreach (var (name, value) in launch.OrderBy(p => p.Key, StringComparer.Ordinal)) material.Append(name).Append('=').Append(value).Append('\0');
        foreach (var alias in sampling.Sinks.Order(StringComparer.Ordinal)) material.Append("throwaway:").Append(alias).Append('\0');
        if (stagedRun is not null) material.Append("staged:").Append(stagedRun.Signature).Append('\0');
        var digest = Convert.ToHexStringLower(HMACSHA256.HashData(_token, Encoding.UTF8.GetBytes(material.ToString())));
        return new RunPlan(path, sampling, launch, writes, digest, stagedRun);
    }

    /// <summary>
    /// The saved job cut at the watchers it was left with, its levels judged against the text and the values it would
    /// run with, and the stages that would run, told before anything does. Pure: nothing leaves the session until
    /// the run starts. The levels of a real run are keyed to every row, never to a sample's bound.
    /// </summary>
    private StagedRun Stage(RunRequest asked, string path, string text, BrickSampling sampling, IReadOnlyDictionary<string, string> launch)
    {
        var cutter = _tools.Engine.Cutter;
        var key = DebugKey(path, null);
        _local.CheckDebug(key);
        var review = asked.Review == true;
        var plan = PlanOf(cutter, text, 0, RealSalt(text, launch));
        var state = LoadDebug(key);
        if (state.Real && review)
            throw new StudioRefusal(409, "A real run is suspended before its targets are written: write it or abandon it before reviewing another.");
        var (watch, keep, drop) = Judge(state, plan, []);

        // A suspended run is continued whole or not at all: a target that holds nothing would be read from its source and written unreviewed.
        if (state.Real)
        {
            var missing = plan.Branches.Where(b => b.Writes && !keep.Any(h => Same(h.Branch, b.Branch) && h.Level == b.Transformers)).Select(b => b.Branch).ToList();
            if (missing.Count > 0)
                throw new StudioRefusal(409, $"The suspended run does not hold the rows of every target ({string.Join(", ", missing)}): abandon it, then review again.");
        }

        DebugCut cut;
        try { cut = cutter.Cut(sampling.Yaml, watch, null, keep.Select(l => new DebugHeld(l.Branch, l.Level, l.Key)).ToList(), write: true, beforeWriters: review); }
        catch (InvalidOperationException ex) { throw new StudioRefusal(422, ex.Message); }
        var holds = review ? plan.Branches.Where(b => b.Writes).Select(b => new DebugWatch(b.Branch, DebugAt.End)).ToList() : [];

        var stages = JobFileParser.ParseContent(cut.Yaml, null, interpolate: false).Select(kv => new
        {
            branch = kv.Key.Split(DebugCutter.Mark, 2)[0],
            reads = kv.Value.FromCheckpoint is not null ? "its checkpoint"
                : kv.Value.Input is { Length: > 0 } input ? ConnectionStringSanitizer.Redact(input)
                : kv.Value.From,
            keeps = !string.IsNullOrEmpty(kv.Value.Checkpoint),
            writes = kv.Value.Output is { Length: > 0 } output && output != "null:" ? ConnectionStringSanitizer.Redact(output) : null,
        }).ToList();

        var store = DebugStore();
        var held = keep.Select(h =>
        {
            var file = new FileInfo(store.PathFor(h.Key));
            var at = watch.FirstOrDefault(w => Same(w.Branch, h.Branch) && plan.Resolve(w) == h.Level);
            return new { branch = h.Branch, card = at?.Card, at = at is null ? null : AtName(at.At), bytes = file.Exists ? file.Length : 0L, readAt = file.Exists ? (DateTime?)file.LastWriteTimeUtc : null };
        }).ToList();

        var signature = string.Join('|', new[] { $"review={(review ? 1 : 0)}" }
            .Concat(watch.Select(w => $"w:{w.Branch}/{AtName(w.At)}").Order(StringComparer.Ordinal))
            .Concat(keep.Select(h => $"h:{h.Branch}/{h.Level}/{h.Key}").Order(StringComparer.Ordinal)));
        return new StagedRun(plan, state, review, holds, watch, keep, drop, cut, stages, held, state.Levels.Count - keep.Count, signature);
    }

    /// <summary>What a held level was read with, hashed under the session's token: the launch values and the state files a cursor reads.</summary>
    private string RealSalt(string text, IReadOnlyDictionary<string, string> launch)
    {
        var material = new StringBuilder();
        foreach (var (name, value) in launch.OrderBy(p => p.Key, StringComparer.Ordinal)) material.Append(name).Append('=').Append(value).Append('\0');
        var states = Regex.Matches(text, @"cursor://([^|}\s""']+)").Select(m => m.Groups[1].Value)
            .Concat(Regex.Matches(text, @"^\s*state:\s*[""']?([^\s""']+)", RegexOptions.Multiline).Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal);
        foreach (var path in states)
        {
            string content;
            try { content = File.Exists(path) ? Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))) : "-"; }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { content = "?"; }
            material.Append("state:").Append(path).Append('=').Append(content).Append('\0');
        }
        return Convert.ToHexStringLower(HMACSHA256.HashData(_token, Encoding.UTF8.GetBytes(material.ToString())));
    }

    private async Task PrepareRunAsync(HttpContext context)
    {
        var plan = PlanRun(await BodyAsync<RunRequest>(context));
        await JsonAsync(context, new
        {
            digest = plan.Digest,
            writes = plan.Writes,
            staged = plan.Staged is not { } staged ? null : new
            {
                stages = staged.Stages,
                held = staged.Held,
                discards = staged.Discards,
                holds = staged.Holds.Select(h => h.Branch),
                writesNow = !staged.Review,
            },
        });
    }

    private async Task StartRunAsync(HttpContext context)
    {
        var asked = await BodyAsync<RunRequest>(context);
        var plan = PlanRun(asked);
        const string Changed = "The job or the values it launches with changed since you confirmed: confirm what it writes again.";
        if (asked.Digest != plan.Digest) throw new StudioRefusal(409, Changed);
        if (!await _sampling.WaitAsync(0))
            throw new StudioRefusal(409, "An action is already running: wait for it to finish.");

        var files = new List<string>();
        try
        {
            // What the levels hold may have moved while the slot was awaited: the confirmation is of what is held now.
            if (plan.Staged is not null)
            {
                plan = PlanRun(asked);
                if (asked.Digest != plan.Digest) throw new StudioRefusal(409, Changed);
            }
            // A sink brick is written to a throwaway file here, never to its node, as a sample does.
            var outputs = new List<string>();
            foreach (var alias in plan.Sampling.Sinks)
            {
                if (plan.Staged?.Cut.Dropped.Contains(alias) == true) continue;
                files.Add(_local.SampleFile(null));
                outputs.Add($"{alias}={Bindable(files[^1])}");
            }
            var bindings = new StudioBindings(null, outputs.Count > 0 ? string.Join(',', outputs) : null);
            if (plan.Staged is { } staged)
            {
                // The session keeps what the cut relies on, and nothing else: a run of every row is not a sample's.
                var store = DebugStore();
                foreach (var gone in staged.Drop) ForgetCheckpoint(store, gone);
                staged.State.Watch = staged.Watch;
                staged.State.Levels = staged.Keep;
                staged.State.Real = staged.Keep.Count > 0;
                SaveDebug(DebugKey(plan.Path, null), staged.State);
            }
            var state = new RunState(plan.Path, new CancellationTokenSource());
            lock (_runLock) _run = state;
            state.Task = Task.Run(() => RunAsync(plan, bindings, state, files));
        }
        catch
        {
            foreach (var file in files) File.Delete(file);
            _sampling.Release();
            throw;
        }
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        await JsonAsync(context, new { started = true });
    }

    private async Task RunAsync(RunPlan plan, StudioBindings bindings, RunState state, List<string> files)
    {
        var code = 1;
        string? error = null;
        IReadOnlyDictionary<string, string> checkpoints = new Dictionary<string, string>();
        try
        {
            // Armed inside the task, for this run's call chain alone; the environment is never written.
            using var armed = LaunchValues.Arm(plan.Launch);
            using var quiet = NonInteractiveGuard.Suppress();
            var outcome = await _tools.Engine.RunAsync(plan.Staged?.Cut.Yaml ?? plan.Sampling.Yaml, bindings, state.Cts.Token);
            code = outcome.ExitCode;
            checkpoints = outcome.Checkpoints;
            foreach (var (alias, reason) in outcome.Failures) state.Failures[alias] = reason;
            // The engine answers a fault and a cancellation alike as a fault: a stop asked decides the verdict below.
            if (outcome.Fault is not null && outcome.Fault is not OperationCanceledException)
                error = ConnectionStringSanitizer.Sanitize(outcome.Fault.Message);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { error = ConnectionStringSanitizer.Sanitize(ex.Message); }
        finally
        {
            foreach (var file in files) { try { File.Delete(file); } catch (IOException) { } }
            // What the run kept is settled before it reads as finished: the page redraws its cards from it.
            if (plan.Staged is { } staged)
            {
                bool stopped;
                lock (_runLock) stopped = state.StopRequested;
                try { state.View = await FinishStagedAsync(plan.Path, staged, checkpoints, stopped ? Stopped : code); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    error ??= "The checkpoints of this run could not be settled: " + ConnectionStringSanitizer.Sanitize(ex.Message);
                }
            }
            lock (_runLock)
            {
                // A stop asked while the run had not finished is its verdict, whatever the engine returned:
                // a linear job throws on cancellation and a DAG answers 1, and neither may read as success.
                state.Code = state.StopRequested ? Stopped : code;
                if (state.StopRequested) error = null;
                state.Error = error;
                state.Running = false;
                state.Finished = DateTimeOffset.UtcNow;
            }
            _sampling.Release();
        }
    }

    /// <summary>
    /// Settles what a staged run kept. A run that reached the end of the job and wrote it consumes the levels that fed it:
    /// they are a snapshot of one moment, and the next run reads the source again. A run that stopped, failed or was
    /// stopped keeps what it completed, suspended until it is continued or abandoned.
    /// </summary>
    private async Task<object> FinishStagedAsync(string path, StagedRun staged, IReadOnlyDictionary<string, string> keys, int code)
    {
        var key = DebugKey(path, null);
        var state = staged.State;
        // The ends a review held are kept for this run and are no pin: they leave the watchers before the state is saved.
        var pins = state.Watch;
        state.Watch = pins.Concat(staged.Holds.Where(h => !pins.Any(p => Same(p.Branch, h.Branch) && p.At == h.At))).ToList();
        Keep(key, state, staged.Plan, staged.Cut, keys);
        state.Watch = pins;
        if (code == 0 && !staged.Review)
        {
            var store = DebugStore();
            foreach (var held in state.Levels) ForgetCheckpoint(store, held.Key);
            state.Levels.Clear();
        }
        state.Real = state.Levels.Count > 0;
        if (staged.Review)
        {
            // Nothing on disk says how many rows a level holds, and the page tells the person how many wait: counted once, here.
            var store = DebugStore();
            for (var i = 0; i < state.Levels.Count; i++)
            {
                var held = state.Levels[i];
                if (staged.Plan.Branches.Any(b => b.Writes && Same(b.Branch, held.Branch) && b.Transformers == held.Level))
                    state.Levels[i] = held with { Rows = await CountRowsAsync(store, held.Key) };
            }
        }
        SaveDebug(key, state);
        return DebugView(staged.Plan, state, staged.Cut.Pieces);
    }

    private static async Task<long?> CountRowsAsync(CheckpointStore store, string key)
    {
        long rows = 0;
        try
        {
            await foreach (var batch in store.ReadAsync(key))
                using (batch) rows += batch.Length;
        }
        // The count only feeds the display, and this runs before the engine slot is released: whatever a checkpoint throws, it is unknown.
        catch (Exception) { return null; }
        return rows;
    }

    private object RunStatus()
    {
        lock (_runLock)
        {
            if (_run is not { } run) return new { running = false, started = false };
            return new
            {
                started = true,
                running = run.Running,
                path = run.Path,
                code = run.Code,
                stopRequested = run.StopRequested,
                error = run.Error,
                failures = run.Failures.ToDictionary(f => f.Key, f => ConnectionStringSanitizer.Sanitize(f.Value)),
                startedAt = run.Started,
                finishedAt = run.Finished,
                view = run.View,
            };
        }
    }

    private void StopRun()
    {
        RunState run;
        lock (_runLock)
        {
            if (_run is not { Running: true } current) throw new StudioRefusal(409, "No run is running.");
            run = current;
            run.StopRequested = true;
        }
        run.Cts.Cancel();
    }

    private async Task StopRunAndWaitAsync()
    {
        RunState? run;
        lock (_runLock)
        {
            run = _run is { Running: true } current ? current : null;
            if (run is not null) run.StopRequested = true;
        }
        if (run is null) return;
        run.Cts.Cancel();
        await Task.WhenAny(run.Task, Task.Delay(TimeSpan.FromSeconds(15)));
    }
}
