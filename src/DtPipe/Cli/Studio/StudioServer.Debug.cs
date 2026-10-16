using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Cli.Split;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Sessions;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The preview: the real sample (<see cref="SampleCoreAsync"/>) of a copy of the job cut at the cards the
/// person pins by <see cref="DebugCutter"/>, each watched level keeping a checkpoint in the studio's own
/// session. It is not the run of the job: every cut passes through Arrow, batches differ and so does the
/// concurrency. The cut is never saved or published. A run stops at the card it was asked for and starts, branch
/// by branch, from the highest level it holds, if the text that produced that level is unchanged and its
/// checkpoint is still in the session.
///
/// <para>
/// The watchers and the levels held live beside the job, in <c>.dtpipe/studio/&lt;job&gt;-debug.json</c>; a job not
/// saved yet keeps them under <c>drafts/&lt;id&gt;</c>, which moves to the job when it is first saved and is
/// forgotten when the studio starts. Everything that changes a checkpoint holds the engine slot.
/// </para>
/// </summary>
public sealed partial class StudioServer
{
    /// <summary>The most source rows a debug run reads from each source; the sample shows at most <see cref="MaxRows"/> per stage.</summary>
    public const int DebugMaxRows = 100_000;

    private sealed record DebugAtRequest(string? Branch, string? At, string? Card = null);

    private sealed record DebugRequest(string? Yaml, int? Rows, string? Path, string? Draft, List<SampledBrick>? Bricks, DebugAtRequest? UpTo);

    private sealed record DebugStateRequest(
        string? Yaml, int? Rows, string? Path, string? Draft, List<DebugAtRequest>? Watch, bool? ResetAll, List<DebugAtRequest>? Reset, string? Adopt);

    /// <summary><see cref="Rows"/> is the count of a level a real run holds before its writer, read once when the run ended; null when unknown.</summary>
    private sealed record HeldLevel(string Branch, int Level, string Digest, string Key, long? Rows = null);

    private sealed class DebugState
    {
        public List<DebugWatch> Watch { get; set; } = [];
        public List<HeldLevel> Levels { get; set; } = [];

        /// <summary>The levels held are the rows of a real run that has not written its target yet.</summary>
        public bool Real { get; set; }
    }

    private static DebugWatch WatchOf(DebugAtRequest? asked)
    {
        if (asked?.Branch is not { Length: > 0 } branch) throw new StudioRefusal(400, "A watcher names a branch and a place: {\"branch\": \"...\", \"at\": \"read|end\"}.");
        return new DebugWatch(branch, asked.At switch
        {
            "read" => DebugAt.Read,
            "end" => DebugAt.End,
            _ => throw new StudioRefusal(400, "A watcher sits at \"read\" or at \"end\"."),
        }, asked.Card is { Length: > 0 } card ? card : null);
    }

    private static string AtName(DebugAt at) => at == DebugAt.Read ? "read" : "end";

    private static bool Same(string a, string b) => a.Equals(b, StringComparison.OrdinalIgnoreCase);

    /// <summary>What the state of a job is kept under: its path, or the draft of a job not saved yet.</summary>
    private string DebugKey(string? path, string? draft)
    {
        if (JobPath(path) is { } job) return job;
        if (draft is { Length: >= 16 and <= 64 } && draft.All(Uri.IsHexDigit)) return "drafts/" + draft;
        throw new StudioRefusal(400, "A debug run belongs to a job: name its path, or the draft of a job not saved yet.");
    }

    private DebugPlan PlanOf(DebugCutter cutter, string? yaml, int rows, string? salt = null)
    {
        if (string.IsNullOrWhiteSpace(yaml)) throw new StudioRefusal(400, "A debug request carries {\"yaml\": \"...\", \"rows\": n}.");
        try { return cutter.Plan(yaml, rows, salt); }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException or YamlDotNet.Core.YamlException)
        {
            throw new StudioRefusal(422, ex.Message);
        }
    }

    private CheckpointStore DebugStore() => new(SessionStore.Resolve(DebugCutter.SessionName, _folder.Root));

    private void PurgeDrafts() => _local.PurgeDrafts(key => ForgetCheckpoint(DebugStore(), key));

    private DebugState LoadDebug(string key)
    {
        var state = new DebugState();
        try
        {
            var node = _local.ReadDebug(key);
            foreach (var w in node?["watch"] as JsonArray ?? [])
                if ((string?)w?["branch"] is { } branch && (string?)w["at"] is "read" or "end")
                    state.Watch.Add(new DebugWatch(branch, (string?)w["at"] == "read" ? DebugAt.Read : DebugAt.End, (string?)w["card"]));
            foreach (var l in node?["levels"] as JsonArray ?? [])
                if ((string?)l?["branch"] is { } branch && (int?)l["level"] is { } level && (string?)l["digest"] is { } digest && (string?)l["key"] is { Length: > 0 } held)
                    state.Levels.Add(new HeldLevel(branch, level, digest, held, (long?)l["rows"]));
            state.Real = (bool?)node?["real"] == true && state.Levels.Count > 0;
        }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException) { return new DebugState(); }
        return state;
    }

    private void SaveDebug(string key, DebugState state)
    {
        if (state.Watch.Count == 0 && state.Levels.Count == 0) { _local.ClearDebug(key); return; }
        _local.WriteDebug(key, new JsonObject
        {
            ["session"] = DebugCutter.SessionName,
            ["real"] = state.Real && state.Levels.Count > 0,
            ["watch"] = new JsonArray(state.Watch.Select(w => (JsonNode)new JsonObject { ["branch"] = w.Branch, ["at"] = AtName(w.At), ["card"] = w.Card }).ToArray()),
            ["levels"] = new JsonArray(state.Levels.Select(l => (JsonNode)new JsonObject
                { ["branch"] = l.Branch, ["level"] = l.Level, ["digest"] = l.Digest, ["key"] = l.Key, ["rows"] = l.Rows }).ToArray()),
        });
    }

    /// <summary>
    /// What is still true: the watchers that can be watched there, the levels a watcher sits on whose text has not
    /// changed and whose checkpoint is in the session, and the keys that are not (with <paramref name="forgotten"/>).
    /// Pure: nothing leaves the session.
    /// </summary>
    private (List<DebugWatch> Watch, List<HeldLevel> Keep, HashSet<string> Drop) Judge(DebugState state, DebugPlan plan, IEnumerable<string> forgotten)
    {
        var store = DebugStore();
        var drop = new HashSet<string>(forgotten);
        var watch = state.Watch.Where(w => plan.Resolve(w) is not null).DistinctBy(w => (w.Branch.ToLowerInvariant(), w.At)).ToList();
        var keep = new List<HeldLevel>();
        foreach (var held in state.Levels)
        {
            var valid = plan.Levels.Any(p => Same(p.Branch, held.Branch) && p.Level == held.Level && p.Digest == held.Digest)
                && (watch.Any(w => Same(w.Branch, held.Branch) && plan.Resolve(w) == held.Level) || IsQueued(state, plan, held))
                && store.Contains(held.Key);
            if (valid) keep.Add(held); else drop.Add(held.Key);
        }
        drop.ExceptWith(keep.Select(h => h.Key));
        return (watch, keep, drop);
    }

    /// <summary>
    /// The rows a real run holds before the branch's writer: the end of a branch that writes, while the run is suspended. No
    /// pin sits there, and none is made: a pin is the person's choice, and this is the run's.
    /// </summary>
    private static bool IsQueued(DebugState state, DebugPlan plan, HeldLevel held) =>
        state.Real && plan.Branches.Any(b => b.Writes && Same(b.Branch, held.Branch) && b.Transformers == held.Level);

    /// <summary>
    /// Keeps only what is still true (<see cref="Judge"/>). What it drops, and <paramref name="forgotten"/>, leaves
    /// the session: a checkpoint is valid or it does not exist.
    /// </summary>
    private void Settle(DebugState state, DebugPlan plan, IEnumerable<string> forgotten)
    {
        var (watch, keep, drop) = Judge(state, plan, forgotten);
        state.Watch = watch;
        state.Levels = keep;
        if (keep.Count == 0) state.Real = false;
        var store = DebugStore();
        foreach (var key in drop) ForgetCheckpoint(store, key);
    }

    /// <summary>
    /// The plan of the job a suspended run was of: the file as saved, whose branches the run held. The cards the page composes may name
    /// them differently until the studio writes the file, and the queue names the branches the rows are held under.
    /// </summary>
    private DebugPlan SuspendedPlan(DebugCutter cutter, string? path, DebugPlan fallback)
    {
        if (JobPath(path) is not { } job) return fallback;
        try { return cutter.Plan(_folder.ReadJob(job).Text, 0); }
        catch (Exception ex) when (ex is StudioRefusal or InvalidOperationException or FormatException or YamlDotNet.Core.YamlException or IOException) { return fallback; }
    }

    private static void RefuseWhileSuspended(DebugState state)
    {
        if (state.Real)
            throw new StudioRefusal(409, "A real run is suspended before its target is written: continue it, or abandon it to forget its rows.");
    }

    private static void ForgetCheckpoint(CheckpointStore store, string key)
    {
        try { store.Delete(key); }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException) { }
    }

    /// <summary>
    /// What the page shows of a job: the watchers with what each holds, and every place a card can be read at, with
    /// the digest of what its rows depend on. After a run, <c>alias</c> names the piece of the cut whose report holds the place
    /// (its first stage for a read, its last for an end), or is null when this run did not produce it: its rows
    /// were read back, or its branch did not run.
    /// </summary>
    private object DebugView(
        DebugPlan plan, DebugState state, IReadOnlyList<DebugPiece>? pieces, DebugCut? cut = null, IReadOnlySet<string>? ran = null, DebugPlan? suspended = null) => new
    {
        real = state.Real,
        branches = plan.Branches.Select(b => new { branch = b.Branch, transformers = b.Transformers, cuttable = b.Cuttable, writes = b.Writes }),
        // Where a suspended run waits: every branch that writes, with the rows it holds before its writer when it holds them.
        queue = !state.Real ? new List<object>() : (suspended ?? plan).Branches.Where(b => b.Writes).Select(b =>
        {
            var held = state.Levels.FirstOrDefault(h => Same(h.Branch, b.Branch) && h.Level == b.Transformers);
            return (object)new { branch = b.Branch, key = held?.Key, rows = held?.Rows };
        }).ToList(),
        places = plan.Branches.SelectMany(b => new[] { DebugAt.Read, DebugAt.End }.Select(at => new
        {
            branch = b.Branch,
            at = AtName(at),
            digest = plan.DigestAt(b.Branch, at),
            alias = cut is null || ran is null ? null : Locate(cut, ran, b, at),
        })),
        watch = state.Watch.Select(w =>
        {
            var level = plan.Resolve(w);
            return new
            {
                branch = w.Branch,
                at = AtName(w.At),
                card = w.Card,
                level,
                key = level is null ? null : state.Levels.FirstOrDefault(h => Same(h.Branch, w.Branch) && h.Level == level)?.Key,
                alias = level is null ? null : pieces?.FirstOrDefault(p => Same(p.Branch, w.Branch) && p.Level == level)?.Alias,
            };
        }),
    };

    /// <summary>
    /// The piece of the cut whose report holds a branch's read or end, or null when the run did not produce it. A read
    /// is the piece cut at level 0, else the branch's own first stage unless the branch was read back from a later level.
    /// </summary>
    private static string? Locate(DebugCut cut, IReadOnlySet<string> ran, DebugBranch branch, DebugAt at)
    {
        string? alias = branch.Branch;
        if (at == DebugAt.Read && branch.Transformers > 0)
        {
            var head = cut.Pieces.FirstOrDefault(p => Same(p.Branch, branch.Branch) && p.Level == 0);
            alias = head is not null ? head.Alias : cut.Unread.Contains(branch.Branch) ? null : branch.Branch;
        }
        return alias is not null && ran.Contains(alias) ? alias : null;
    }

    /// <summary>The aliases a sample reported a branch for: the pieces of the cut that ran.</summary>
    private static HashSet<string> RanOf(string reported) =>
        (JsonNode.Parse(reported)?["branches"] as JsonArray ?? []).Select(b => (string?)b?["branch"]).OfType<string>().ToHashSet(StringComparer.OrdinalIgnoreCase);

    private async Task DebugAsync(HttpContext context)
    {
        var cutter = _tools.Engine.Cutter;
        var asked = await BodyAsync<DebugRequest>(context);
        var rows = Math.Clamp(asked.Rows ?? 1000, 1, DebugMaxRows);
        var key = DebugKey(asked.Path, asked.Draft);
        _local.CheckDebug(key);
        var upTo = asked.UpTo is null ? null : WatchOf(asked.UpTo);
        var plan = PlanOf(cutter, asked.Yaml, rows);

        DebugCut? cut = null;
        DebugState? state = null;
        (string, IReadOnlySet<string>, IReadOnlySet<string>) Rewrite(string yaml)
        {
            // The engine slot is held here: what is settled and cut cannot change under another action.
            state = LoadDebug(key);
            RefuseWhileSuspended(state);
            Settle(state, plan, []);
            try { cut = cutter.Cut(yaml, state.Watch, upTo, state.Levels.Select(l => new DebugHeld(l.Branch, l.Level, l.Key)).ToList()); }
            catch (InvalidOperationException ex) { throw new StudioRefusal(422, ex.Message); }
            SaveDebug(key, state);
            return (cut.Yaml, cut.Dropped, cut.Unread);
        }

        var sampler = _tools.Engine.DebugSampleAsync;
        string? reported = null;
        async Task<string> Run(string yaml, int n, StudioBindings bindings, CancellationToken ct)
        {
            reported = await sampler(yaml, n, bindings, ct);
            Keep(key, state!, plan, cut!, reported);
            return reported;
        }

        await SampleCoreAsync(asked.Yaml!, rows, asked.Path, asked.Bricks, Run, Rewrite, context.RequestAborted);

        await JsonAsync(context, new
        {
            report = JsonNode.Parse(reported!),
            rows,
            upTo = asked.UpTo,
            view = DebugView(plan, state!, cut!.Pieces, cut, RanOf(reported!)),
            notice = "This is a debug run, not the job's run: every cut passes through Arrow, batches differ and so does the concurrency. "
                   + "The cut is never saved or published.",
        });
    }

    /// <summary>What a debug run keeps for the next: the checkpoint each watched piece wrote, under the digest of its level.</summary>
    private void Keep(string key, DebugState state, DebugPlan plan, DebugCut cut, string reported)
    {
        var keys = (JsonNode.Parse(reported)?["branches"] as JsonArray ?? [])
            .Where(b => (string?)b?["checkpoint"] is { Length: > 0 })
            .ToDictionary(b => (string)b!["branch"]!, b => (string)b!["checkpoint"]!, StringComparer.OrdinalIgnoreCase);
        Keep(key, state, plan, cut, keys);
    }

    private void Keep(string key, DebugState state, DebugPlan plan, DebugCut cut, IReadOnlyDictionary<string, string> keys)
    {
        var store = DebugStore();
        foreach (var piece in cut.Pieces)
        {
            // A level read back wrote nothing: it keeps the key it was held under.
            if (!keys.TryGetValue(piece.Alias, out var written) || !store.Contains(written)) continue;
            // A stop that nobody watches kept its rows for this run only: its checkpoint does not outlive it, unless a level held has the same key.
            if (!state.Watch.Any(w => Same(w.Branch, piece.Branch) && plan.Resolve(w) == piece.Level))
            {
                if (!state.Levels.Any(h => h.Key == written)) ForgetCheckpoint(store, written);
                continue;
            }
            var digest = plan.Levels.FirstOrDefault(p => Same(p.Branch, piece.Branch) && p.Level == piece.Level)?.Digest;
            if (digest is null) continue;
            state.Levels.RemoveAll(h => Same(h.Branch, piece.Branch) && h.Level == piece.Level);
            state.Levels.Add(new HeldLevel(piece.Branch, piece.Level, digest, written));
        }
        SaveDebug(key, state);
    }

    /// <summary>
    /// Changes what a job watches and holds, and tells what it holds now: replaces the watchers, resets a card (its
    /// checkpoint and every one downstream) or all, and settles what the job's text no longer supports. A draft
    /// <c>adopt</c>ed by a saved job hands its state over first.
    /// </summary>
    private async Task DebugStateAsync(HttpContext context)
    {
        var cutter = _tools.Engine.Cutter;
        var asked = await BodyAsync<DebugStateRequest>(context);
        var rows = Math.Clamp(asked.Rows ?? 1000, 1, DebugMaxRows);
        var key = DebugKey(asked.Path, asked.Draft);
        _local.CheckDebug(key);
        var plan = PlanOf(cutter, asked.Yaml, rows);
        var wanted = asked.Watch?.Select(WatchOf).ToList();
        var reset = (asked.Reset ?? []).Select(WatchOf).ToList();
        string? adopted = asked.Adopt is null || asked.Path is null ? null : DebugKey(null, asked.Adopt);
        if (adopted is not null) _local.CheckDebug(adopted);

        if (!await _sampling.WaitAsync(0))
            throw new StudioRefusal(409, "An action is already running: wait for it to finish.");
        try
        {
            if (adopted is not null) _local.AdoptDebug(adopted, key);
            var state = LoadDebug(key);
            // Abandoning a suspended real run is the one change it allows: its rows belong to the text and the values it started with.
            // Asking what it holds changes nothing, and is answered as it is: a sample's plan would judge those rows by the wrong digest.
            if (state.Real && asked.ResetAll != true)
            {
                if (wanted is not null || reset.Count > 0) RefuseWhileSuspended(state);
                await JsonAsync(context, new { rows, view = DebugView(plan, state, null, suspended: SuspendedPlan(cutter, asked.Path, plan)) });
                return;
            }
            if (wanted is not null) state.Watch = wanted;
            var forgotten = new List<string>();
            if (asked.ResetAll == true)
            {
                forgotten.AddRange(state.Levels.Select(h => h.Key));
                state.Levels.Clear();
            }
            foreach (var card in reset)
            {
                if (plan.Resolve(card) is not { } level) continue;
                var downstream = plan.Downstream(card.Branch);
                var gone = state.Levels.Where(h => Same(h.Branch, card.Branch) && h.Level >= level || downstream.Contains(h.Branch)).ToList();
                forgotten.AddRange(gone.Select(h => h.Key));
                state.Levels.RemoveAll(gone.Contains);
            }
            Settle(state, plan, forgotten);
            SaveDebug(key, state);
            await JsonAsync(context, new { rows, view = DebugView(plan, state, null) });
        }
        finally { _sampling.Release(); }
    }

    private sealed record DebugPlanRequest(string? Yaml, int? Rows);

    /// <summary>
    /// The places of a job's cards and the digest of what each depends on. Pure: it reads nothing held, changes nothing and
    /// takes no engine slot, so the page asks it after every edit to tell which previews the edit made untrue.
    /// </summary>
    private async Task DebugPlanAsync(HttpContext context)
    {
        var cutter = _tools.Engine.Cutter;
        var asked = await BodyAsync<DebugPlanRequest>(context);
        var plan = PlanOf(cutter, asked.Yaml, Math.Clamp(asked.Rows ?? 1000, 1, DebugMaxRows));
        await JsonAsync(context, new { view = DebugView(plan, new DebugState(), null) });
    }

    private sealed record DebugRowsRequest(string? Path, string? Draft, string? Key);

    /// <summary>
    /// The first rows of a checkpoint this job holds, shaped as a stage of a sample: how a real run, which reports no rows,
    /// shows a person what is held before its target is written. Only a key the job's state holds is read.
    /// </summary>
    private async Task DebugRowsAsync(HttpContext context)
    {
        var asked = await BodyAsync<DebugRowsRequest>(context);
        var key = DebugKey(asked.Path, asked.Draft);
        _local.CheckDebug(key);
        if (asked.Key is not { Length: > 0 } held || !LoadDebug(key).Levels.Any(l => l.Key == held))
            throw new StudioRefusal(404, "This job holds no such checkpoint.");

        var store = DebugStore();
        var rows = new List<List<string?>>();
        List<object> columns = [];
        var more = false;
        try
        {
            await foreach (var batch in store.ReadAsync(held, context.RequestAborted))
            {
                using (batch)
                {
                    if (columns.Count == 0)
                        columns = batch.Schema.FieldsList.Select(f => (object)new { name = f.Name, type = f.DataType.Name, isNullable = f.IsNullable }).ToList();
                    for (var i = 0; i < batch.Length; i++)
                    {
                        if (rows.Count >= MaxRows) { more = true; break; }
                        rows.Add(Enumerable.Range(0, batch.Schema.FieldsList.Count)
                            .Select(c => ArrowTypeMapper.GetValueForField(batch.Column(c), batch.Schema.FieldsList[c], i)?.ToString()).ToList());
                    }
                }
                if (more) break;
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            throw new StudioRefusal(404, "This checkpoint can no longer be read.");
        }
        await JsonAsync(context, new { stage = new { index = 0, name = "checkpoint", columns, rowsSeen = rows.Count, more, sample = rows } });
    }
}
