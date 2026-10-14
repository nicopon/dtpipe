using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Cli.Split;
using DtPipe.Sessions;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The debug run: the real sample (<see cref="SampleCoreAsync"/>) of a copy of the job cut at the cards the
/// person watches by <see cref="DebugCutter"/>, each watched level keeping a checkpoint in the studio's own
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

    private sealed record DebugAtRequest(string? Branch, string? At, string? Card = null, bool? Break = null);

    private sealed record DebugRequest(string? Yaml, int? Rows, string? Path, string? Draft, List<SampledBrick>? Bricks, DebugAtRequest? UpTo);

    private sealed record DebugStateRequest(
        string? Yaml, int? Rows, string? Path, string? Draft, List<DebugAtRequest>? Watch, bool? ResetAll, List<DebugAtRequest>? Reset, string? Adopt);

    private sealed record HeldLevel(string Branch, int Level, string Digest, string Key);

    private sealed class DebugState
    {
        public List<DebugWatch> Watch { get; set; } = [];
        public List<HeldLevel> Levels { get; set; } = [];
    }

    private static DebugWatch WatchOf(DebugAtRequest? asked)
    {
        if (asked?.Branch is not { Length: > 0 } branch) throw new StudioRefusal(400, "A watcher names a branch and a place: {\"branch\": \"...\", \"at\": \"read|end\"}.");
        return new DebugWatch(branch, asked.At switch
        {
            "read" => DebugAt.Read,
            "end" => DebugAt.End,
            _ => throw new StudioRefusal(400, "A watcher sits at \"read\" or at \"end\"."),
        }, asked.Card is { Length: > 0 } card ? card : null, asked.Break == true);
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

    private DebugPlan PlanOf(DebugCutter cutter, string? yaml, int rows)
    {
        if (string.IsNullOrWhiteSpace(yaml)) throw new StudioRefusal(400, "A debug request carries {\"yaml\": \"...\", \"rows\": n}.");
        try { return cutter.Plan(yaml, rows); }
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
                    state.Watch.Add(new DebugWatch(branch, (string?)w["at"] == "read" ? DebugAt.Read : DebugAt.End, (string?)w["card"], (bool?)w["break"] == true));
            foreach (var l in node?["levels"] as JsonArray ?? [])
                if ((string?)l?["branch"] is { } branch && (int?)l["level"] is { } level && (string?)l["digest"] is { } digest && (string?)l["key"] is { Length: > 0 } held)
                    state.Levels.Add(new HeldLevel(branch, level, digest, held));
        }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException) { return new DebugState(); }
        return state;
    }

    private void SaveDebug(string key, DebugState state) => _local.WriteDebug(key, new JsonObject
    {
        ["session"] = DebugCutter.SessionName,
        ["watch"] = new JsonArray(state.Watch.Select(w => (JsonNode)new JsonObject { ["branch"] = w.Branch, ["at"] = AtName(w.At), ["card"] = w.Card, ["break"] = w.Break }).ToArray()),
        ["levels"] = new JsonArray(state.Levels.Select(l => (JsonNode)new JsonObject
            { ["branch"] = l.Branch, ["level"] = l.Level, ["digest"] = l.Digest, ["key"] = l.Key }).ToArray()),
    });

    /// <summary>
    /// Keeps only what is still true: a watcher on a branch that can be watched there, and a level that a watcher
    /// sits on, whose text has not changed and whose checkpoint is in the session. What it drops, and
    /// <paramref name="forgotten"/>, leaves the session: a checkpoint is valid or it does not exist.
    /// </summary>
    private void Settle(DebugState state, DebugPlan plan, IEnumerable<string> forgotten)
    {
        var store = DebugStore();
        var drop = new HashSet<string>(forgotten);
        state.Watch = state.Watch.Where(w => plan.Resolve(w) is not null).DistinctBy(w => (w.Branch.ToLowerInvariant(), w.At)).ToList();
        var keep = new List<HeldLevel>();
        foreach (var held in state.Levels)
        {
            var valid = plan.Levels.Any(p => Same(p.Branch, held.Branch) && p.Level == held.Level && p.Digest == held.Digest)
                && state.Watch.Any(w => Same(w.Branch, held.Branch) && plan.Resolve(w) == held.Level)
                && store.Contains(held.Key);
            if (valid) keep.Add(held); else drop.Add(held.Key);
        }
        state.Levels = keep;
        drop.ExceptWith(keep.Select(h => h.Key));
        foreach (var key in drop) ForgetCheckpoint(store, key);
    }

    private static void ForgetCheckpoint(CheckpointStore store, string key)
    {
        try { store.Delete(key); }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException) { }
    }

    private object DebugView(DebugPlan plan, DebugState state, IReadOnlyList<DebugPiece>? pieces) => new
    {
        branches = plan.Branches.Select(b => new { branch = b.Branch, transformers = b.Transformers, cuttable = b.Cuttable }),
        watch = state.Watch.Select(w =>
        {
            var level = plan.Resolve(w);
            return new
            {
                branch = w.Branch,
                at = AtName(w.At),
                card = w.Card,
                @break = w.Break,
                level,
                key = level is null ? null : state.Levels.FirstOrDefault(h => Same(h.Branch, w.Branch) && h.Level == level)?.Key,
                alias = level is null ? null : pieces?.FirstOrDefault(p => Same(p.Branch, w.Branch) && p.Level == level)?.Alias,
            };
        }),
    };

    private async Task DebugAsync(HttpContext context)
    {
        var cutter = _tools.Debug ?? throw new StudioRefusal(404, "This studio runs no debug run.");
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
            if (upTo is not null && !state.Watch.Any(w => Same(w.Branch, upTo.Branch) && w.At == upTo.At)) state.Watch.Add(upTo);
            Settle(state, plan, []);
            try { cut = cutter.Cut(yaml, state.Watch, upTo, state.Levels.Select(l => new DebugHeld(l.Branch, l.Level, l.Key)).ToList()); }
            catch (InvalidOperationException ex) { throw new StudioRefusal(422, ex.Message); }
            SaveDebug(key, state);
            return (cut.Yaml, cut.Dropped, cut.Unread);
        }

        var sampler = _tools.DebugSample ?? _tools.Sample;
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
            view = DebugView(plan, state!, cut!.Pieces),
            notice = "This is a debug run, not the job's run: every cut passes through Arrow, batches differ and so does the concurrency. "
                   + "The cut is never saved or published.",
        });
    }

    /// <summary>What a debug run keeps for the next: the checkpoint each watched piece wrote, under the digest of its level.</summary>
    private void Keep(string key, DebugState state, DebugPlan plan, DebugCut cut, string reported)
    {
        var store = DebugStore();
        var keys = (JsonNode.Parse(reported)?["branches"] as JsonArray ?? [])
            .Where(b => (string?)b?["checkpoint"] is { Length: > 0 })
            .ToDictionary(b => (string)b!["branch"]!, b => (string)b!["checkpoint"]!, StringComparer.OrdinalIgnoreCase);
        foreach (var piece in cut.Pieces)
        {
            // A level read back wrote nothing: it keeps the key it was held under.
            if (!keys.TryGetValue(piece.Alias, out var written) || !store.Contains(written)) continue;
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
        var cutter = _tools.Debug ?? throw new StudioRefusal(404, "This studio runs no debug run.");
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
}
