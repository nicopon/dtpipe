using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Cli.Split;
using DtPipe.Sessions;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The debug run: the real sample (<see cref="SampleCoreAsync"/>) of a copy of the job cut after every
/// transformer by <see cref="DebugCutter"/>, each level keeping a checkpoint in the studio's own session. It is
/// not the run of the job: every cut passes through Arrow, batches differ and so does the concurrency. The cut
/// is never saved or published. A later debug run can start from a level it kept, if what produced that
/// level is unchanged and the checkpoint is still in the session.
/// </summary>
public sealed partial class StudioServer
{
    /// <summary>The most source rows a debug run reads from each source; the sample shows at most <see cref="MaxRows"/> per stage.</summary>
    public const int DebugMaxRows = 100_000;

    private sealed record DebugRequest(string? Yaml, int? Rows, string? Path, List<SampledBrick>? Bricks, DebugResumeRequest? Resume);

    private sealed record DebugResumeRequest(string? Branch, int Level);

    private async Task DebugAsync(HttpContext context)
    {
        var cutter = _tools.Debug ?? throw new StudioRefusal(404, "This studio runs no debug run.");
        var asked = await BodyAsync<DebugRequest>(context);
        if (string.IsNullOrWhiteSpace(asked.Yaml)) throw new StudioRefusal(400, "A debug run is asked as {\"yaml\": \"...\", \"rows\": n}.");
        var rows = Math.Clamp(asked.Rows ?? 1000, 1, DebugMaxRows);
        var job = JobPath(asked.Path);
        if (job is not null) _local.CheckDebug(job);
        if (asked.Resume is not null && job is null) throw new StudioRefusal(400, "Resume a debug run of a job saved in the folder: name its path.");

        DebugCut? cut = null;
        string Rewrite(string yaml)
        {
            try
            {
                cut = cutter.Cut(yaml);
                if (asked.Resume is { } resume) cut = cutter.Cut(yaml, ResumeOf(job!, resume, cut));
            }
            catch (InvalidOperationException ex) { throw new StudioRefusal(422, ex.Message); }
            return cut.Yaml;
        }

        var json = await SampleCoreAsync(asked.Yaml, rows, asked.Path, asked.Bricks, _tools.DebugSample ?? _tools.Sample, Rewrite, context.RequestAborted);

        var report = JsonNode.Parse(json)!;
        var keys = (report["branches"] as JsonArray ?? [])
            .Where(b => b?["checkpoint"]?.GetValue<string>() is { Length: > 0 })
            .ToDictionary(b => b!["branch"]!.GetValue<string>(), b => b!["checkpoint"]!.GetValue<string>(), StringComparer.OrdinalIgnoreCase);
        var levels = cut!.Levels.Select(l => new { l.Branch, l.Level, l.Alias, l.Digest, Key = keys.GetValueOrDefault(l.Alias) }).ToList();
        if (job is not null)
        {
            KeepLevels(job, levels.Select(l => (l.Branch, l.Level, l.Alias, l.Digest, l.Key)), asked.Resume, rows);
            // A level read back keeps the key it was kept with: it ran no checkpoint of its own.
            var held = (_local.ReadDebug(job)?["levels"] as JsonArray)?.OfType<JsonObject>().ToList() ?? [];
            levels = levels.Select(l => l with
            {
                Key = l.Key ?? held.FirstOrDefault(h => h["branch"]?.GetValue<string>() == l.Branch && h["level"]?.GetValue<int>() == l.Level)?["key"]?.GetValue<string>(),
            }).ToList();
        }
        await JsonAsync(context, new
        {
            report,
            rows,
            resumed = asked.Resume,
            levels = levels.Select(l => new { branch = l.Branch, level = l.Level, alias = l.Alias, key = l.Key }),
            notice = "This is a debug run, not the job's run: every cut passes through Arrow, batches differ and so does the concurrency. "
                   + "The cut is never saved or published.",
        });
    }

    /// <summary>The level the person asked to start from, if it is still the one a debug run kept.</summary>
    private DebugResume ResumeOf(string job, DebugResumeRequest resume, DebugCut current)
    {
        var branch = resume.Branch ?? throw new StudioRefusal(400, "Resume names a branch and a level.");
        var kept = (_local.ReadDebug(job)?["levels"] as JsonArray)?
            .FirstOrDefault(l => l?["branch"]?.GetValue<string>() == branch && l["level"]?.GetValue<int>() == resume.Level);
        if (kept?["key"]?.GetValue<string>() is not { Length: > 0 } key)
            throw new StudioRefusal(409, $"No debug run of this job kept level {resume.Level} of '{branch}': run it first.");
        var now = current.Levels.FirstOrDefault(l => l.Branch == branch && l.Level == resume.Level)
            ?? throw new StudioRefusal(409, $"The job has no level {resume.Level} of '{branch}' any more: run the debug run again.");
        if (kept["digest"]?.GetValue<string>() != now.Digest)
            throw new StudioRefusal(409, $"Level {resume.Level} of '{branch}' was kept for another job: what produced it changed since. Run the debug run again.");
        if (!new CheckpointStore(SessionStore.Resolve(DebugCutter.SessionName, _folder.Root)).List().Contains(key))
            throw new StudioRefusal(409, $"The checkpoint of level {resume.Level} of '{branch}' is no longer in the session: it expired. Run the debug run again.");
        return new DebugResume(branch, resume.Level, key);
    }

    /// <summary>
    /// What a debug run keeps for the next: each level's key and digest. A resumed run replaces only the levels
    /// after the one it started from; the levels before it were not run and keep what they had.
    /// </summary>
    private void KeepLevels(string job, IEnumerable<(string Branch, int Level, string Alias, string Digest, string? Key)> levels,
        DebugResumeRequest? resume, int rows)
    {
        var previous = (_local.ReadDebug(job)?["levels"] as JsonArray)?.OfType<JsonObject>().ToList() ?? [];
        var kept = new JsonArray();
        foreach (var (branch, level, alias, digest, key) in levels)
        {
            var before = resume is not null && resume.Branch == branch && level <= resume.Level;
            var old = previous.FirstOrDefault(p => p["branch"]?.GetValue<string>() == branch && p["level"]?.GetValue<int>() == level);
            if (before && old is not null) { kept.Add(old.DeepClone()); continue; }
            if (key is null) continue;
            kept.Add(new JsonObject { ["branch"] = branch, ["level"] = level, ["alias"] = alias, ["digest"] = digest, ["key"] = key });
        }
        _local.WriteDebug(job, new JsonObject { ["session"] = DebugCutter.SessionName, ["rows"] = rows, ["levels"] = kept });
    }
}
