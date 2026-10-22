using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// How a fragment's child keeps a cursor without touching the coordinator or a file the brick names. A sink branch that sets
/// <c>cursor</c> is given a state file of the node's own, per run, fragment and alias; the child writes the file it always writes, and
/// the node reads it once the child has exited with 0 and sends the value with <c>Exited</c>. Whether it is kept is the coordinator's,
/// at the verdict of the whole run. The value a run starts from reaches the child as the one environment variable that
/// <c>--cursor-from</c> sets.
/// </summary>
internal static class FragmentCursors
{
    /// <summary>The variable the child's <c>${{cursor://|default}}</c> reads before it reads any file.</summary>
    public const string OverrideVariable = "DTPIPE_CURSOR_OVERRIDE";

    /// <summary>
    /// <paramref name="yaml"/> with a state file added to each branch that sets a cursor, and the path of each by alias. Only a job
    /// composed from bricks goes through here: a brick never names the file.
    /// </summary>
    public static (string Yaml, IReadOnlyDictionary<string, string> Files) Arm(string yaml, string directory, string stem)
    {
        var job = JobYaml.Parse(yaml);
        var files = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (alias, branch) in JobYaml.Branches(job))
        {
            if (string.IsNullOrWhiteSpace(JobYaml.Scalar(branch, BrickRules.CursorKey))) continue;
            var path = Path.Combine(directory, $"{stem}-{alias}.cursor.json");
            JobYaml.Set(branch, "state", path);
            files[alias] = path;
        }
        return files.Count == 0 ? (yaml, files) : (JobYaml.Serialize(job), files);
    }

    /// <summary>
    /// What each armed branch tracked: nothing for a branch that saw no row (the child writes no file), and nothing, with a warning, for a
    /// value the rule of <see cref="CursorValues"/> refuses.
    /// </summary>
    public static IReadOnlyDictionary<string, CursorReport>? Read(IReadOnlyDictionary<string, string> files, ILogger logger)
    {
        var reports = new Dictionary<string, CursorReport>(StringComparer.Ordinal);
        foreach (var (alias, path) in files)
        {
            try
            {
                if (!File.Exists(path)) continue;
                using var document = JsonDocument.Parse(File.ReadAllText(path));
                var cursor = document.RootElement.GetProperty("cursor");
                var report = new CursorReport(
                    cursor.GetProperty("column").GetString() ?? "", CursorValues.TypeOf(cursor.GetProperty("type").GetString()) ?? "",
                    cursor.GetProperty("value").GetString() ?? "");
                if (CursorValues.Refusal(report.Type, report.Value) is { } refusal)
                    logger.LogWarning("The cursor of '{Alias}' is not reported: {Reason}", alias, refusal);
                else
                    reports[alias] = report;
            }
            catch (Exception ex) when (ex is IOException or JsonException or KeyNotFoundException or InvalidOperationException)
            {
                logger.LogWarning("The cursor of '{Alias}' could not be read: {Error}", alias, ex.Message);
            }
        }
        return reports.Count == 0 ? null : reports;
    }
}
