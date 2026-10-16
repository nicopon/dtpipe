using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Split;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The studio's engine over <see cref="JobAnalysis"/>, the one sample and validation path it shares with the
/// <c>dry-run</c> tool, and <see cref="JobRunner"/>, which arms the collection of a real run so it never reads another's.
/// </summary>
public sealed class StudioEngine(JobAnalysis analysis, JobRunner runner, DebugCutter cutter) : IStudioEngine
{
    public DebugCutter Cutter => cutter;

    public Task<string> SampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        analysis.SampleAsync(yaml, rows, shownRows: rows, ct, bindings.Input, bindings.Output);

    public Task<string> DebugSampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        analysis.SampleAsync(yaml, rows, shownRows: Math.Min(rows, StudioServer.MaxRows), ct, bindings.Input, bindings.Output,
            maxRows: StudioServer.DebugMaxRows);

    public Task<RunOutcome> RunAsync(string yaml, StudioBindings bindings, CancellationToken ct)
    {
        var (jobs, dag) = analysis.ResolveForRun(yaml, bindings.Input, bindings.Output);
        var contexts = jobs.ToDictionary(kv => kv.Key, kv => new CliJobContext(null, null, null, Array.Empty<string>()));
        return runner.RunAsync(jobs, dag, contexts, new GlobalOptions { Quiet = true }, ct);
    }
}
