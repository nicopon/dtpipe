using DtPipe.Cli.Mcp;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Split;
using DtPipe.Core.Models;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The studio's engine over the one sample path of the <c>dry-run</c> tool and <see cref="JobRunner"/>, which
/// arms the collection of a real run so it never reads another's.
/// </summary>
public sealed class StudioEngine(DtPipeMcpTools tools, JobRunner runner, DebugCutter cutter) : IStudioEngine
{
    public DebugCutter Cutter => cutter;

    public Task<string> SampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        tools.SampleAsync(yaml, rows, shownRows: rows, ct, bindings.Input, bindings.Output);

    public Task<string> DebugSampleAsync(string yaml, int rows, StudioBindings bindings, CancellationToken ct) =>
        tools.SampleAsync(yaml, rows, shownRows: Math.Min(rows, StudioServer.MaxRows), ct, bindings.Input, bindings.Output,
            maxRows: StudioServer.DebugMaxRows);

    public Task<RunOutcome> RunAsync(string yaml, StudioBindings bindings, CancellationToken ct)
    {
        var (jobs, dag) = tools.ResolveForRun(yaml, bindings.Input, bindings.Output);
        var contexts = jobs.ToDictionary(kv => kv.Key, kv => new CliJobContext(null, null, null, Array.Empty<string>()));
        return runner.RunAsync(jobs, dag, contexts, new GlobalOptions { Quiet = true }, ct);
    }
}
