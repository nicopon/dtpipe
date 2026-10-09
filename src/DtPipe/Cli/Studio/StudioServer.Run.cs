using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using DtPipe.Cli.Expressions;
using DtPipe.Core.Security;
using DtPipe.Jobs;
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
    }

    private sealed record RunRequest(string? Path, string? Hash, List<SampledBrick>? Bricks, string? Digest);

    private sealed record RunWrite(string Alias, bool Throwaway, string? Output, string? Block);

    private sealed record RunPlan(
        string Path, BrickSampling Sampling, IReadOnlyDictionary<string, string> Launch, IReadOnlyList<RunWrite> Writes, string Digest);

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
        var launch = _variables.LaunchEnvironment(path, sampling.Yaml);

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

        var material = new StringBuilder(job.Text).Append('\0');
        foreach (var (name, value) in launch.OrderBy(p => p.Key, StringComparer.Ordinal)) material.Append(name).Append('=').Append(value).Append('\0');
        foreach (var alias in sampling.Sinks.Order(StringComparer.Ordinal)) material.Append("throwaway:").Append(alias).Append('\0');
        var digest = Convert.ToHexStringLower(HMACSHA256.HashData(_token, Encoding.UTF8.GetBytes(material.ToString())));
        return new RunPlan(path, sampling, launch, writes, digest);
    }

    private async Task PrepareRunAsync(HttpContext context)
    {
        if (_tools.Run is null) throw new StudioRefusal(404, "This studio runs no job.");
        var plan = PlanRun(await BodyAsync<RunRequest>(context));
        await JsonAsync(context, new { digest = plan.Digest, writes = plan.Writes });
    }

    private async Task StartRunAsync(HttpContext context)
    {
        var runner = _tools.Run ?? throw new StudioRefusal(404, "This studio runs no job.");
        var asked = await BodyAsync<RunRequest>(context);
        var plan = PlanRun(asked);
        if (asked.Digest != plan.Digest)
            throw new StudioRefusal(409, "The job or the values it launches with changed since you confirmed: confirm what it writes again.");
        if (!await _sampling.WaitAsync(0))
            throw new StudioRefusal(409, "An action is already running: wait for it to finish.");

        var files = new List<string>();
        try
        {
            // A sink brick is written to a throwaway file here, never to its node, as a sample does.
            var outputs = new List<string>();
            foreach (var alias in plan.Sampling.Sinks)
            {
                files.Add(_local.SampleFile(null));
                outputs.Add($"{alias}={Bindable(files[^1])}");
            }
            var bindings = new StudioBindings(null, outputs.Count > 0 ? string.Join(',', outputs) : null);
            var state = new RunState(plan.Path, new CancellationTokenSource());
            lock (_runLock) _run = state;
            state.Task = Task.Run(() => RunAsync(runner, plan, bindings, state, files));
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

    private async Task RunAsync(StudioRunner runner, RunPlan plan, StudioBindings bindings, RunState state, List<string> files)
    {
        var code = 1;
        string? error = null;
        try
        {
            // Armed inside the task, for this run's call chain alone; the environment is never written.
            using var armed = LaunchValues.Arm(plan.Launch);
            using var quiet = NonInteractiveGuard.Suppress();
            code = await runner(plan.Sampling.Yaml, bindings, state.Failures, state.Cts.Token);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { error = ConnectionStringSanitizer.Sanitize(ex.Message); }
        finally
        {
            foreach (var file in files) { try { File.Delete(file); } catch (IOException) { } }
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
