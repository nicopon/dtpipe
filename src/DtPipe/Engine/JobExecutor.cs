using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Engine;

/// <summary>
/// Runs a resolved job: binding, validation and orchestration, one run at a time in the process.
/// What the run shows and how it is interrupted belong to the <see cref="IRunHost"/> it is given.
/// </summary>
public class JobExecutor
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;

    public JobExecutor(
        IServiceProvider serviceProvider,
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
    {
        _serviceProvider = serviceProvider;

        var list = new List<ICliContributor>();
        list.AddRange(readerFactories.OfType<ICliContributor>());
        list.AddRange(transformerFactories.OfType<ICliContributor>());
        list.AddRange(writerFactories.OfType<ICliContributor>());
        _contributors = list;
    }

    /// <param name="branchFailures">Filled with the reason each failing branch stopped, for a caller
    /// that has silenced the console — an MCP tool reads a result, not a terminal.</param>
    public async Task<int> ExecuteAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, CliJobContext> contexts, GlobalOptions globals, IRunHost host, CancellationToken ct,
        System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures = null)
    {
        // A run waits here while another holds the process; cancelled while it waits, it never started.
        try { await OneRunAtATime.WaitAsync(ct); }
        catch (OperationCanceledException) { return 130; }

        try { return await RunHeldAsync(jobs, dag, contexts, globals, host, ct, branchFailures); }
        finally { OneRunAtATime.Release(); }
    }

    /// <summary>
    /// One run at a time in the process. A run owns state it shares with any other: the channel registry
    /// <c>DagOrchestrator</c> clears at its start, the console's Ctrl-C. Two runs over them trade rows, and one can still exit 0. Static, so a second
    /// container in the process waits too. A run started from inside a run would wait on itself.
    /// </summary>
    private static readonly SemaphoreSlim OneRunAtATime = new(1, 1);

    private async Task<int> RunHeldAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, CliJobContext> contexts, GlobalOptions globals, IRunHost host, CancellationToken ct,
        System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures)
    {
        // The dedicated user token discriminates user shutdown from internal cancellation (F16),
        // so the process can report exit code 130 instead of masking cancellation as success.
        using var userCts = new CancellationTokenSource();
        using var hosted = host.BeginRun(globals, userCts);

        var resultsCollector = new System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>();

        // Alias binding: substitute --bind-input/--bind-output onto the job dictionary before
        // validation, so a bound fragment is checked under exactly the same rules as a
        // complete job (indexed by alias, whether it came from --job or the command line).
        var bindingErrors = AliasBindingApplier.Apply(jobs, globals.BindInput, globals.BindOutput);
        if (bindingErrors.Count > 0)
        {
            if (!globals.Quiet)
                host.ShowRefusal(RunRefusal.Binding, bindingErrors);
            return 1;
        }

        // Validation
        var processorFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
        var validationErrors = PipelineValidator.Validate(dag, jobs, processorFactories);
        if (validationErrors.Any())
        {
            if (!globals.Quiet)
                host.ShowRefusal(RunRefusal.Validation, validationErrors);
            return 1;
        }

        if (dag.IsDag)
        {
            // Dry-run selection logic
            if (globals.DryRunCount > 0 && dag.Branches.Count > 1)
                globals.DryRunInteractiveBranch = host.ChooseSampleBranch(dag);

            if (!globals.Quiet)
                host.ShowTopology(dag);

            var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
            if (!globals.Quiet)
                orchestrator.OnLogEvent = host.ShowDagEvent;

            var fingerprints = DtPipe.Cli.Services.BranchFingerprint.ForDag(dag, jobs, contexts);
            Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor = async (branch, ctx, token) =>
            {
                var job = jobs[branch.Alias];
                contexts.TryGetValue(branch.Alias, out var branchCtx);
                var fingerprinted = ctx with { DefinitionFingerprint = fingerprints.GetValueOrDefault(branch.Alias) };
                var run = new BranchRun(job)
                {
                    Context = branchCtx, Alias = branch.Alias, IsDag = true, Channels = fingerprinted,
                    Results = resultsCollector, Globals = globals, UserCancellation = userCts.Token
                };
                return await RunSingleJobAsync(run, host, token, branchFailures);
            };

            var exitCode = await host.RunDagAsync(dag, globals, () => orchestrator.ExecuteAsync(dag, branchExecutor, ct), ct);

            if (!globals.Quiet)
                host.ShowResults(resultsCollector.ToList(), dag);
            return exitCode;
        }
        else
        {
            var (mainAlias, mainJob) = jobs.First();
            _serviceProvider.GetService<DtPipe.DryRun.SampleReportCollector>()?.LinearBranch = mainAlias;
            if (!globals.Quiet)
                host.ShowTopology(mainJob);

            var mainContext = contexts.Values.FirstOrDefault();

            // The linear path goes through DagOrchestrator too: a single-branch
            // DAG gives uniform cancellation, channel wiring and exit-code semantics.
            // Jobs without an output (validation-only mode) keep the direct path.
            if (!string.IsNullOrEmpty(mainJob.Output))
            {
                var linearDag = new JobDagDefinition
                {
                    Branches = new[] { BranchDefinition.FromJob("main", mainJob, processorFactories) }
                };
                var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();

                Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> linearExecutor =
                    (_, branchCtx, token) => RunSingleJobAsync(
                        new BranchRun(mainJob)
                        {
                            Context = mainContext, Alias = "main", Channels = branchCtx,
                            Globals = globals, UserCancellation = userCts.Token
                        },
                        host, token, branchFailures);

                return await orchestrator.ExecuteAsync(linearDag, linearExecutor, ct);
            }

            return await RunSingleJobAsync(
                new BranchRun(mainJob) { Context = mainContext, Globals = globals, UserCancellation = userCts.Token },
                host, ct, branchFailures);
        }
    }

    private async Task<int> RunSingleJobAsync(BranchRun run, IRunHost host, CancellationToken ct,
        System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures)
    {
        var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
        registry.BeginScope();

        var channelRegistry = _serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var linearPipelineService = new LinearPipelineService(_contributors, _serviceProvider, channelRegistry, registry, host);
        var exitCode = await linearPipelineService.ExecuteAsync(run, ct);
        if (branchFailures is not null && linearPipelineService.Failure is { } why)
            branchFailures[run.Alias ?? "main"] = why;
        return exitCode;
    }
}
