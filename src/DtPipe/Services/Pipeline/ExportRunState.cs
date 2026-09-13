using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using DtPipe.Core.Validation;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Apache.Arrow;
using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Pipelines;
using DtPipe.Configuration;
using Apache.Arrow.Types;
using DtPipe.Services;
using DtPipe.Core.Infrastructure.Retry;

namespace DtPipe.Services.Pipeline;

/// <summary>
/// Carries the per-run state of one export execution through its phases (P1-8):
/// Preflight → Schema → Execution → Hooks/Cursor/Metrics. Phases are partial methods
/// spread across ExportRunState.*.cs so each concern stays a readable unit.
/// </summary>
internal sealed partial class ExportRunState
{
    private readonly ExportService _svc;
    internal readonly PipelineOptions Options;
    internal readonly string ProviderName;
    internal readonly string OutputPath;
    internal readonly List<IDataTransformer> Pipeline;
    internal readonly IStreamReaderFactory ReaderFactory;
    internal readonly IDataWriterFactory WriterFactory;
    internal readonly OptionsRegistry Registry;
    internal readonly string? Alias;
    internal readonly System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? ResultsCollector;
    internal readonly bool ShowStatusMessages;
    internal readonly bool SilenceInternal;
    internal readonly bool OutputIsStdio;

    // Shared phase outputs
    internal IStreamReader Reader = null!;
    internal IDataWriter Writer = null!;
    internal IDataWriter EffectiveWriter = null!;
    internal DtPipe.Core.Cursor.ICursorTracker? CursorTracker;
    internal IReadOnlyList<PipeColumnInfo> CurrentSchema = System.Array.Empty<PipeColumnInfo>();
    internal Dictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)> TransformerSchemas = new();
    internal List<PipelineSegment> Segments = new();
    internal IExportProgress Progress = null!;
    internal DtPipe.Core.Options.IHookAware? WriterHooks;
    internal CancellationTokenSource LinkedCts = null!;

    // ── Sample mode ────────────────────────────────────────────────────────
    // A sample run is the real run on N rows with the writer neutralised, so the state it
    // needs lives here alongside everything else rather than in a parallel object.
    internal DtPipe.DryRun.SampleTapRecorder? SampleTap;
    internal DtPipe.DryRun.SampleRun? SampleResult;
    internal DtPipe.Core.Models.TargetSchemaInfo? InspectedTarget;
    internal string? TargetInspectionError;

    /// <summary>The content-addressed key this run materialises under, when --checkpoint is set.</summary>
    internal string? CheckpointKey;

    /// <summary>How far this sample run could guarantee it would not write. Reported, never assumed.</summary>
    internal DtPipe.Sessions.SampleSafetyVerdict? SafetyVerdict;

    /// <summary>True when this run reports rather than writes.</summary>
    internal bool IsSampleMode => Options.DryRunCount > 0;

    /// <summary>
    /// Rows the reader is allowed to produce. A sample never reads more than it shows, which
    /// is what keeps it cheap against a large source; an explicit --limit still wins if tighter.
    /// </summary>
    internal int EffectiveLimit => IsSampleMode
        ? (Options.Limit > 0 ? Math.Min(Options.Limit, Options.DryRunCount) : Options.DryRunCount)
        : Options.Limit;

    internal ExportRunState(
        ExportService svc,
        PipelineOptions options,
        string providerName,
        string outputPath,
        List<IDataTransformer> pipeline,
        IStreamReaderFactory readerFactory,
        IDataWriterFactory writerFactory,
        OptionsRegistry registry,
        string? alias,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector,
        bool showStatusMessages,
        bool silenceInternal,
        bool outputIsStdio)
    {
        _svc = svc;
        Options = options;
        ProviderName = providerName;
        OutputPath = outputPath;
        Pipeline = pipeline;
        ReaderFactory = readerFactory;
        WriterFactory = writerFactory;
        Registry = registry;
        Alias = alias;
        ResultsCollector = resultsCollector;
        ShowStatusMessages = showStatusMessages;
        SilenceInternal = silenceInternal;
        OutputIsStdio = outputIsStdio;
    }

    private ILogger Logger => _svc._logger;
    private IExportObserver Observer => _svc._observer;

    internal async Task RunAsync(CancellationToken retryCt)
    {
        // ── Preflight ──────────────────────────────────────────────────────
        Logger.LogDebug("[Preflight] starting export from {Provider} branch {Alias}", ProviderName, Alias ?? "(main)");
        await LoadPersistedSchemaAsync(retryCt);

        Reader = ReaderFactory.Create(Registry);
        try
        {
            try
            {
                await Reader.OpenAsync(retryCt);
            }
            catch (Exception ex) when (AutoBuiltQueryHint(ex) is { } hint)
            {
                throw new InvalidOperationException(hint, ex);
            }

            await RunCoreAsync(retryCt);
        }
        finally
        {
            // Parquet footers and DB writers flush on dispose — never skip it.
            await Reader.DisposeAsync();
            if (Writer is not null) await Writer.DisposeAsync();
        }
    }

    /// <summary>
    /// The message to raise when opening the source failed and its query came from
    /// <c>--table</c>, or <c>null</c> when the user wrote the query themselves and the driver's
    /// own message is already about what they typed.
    /// </summary>
    private string? AutoBuiltQueryHint(Exception ex)
    {
        if (ex is OperationCanceledException) return null;
        if (!Registry.TryGet<AutoBuiltSourceQuery>(out var auto)) return null;

        return $"Reading --table \"{auto.Table}\" failed: {ex.Message.TrimEnd()}" + Environment.NewLine +
               $"dtpipe ran the query it built from that flag: {auto.Query}" + Environment.NewLine +
               "Pass --query to write the statement yourself, or check the name and its schema.";
    }

    private async Task RunCoreAsync(CancellationToken retryCt)
    {
        var columns = Reader.Columns;
        if (columns is null || columns.Count == 0)
        {
            if (!SilenceInternal) Observer.LogWarning("No columns returned by query.");
            return;
        }

        CurrentSchema = columns;

        if (!SilenceInternal && Reader is IColumnTypeInferenceCapable autoCapable
            && autoCapable.AutoAppliedTypes?.Count > 0)
            Observer.ShowColumnTypeInferenceSuggestion(autoCapable.AutoAppliedTypes, 100, applied: true);

        if (ShowStatusMessages && !SilenceInternal)
            Observer.ShowConnectionStatus(true, Reader.Columns?.Count);

        // ── Schema ─────────────────────────────────────────────────────────
        Logger.LogDebug("[Schema] initializing transformers and segmenting pipeline");
        await InitializeTransformersAsync(retryCt);
        Segments = PipelineSegmenter.GetSegments(Pipeline);
        FillSegmentSchemas();
        PublishBranchSchema();
        await InferAdvisoryAsync(retryCt);

        // ── Execution ──────────────────────────────────────────────────────
        // Sample mode is not a branch here. It is the same execution with the writer
        // neutralised and the reader bounded — which is the whole point: a preview that took
        // a different path could disagree with the run it previews, and used to.
        Logger.LogDebug("[Execution] target '{Writer}', {Columns} columns", WriterFactory.ComponentName, CurrentSchema.Count);
        await PrepareWriterAsync(retryCt);
        await ExecutePipelineAsync(retryCt);

        if (IsSampleMode) await RenderSampleReportAsync(retryCt);
    }
}
