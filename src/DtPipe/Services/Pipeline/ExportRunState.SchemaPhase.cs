using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Pipelines;
using DtPipe.Configuration;
using DtPipe.DryRun;

namespace DtPipe.Services.Pipeline;

// ── Schema phase (P1-8): transformer initialization, segmentation support,
//    branch schema publication and dry-run inference advisory. ──────────────
internal sealed partial class ExportRunState
{
    internal async Task LoadPersistedSchemaAsync(CancellationToken retryCt)
    {
        var readerSchemaPersist = Registry.Get(ReaderFactory.OptionsType) as ISchemaPersistenceAware;
        var schemaLoadName = readerSchemaPersist?.SchemaLoad;
        if (string.IsNullOrEmpty(schemaLoadName)) return;

        var loadedSchema = SchemaStore.Load(schemaLoadName);
        if (loadedSchema != null)
            ExportService.InjectSchema(ReaderFactory, Registry, ArrowSchemaSerializer.SerializeCompact(loadedSchema));
        else
            _svc._logger.LogWarning("Schema file '{Name}' not found — falling back to inference.", schemaLoadName);
    }

    internal async Task InitializeTransformersAsync(CancellationToken retryCt)
    {
        CurrentSchema = Reader.Columns ?? System.Array.Empty<PipeColumnInfo>();
        TransformerSchemas = new Dictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>();

        if (Pipeline.Count == 0) return;

        var transformerNames = Pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", ""));
        if (ShowStatusMessages && !SilenceInternal) Observer.ShowPipeline(transformerNames);

        foreach (var t in Pipeline)
        {
            var inputSchema = CurrentSchema;
            CurrentSchema = await t.InitializeAsync(CurrentSchema, retryCt);
            TransformerSchemas[t] = (inputSchema, CurrentSchema);
        }
    }

    internal void FillSegmentSchemas()
    {
        if (Pipeline.Count == 0) return;
        foreach (var segment in Segments)
        {
            segment.InputSchema = TransformerSchemas.Count > 0 && segment.Transformers.Count > 0
                ? TransformerSchemas[segment.Transformers[0]].In
                : Reader.Columns ?? System.Array.Empty<PipeColumnInfo>();

            segment.OutputSchema = TransformerSchemas.Count > 0 && segment.Transformers.Count > 0
                ? TransformerSchemas[segment.Transformers[^1]].Out
                : CurrentSchema;
        }
    }

    internal void PublishBranchSchema()
    {
        if (string.IsNullOrEmpty(Alias) || _svc._channelRegistry == null || !_svc._channelRegistry.ContainsChannel(Alias))
            return;

        Schema? sourceSchema = (Reader as IStreamTransformer)?.Schema ?? (Reader as IColumnarStreamReader)?.Schema;
        if (!string.IsNullOrEmpty(Alias) && sourceSchema != null)
        {
            var evolvedSchema = ExportService.EvolveSchema(sourceSchema, CurrentSchema);
            _svc._channelRegistry.UpdateArrowChannelSchema(Alias, evolvedSchema);
        }
        _svc._channelRegistry.UpdateChannelColumns(Alias, CurrentSchema ?? System.Array.Empty<PipeColumnInfo>());
    }

    internal async Task InferAdvisoryAsync(CancellationToken retryCt)
    {
        if (Options.DryRunCount <= 0 || Reader is not IColumnTypeInferenceCapable inferCapable) return;
        try
        {
            var sampleCount = Math.Max(Options.DryRunCount, 100);
            var suggested = await inferCapable.InferColumnTypesAsync(sampleCount, retryCt);
            if (suggested.Count > 0 && !SilenceInternal)
                Observer.ShowColumnTypeInferenceSuggestion(suggested, sampleCount);
        }
        catch { /* inference is best-effort, never fail the dry-run */ }
    }

    /// <summary>
    /// The content-addressed name for this branch's prefix — the definition that produces the
    /// rows, not the alias that labels them (see CheckpointKey).
    /// </summary>
    internal string ComputeCheckpointKey()
    {
        var route = Registry.TryGetByType(typeof(DtPipe.Cli.Infrastructure.ConnectionRoute), out var routeObj)
            ? routeObj as DtPipe.Cli.Infrastructure.ConnectionRoute
            : null;

        var query = (Registry.Get(ReaderFactory.OptionsType) as DtPipe.Core.Options.IQueryAwareOptions)?.Query;

        return DtPipe.Sessions.CheckpointKey.Compute(
            route?.Input ?? ProviderName,
            query,
            Pipeline.Select(t => t.GetType().FullName ?? t.GetType().Name),
            Options.SamplingRate,
            Options.SamplingSeed,
            Options.Limit,
            Options.BatchSize,
            Options.MaxBatchBytes,
            segmentIndex: Segments.Count);
    }

    /// <summary>
    /// Assembles what the run observed into a report and hands it to the observer to render.
    /// Everything here is derived from the execution that already happened — the target
    /// inspection captured during writer preparation, the tap's capture, and the validators.
    /// Nothing re-reads the source.
    /// </summary>
    internal async Task RenderSampleReportAsync(CancellationToken retryCt)
    {
        if (SampleResult is null) return;

        var report = SampleReportBuilder.Build(
            SampleResult,
            Pipeline.Select(t => t.GetType().Name.Replace("DataTransformer", "")).ToList(),
            (Writer as IHasSqlDialect)?.Dialect,
            Writer as IKeyValidator,
            InspectedTarget,
            TargetInspectionError,
            SampleTap?.TypeHints,
            SafetyVerdict?.Enforcement ?? DtPipe.Sessions.ReadOnlyEnforcement.VerbScanOnly,
            Alias,
            CheckpointKey,
            ContractPath);

        var executionPlan = ExportService.BuildExecutionPlan(ProviderName, Reader, WriterFactory.ComponentName, Writer, Pipeline, Segments);

        bool isInteractive = string.IsNullOrEmpty(Options.DryRunInteractiveBranch)
            || string.Equals(Alias, Options.DryRunInteractiveBranch, StringComparison.OrdinalIgnoreCase);

        await Observer.RenderSampleReportAsync(report, executionPlan, isInteractive && !SilenceInternal, retryCt);
    }
}
