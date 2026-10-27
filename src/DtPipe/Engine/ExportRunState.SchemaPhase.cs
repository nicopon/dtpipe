using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.Logging;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Pipelines;
using DtPipe.Configuration;
using DtPipe.DryRun;

namespace DtPipe.Engine;

// ── Schema phase: transformer initialization, segmentation support,
//    branch schema publication and dry-run inference advisory. ──────────────
internal sealed partial class ExportRunState
{
    internal async Task LoadPersistedSchemaAsync(CancellationToken ct)
    {
        var readerSchemaPersist = Registry.Get(ReaderFactory.OptionsType) as ISchemaPersistenceAware;
        var schemaLoadName = readerSchemaPersist?.SchemaLoad;
        if (string.IsNullOrEmpty(schemaLoadName)) return;

        var loadedSchema = SchemaStore.Load(schemaLoadName)
            ?? throw new FileNotFoundException($"Schema file '{schemaLoadName}' could not be found.");

        ExportService.InjectSchema(ReaderFactory, Registry, ArrowSchemaSerializer.SerializeCompact(loadedSchema));
    }

    internal void SavePersistedSchema()
    {
        var readerSchemaPersist = Registry.Get(ReaderFactory.OptionsType) as ISchemaPersistenceAware;
        var schemaSaveName = readerSchemaPersist?.SchemaSave;
        if (string.IsNullOrEmpty(schemaSaveName) || ReaderFactory is IStreamProcessorSource)
            return;

        var schema = (Reader as IColumnarStreamReader)?.Schema;
        if (schema is { FieldsList.Count: > 0 })
        {
            SchemaStore.Save(schemaSaveName, ProviderName, schema);
            Logger.LogInformation("Schema saved: {Name}.dtschema ({Count} fields)", schemaSaveName, schema.FieldsList.Count);
        }
        else if (Reader.Columns is { Count: > 0 })
        {
            var fields = Reader.Columns
                .Select(c => ArrowTypeMapper.GetField(c.Name, c.ClrType, c.IsNullable))
                .ToList();
            var rowSchema = new Schema(fields, null);
            SchemaStore.Save(schemaSaveName, ProviderName, rowSchema);
            Logger.LogInformation("Schema saved: {Name}.dtschema ({Count} fields)", schemaSaveName, fields.Count);
        }
    }

    internal async Task InitializeTransformersAsync(CancellationToken ct)
    {
        CurrentSchema = Reader.Columns ?? System.Array.Empty<PipeColumnInfo>();
        TransformerSchemas = new Dictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>();

        if (Pipeline.Count == 0) return;

        foreach (var t in Pipeline)
        {
            var inputSchema = CurrentSchema;
            CurrentSchema = await t.InitializeAsync(CurrentSchema, ct);
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
        if (string.IsNullOrEmpty(Alias) || _svc.ChannelRegistry == null || !_svc.ChannelRegistry.ContainsChannel(Alias))
            return;

        Schema? sourceSchema = (Reader as IStreamTransformer)?.Schema ?? (Reader as IColumnarStreamReader)?.Schema;
        if (!string.IsNullOrEmpty(Alias) && sourceSchema != null)
        {
            var evolvedSchema = ExportService.EvolveSchema(sourceSchema, CurrentSchema);
            _svc.ChannelRegistry.UpdateArrowChannelSchema(Alias, evolvedSchema);
        }
        // The only publication for a reader with no Arrow schema of its own; the registry keeps
        // a richer schema published above over this flat one.
        _svc.ChannelRegistry.UpdateArrowChannelSchema(Alias, ArrowSchemaFactory.Create(CurrentSchema ?? System.Array.Empty<PipeColumnInfo>()));
    }

    internal async Task InferAdvisoryAsync(CancellationToken ct)
    {
        if (Options.DryRunCount <= 0 || Reader is not IColumnTypeInferenceCapable inferCapable) return;
        try
        {
            var sampleCount = Math.Max(Options.DryRunCount, 100);
            var suggested = await inferCapable.InferColumnTypesAsync(sampleCount, ct);
            if (suggested.Count > 0 && !SilenceInternal)
                Observer.ShowColumnTypeInferenceSuggestion(suggested, sampleCount);
        }
        catch { /* inference is best-effort, never fail the dry-run */ }
    }

    /// <summary>
    /// The content-addressed name for this branch's prefix — the definition that produces the
    /// rows, not the alias that labels them (see CheckpointKey). What the run reads (the
    /// connection, the query, the effective limit) comes from the resolved run; what defines the
    /// transformers, a processor and the branches upstream comes from the branch's fingerprint.
    /// </summary>
    internal string ComputeCheckpointKey()
    {
        var route = Registry.TryGetByType(typeof(DtPipe.Cli.Infrastructure.ConnectionRoute), out var routeObj)
            ? routeObj as DtPipe.Cli.Infrastructure.ConnectionRoute
            : null;

        var query = (Registry.Get(ReaderFactory.OptionsType) as DtPipe.Core.Options.IQueryAwareOptions)?.Query;

        return DtPipe.Sessions.CheckpointKey.Compute(
            ProviderName,
            // A non-source reader's route is the alias of the channel it reads: a label, and the
            // upstream's definition (the fingerprint) already stands for what it carries.
            ReaderFactory.IsSource ? route?.Input : null,
            query,
            Pipeline.Select(t => t.GetType().FullName ?? t.GetType().Name),
            Options.DefinitionFingerprint,
            Options.SamplingRate,
            Options.SamplingSeed,
            EffectiveLimit,
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
    internal async Task RenderSampleReportAsync(CancellationToken ct)
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

        await Observer.RenderSampleReportAsync(report, executionPlan, isInteractive && !SilenceInternal, ct);
    }
}
