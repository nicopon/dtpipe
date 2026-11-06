using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Security;
using DtPipe.Core.Validation;
using Microsoft.Extensions.Logging;
using Apache.Arrow;
using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Pipelines;
using DtPipe.Configuration;
using Apache.Arrow.Types;
using DtPipe.Services;

namespace DtPipe.Engine;

public class ExportService
{
    private readonly ILogger<ExportService> _logger;
    private readonly IExportObserver _observer;
    private readonly ExportRunServices _runServices;
    internal const int HookTimeoutSeconds = 30;

    public ExportService(
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataWriterFactory> writerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        OptionsRegistry optionsRegistry,
        IExportObserver observer,
        ILogger<ExportService> logger,
        HookExecutor hookExecutor,
        MetricsService metricsService,
        SchemaValidationService schemaValidator,
        PipelineExecutor pipelineExecutor,
        IMemoryChannelRegistry? channelRegistry = null,
        DtPipe.DryRun.SampleReportCollector? reports = null)
    {
        _logger = logger;
        _observer = observer;
        _runServices = new ExportRunServices(observer, logger, hookExecutor, metricsService, schemaValidator,
            pipelineExecutor, channelRegistry, reports);
    }

    public async Task RunExportAsync(
        PipelineOptions options,
        string providerName,
        string outputPath,
        CancellationToken ct,
        List<IDataTransformer> pipeline,
        IStreamReaderFactory readerFactory,
        IDataWriterFactory writerFactory,
        OptionsRegistry registry,
        string? alias = null,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
        bool quiet = false)
    {
        // Ensure the registry has the correct pipeline options for this run
        registry.Register(options);
        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Starting export from {Provider} to {OutputPath}", providerName, ConnectionStringSanitizer.Redact(outputPath));

        // A run started for a program (the MCP tools) suppresses the chatter, even under DEBUG,
        // because its console feeds a JSON reply or a screen the agent's toolkit owns.
        bool silenceInternal = quiet;
        bool outputIsStdio = string.Equals(outputPath, "-", StringComparison.Ordinal);

        var state = new ExportRunState(_runServices, options, providerName, outputPath, pipeline, readerFactory, writerFactory,
            registry, alias, resultsCollector, silenceInternal, outputIsStdio);
        await state.RunAsync(ct);
    }

    /// <summary>
    /// Injects a compact Arrow schema JSON string into the reader's registered options via
    /// <c>Schema</c> (preferred) or, for options that take column-type hints, as those hints.
    /// </summary>
    internal static void InjectSchema(IStreamReaderFactory readerFactory, OptionsRegistry registry, string schemaJson)
    {
        var optType = readerFactory.GetSupportedOptionTypes().FirstOrDefault();
        if (optType == null) return;
        var opts = registry.Get(optType);

        if (opts is IHasSchemaOverride schemaOverride && string.IsNullOrEmpty(schemaOverride.Schema))
        {
            schemaOverride.Schema = schemaJson;
            registry.RegisterByType(optType, opts);
            return;
        }

        // Flat, row-based readers (CSV): the scalar columns of the schema, as hints.
        if (opts is IHasColumnTypeHints typed && string.IsNullOrEmpty(typed.ColumnTypes))
        {
            try
            {
                var schema = ArrowSchemaSerializer.Deserialize(schemaJson);
                var hints = schema.FieldsList
                    .Select(f => (f.Name, Hint: DtPipe.Core.Helpers.TypeHelper.HintFor(f)))
                    .Where(x => !string.IsNullOrEmpty(x.Hint))
                    .Select(x => $"{x.Name}:{x.Hint}");
                var columnTypes = string.Join(",", hints);
                if (!string.IsNullOrEmpty(columnTypes))
                {
                    typed.ColumnTypes = columnTypes;
                    registry.RegisterByType(optType, opts);
                }
            }
            catch { /* best-effort */ }
        }
    }

    internal static Schema EvolveSchema(Schema original, IReadOnlyList<PipeColumnInfo> transformed)
    {
        var fields = new List<Field>(transformed.Count);
        foreach (var col in transformed)
        {
            // Try to find matching field in original schema to preserve nested types
            var originalField = original.FieldsList.FirstOrDefault(f => string.Equals(f.Name, col.Name, StringComparison.OrdinalIgnoreCase));
            if (originalField != null)
            {
                fields.Add(originalField);
            }
            else
            {
                // New field (e.g. from --fake), map from CLR type and the width the column declares
                fields.Add(DtPipe.Core.Infrastructure.Arrow.ArrowTypeMapper.GetField(
                    col.Name, col.ClrType, col.IsNullable, col.Precision, col.Scale));
            }
        }
        return new Schema(fields, null);
    }

    internal static PipelineExecutionPlan BuildExecutionPlan(
        string readerName,
        IStreamReader reader,
        string writerName,
        IDataWriter? writer,
        List<IDataTransformer> pipeline,
        List<PipelineSegment> segments)
    {
        bool readerIsColumnar = reader is IColumnarStreamReader;
        bool writerIsColumnar = writer is IColumnarDataWriter;
        bool takesEitherShape = writer is IEitherShapeWriter;
        // A writer that takes either shape stays in the shape the chain is in, which ExecuteSegmentedPipelineAsync
        // decides from the reader and the segments.
        bool rowModePreferred = takesEitherShape
            ? !readerIsColumnar || (segments.Count > 0 && !segments.Any(s => s.IsColumnar))
            : !writerIsColumnar;

        var steps = new List<PipelineExecutionStep>(pipeline.Count);
        foreach (var segment in segments)
        {
            bool willRunColumnar = segment.IsColumnar && !rowModePreferred;
            foreach (var t in segment.Transformers)
            {
                steps.Add(new PipelineExecutionStep(
                    t.GetType().Name.Replace("DataTransformer", ""),
                    segment.IsColumnar,
                    willRunColumnar));
            }
        }

        // Count mode-transition bridges
        int bridges = 0;
        bool current = readerIsColumnar && !rowModePreferred;
        foreach (var segment in segments)
        {
            bool useColumnar = segment.IsColumnar && !rowModePreferred;
            if (useColumnar != current) { bridges++; current = useColumnar; }
        }
        if (!takesEitherShape && writerIsColumnar != current) bridges++;

        return new PipelineExecutionPlan(readerName, readerIsColumnar, writerName, writerIsColumnar, rowModePreferred, steps, bridges);
    }
}
