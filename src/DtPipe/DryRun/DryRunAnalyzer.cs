using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.DryRun;

/// <summary>
/// Result of a dry-run analysis.
/// </summary>
public record DryRunResult(
    List<SampleTrace> Samples,
    List<string> StepNames,
    SchemaCompatibilityReport? CompatibilityReport,
    string? SchemaInspectionError
);

/// <summary>
/// Analyzes a data pipeline by running a sample of data through it.
/// </summary>
public class DryRunAnalyzer
{
    private const int DefaultSampleLimit = 1000;

    /// <summary>
    /// Executes the dry run analysis.
    /// </summary>
    /// <param name="reader">Input data reader</param>
    /// <param name="pipeline">List of transformers</param>
    /// <param name="sampleCount">Number of samples to collect</param>
    /// <param name="inspector">Optional schema inspector for target validation</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Analysis result containing samples and compatibility info</returns>
    public async Task<DryRunResult> AnalyzeAsync(
        IStreamReader reader,
        List<IDataTransformer> pipeline,
        int sampleCount,
        ISchemaInspector? inspector = null,
        CancellationToken ct = default)
    {
        // 1. Capture schema evolution through pipeline
        var traceSchemas = new List<IReadOnlyList<ColumnInfo>>();
        var stepNames = new List<string>();

        var simSchema = reader.Columns ?? throw new InvalidOperationException("Reader columns must be initialized before analysis.");
        traceSchemas.Add(simSchema); // Input step

        foreach (var t in pipeline)
        {
            simSchema = await t.InitializeAsync(simSchema, ct);
            traceSchemas.Add(simSchema);
            stepNames.Add(t.GetType().Name.Replace("DataTransformer", ""));
        }

        // 2. Collect samples and process each through pipeline
        var samples = new List<SampleTrace>();
        int collected = 0;
        int batchLimit = Math.Min(sampleCount, DefaultSampleLimit);

        await foreach (var batch in reader.ReadBatchesAsync(batchLimit, ct))
        {
            for (int i = 0; i < batch.Length && collected < sampleCount; i++)
            {
                var row = batch.Span[i].ToArray();
                var trace = ProcessRowThroughPipeline(row, pipeline, traceSchemas);
                samples.Add(trace);
                collected++;
            }

            if (collected >= sampleCount) break;
        }

        // 3. Schema Compatibility Analysis (if inspector provided)
        SchemaCompatibilityReport? compatibilityReport = null;
        string? schemaInspectionError = null;

        if (inspector != null && samples.Count > 0)
        {
            try
            {
                var targetSchema = await inspector.InspectTargetAsync(ct);
                var finalSourceSchema = traceSchemas.Last();
                compatibilityReport = SchemaCompatibilityAnalyzer.Analyze(finalSourceSchema, targetSchema);
            }
            catch (Exception ex)
            {
                schemaInspectionError = ex.Message;
            }
        }

        return new DryRunResult(samples, stepNames, compatibilityReport, schemaInspectionError);
    }

    private SampleTrace ProcessRowThroughPipeline(
        object?[] inputRow,
        List<IDataTransformer> pipeline,
        List<IReadOnlyList<ColumnInfo>> traceSchemas)
    {
        var stages = new List<StageTrace>();
        
        // Input Trace
        var currentValues = (object?[])inputRow.Clone();
        stages.Add(new StageTrace(traceSchemas[0], currentValues));

        var currentRow = inputRow;
        
        // Pipeline Traces
        for (int i = 0; i < pipeline.Count; i++)
        {
            var transformer = pipeline[i];
            var schema = traceSchemas[i + 1]; // +1 because 0 is input

            currentRow = transformer.Transform(currentRow);
            var nextValues = (object?[])currentRow.Clone();
            
            stages.Add(new StageTrace(schema, nextValues));
        }

        return new SampleTrace(stages);
    }
}
