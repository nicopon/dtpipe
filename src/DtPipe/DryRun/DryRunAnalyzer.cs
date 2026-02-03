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
    string? SchemaInspectionError,
    ISqlDialect? Dialect = null,
    KeyValidationResult? KeyValidation = null  // Phase 1: Primary key validation
);

/// <summary>
/// Result of primary key validation in dry run.
/// </summary>
/// <param name="IsRequired">Does the strategy require a primary key?</param>
/// <param name="RequestedKeys">Keys specified via --key option (raw user input)</param>
/// <param name="ResolvedKeys">Keys successfully resolved against final schema</param>
/// <param name="Errors">Error messages for unresolved or missing keys</param>
public record KeyValidationResult(
    bool IsRequired,
    IReadOnlyList<string>? RequestedKeys,
    IReadOnlyList<string>? ResolvedKeys,
    IReadOnlyList<string>? Errors
)
{
    /// <summary>
    /// Returns true if validation passed (no errors).
    /// </summary>
    public bool IsValid => Errors == null || Errors.Count == 0;
};

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
        ISqlDialect? dialect = null;

        if (inspector != null)
        {
            if (inspector is IHasSqlDialect hasDialect)
            {
                dialect = hasDialect.Dialect;
            }

            if (samples.Count > 0)
            {
                try
                {
                    var targetSchema = await inspector.InspectTargetAsync(ct);
                    var finalSourceSchema = traceSchemas.Last();
                    compatibilityReport = SchemaCompatibilityAnalyzer.Analyze(finalSourceSchema, targetSchema, dialect);
                }
                catch (Exception ex)
                {
                    schemaInspectionError = ex.Message;
                }
            }
        }

        // 4. Primary Key Validation (Phase 1 - NEW)
        KeyValidationResult? keyValidation = null;
        
        if (inspector is IKeyValidator keyValidator && samples.Count > 0)
        {
            var finalSchema = traceSchemas.Last(); // Schema AFTER all transformations
            keyValidation = ValidatePrimaryKeys(keyValidator, finalSchema, dialect);
        }

        return new DryRunResult(
            samples, 
            stepNames, 
            compatibilityReport, 
            schemaInspectionError, 
            dialect,
            keyValidation);  // Phase 1: Include key validation
    }

    private KeyValidationResult ValidatePrimaryKeys(
        IKeyValidator validator,
        IReadOnlyList<ColumnInfo> finalSchema,
        ISqlDialect? dialect)
    {
        var isRequired = validator.RequiresPrimaryKey();
        var requestedKeys = validator.GetRequestedPrimaryKeys();
        
        if (!isRequired)
        {
            // Strategy doesn't need a key (Recreate, Truncate, Append)
            return new KeyValidationResult(false, null, null, null);
        }
        
        if (requestedKeys == null || requestedKeys.Count == 0)
        {
            // Strategy requires a key but none was provided
            return new KeyValidationResult(
                true,
                null,
                null,
                new[] { $"Strategy '{validator.GetWriteStrategy()}' requires a primary key. Specify with --key option." });
        }
        
        // Validate each requested key
        var resolvedKeys = new List<string>();
        var errors = new List<string>();
        
        foreach (var keyName in requestedKeys)
        {
            // CRITICAL: Use ColumnMatcher for consistency with schema matching
            var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(
                keyName,
                finalSchema,
                c => c.Name);
            
            if (match != null)
            {
                resolvedKeys.Add(match.Name);
            }
            else
            {
                var available = string.Join(", ", finalSchema.Select(c => c.Name));
                errors.Add(
                    $"Key column '{keyName}' not found in final schema. " +
                    $"Available columns after transformations: {available}");
            }
        }
        
        return new KeyValidationResult(isRequired, requestedKeys, resolvedKeys, errors);
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

