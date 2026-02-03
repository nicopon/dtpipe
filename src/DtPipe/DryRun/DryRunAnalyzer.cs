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
        
        // 4. Primary Key Validation (Phase 2 - Enhanced)
        KeyValidationResult? keyValidation = null;

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
                    // CRITICAL: Capture target info for PK validation
                    var targetSchema = await inspector.InspectTargetAsync(ct);
                    var finalSourceSchema = traceSchemas.Last();
                    
                    compatibilityReport = SchemaCompatibilityAnalyzer.Analyze(finalSourceSchema, targetSchema, dialect);
                    
                    // 4. Primary Key Validation (Phase 2 - Enhanced)
                    if (inspector is IKeyValidator keyValidator)
                    {
                        keyValidation = ValidatePrimaryKeys(keyValidator, finalSourceSchema, targetSchema, dialect);
                    }
                }
                catch (Exception ex)
                {
                    schemaInspectionError = ex.Message;
                }
            }
        }
        else
        {
            // No inspector, but purely validating key presence in schema (Phase 1 behavior)
            if (inspector is IKeyValidator keyValidator && samples.Count > 0)
            {
                var finalSchema = traceSchemas.Last();
                keyValidation = ValidatePrimaryKeys(keyValidator, finalSchema, null, dialect);
            }
        }

        return new DryRunResult(
            samples, 
            stepNames, 
            compatibilityReport, 
            schemaInspectionError, 
            dialect,
            keyValidation); 
    }

    private KeyValidationResult ValidatePrimaryKeys(
        IKeyValidator validator,
        IReadOnlyList<ColumnInfo> finalSchema,
        TargetSchemaInfo? targetInfo,
        ISqlDialect? dialect)
    {
        var isRequired = validator.RequiresPrimaryKey();
        var requestedKeys = validator.GetRequestedPrimaryKeys();
        
        // Default result containers
        var resolvedKeys = new List<string>();
        var errors = new List<string>();
        var warnings = new List<string>();
        
        // 1. Check Requirement
        if (!isRequired)
        {
            return new KeyValidationResult(false, requestedKeys, null, null, null, null);
        }
        
        if (requestedKeys == null || requestedKeys.Count == 0)
        {
            errors.Add($"Strategy '{validator.GetWriteStrategy()}' requires a primary key. Specify with --key option.");
            return new KeyValidationResult(true, null, null, null, errors, null);
        }
        
        // 2. Validate existence in Flow Schema (Output)
        foreach (var keyName in requestedKeys)
        {
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
                errors.Add($"Key column '{keyName}' not found in final schema. Available columns: {available}");
            }
        }

        // If schema validation failed, stop here
        if (errors.Count > 0)
        {
            return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, errors, null);
        }

        // 3. Cross-Validate against Target Table (Phase 2)
        // Only if target exists and has PKs defined
        if (targetInfo != null && targetInfo.Exists && targetInfo.PrimaryKeyColumns?.Count > 0)
        {
            var targetPKs = targetInfo.PrimaryKeyColumns;
            
            // Normalize resolved keys to physical names for comparison
            var physicalResolvedKeys = resolvedKeys.Select(k => 
                Core.Helpers.ColumnMatcher.ResolvePhysicalName(k, false, dialect)) // Assuming output cols are not case sensitive by default
                .ToHashSet(StringComparer.OrdinalIgnoreCase); // Use set for easy lookup

            // Check 1: Do we have all target PK columns?
            // "Target requires (A, B) but user provided (A)" -> Error
            var missingInUser = new List<string>();
            foreach (var targetKey in targetPKs)
            {
                // We need to check if any physical resolved key matches this target key
                // Since we normalized both (conceptually), we can check existence
                // Note: ResolvePhysicalName might produce quoted names depending on dialect.
                // TargetPKs from InspectTargetAsync should be raw names (usually).
                
                // Let's assume loose matching to be safe: 
                // Does any resolved key normalize to this target key?
                bool found = resolvedKeys.Any(rk => 
                {
                    var phys = Core.Helpers.ColumnMatcher.ResolvePhysicalName(rk, false, dialect);
                    return phys.Equals(targetKey, StringComparison.OrdinalIgnoreCase);
                });

                if (!found)
                {
                    missingInUser.Add(targetKey);
                }
            }

            if (missingInUser.Count > 0)
            {
                errors.Add($"Target table primary key requires columns: {string.Join(", ", targetPKs)}. Missing: {string.Join(", ", missingInUser)}.");
            }

            // Check 2: Do we have extra keys?
            // "Target requires (A) but user provided (A, B)" -> Warning (or Error depending on strictness)
            // Usually, using extra columns for key matching in Upsert is actually OK (it just becomes a more specific filter), 
            // BUT for actual database constraints, it might be misleading.
            // Let's treat it as a warning for now.
             var extraInUser = new List<string>();
             foreach (var rk in resolvedKeys)
             {
                 var phys = Core.Helpers.ColumnMatcher.ResolvePhysicalName(rk, false, dialect);
                 if (!targetPKs.Contains(phys, StringComparer.OrdinalIgnoreCase))
                 {
                     extraInUser.Add(rk);
                 }
             }

             if (extraInUser.Count > 0)
             {
                 warnings.Add($"User key includes columns not present in target primary key: {string.Join(", ", extraInUser)}. Upsert may behave unexpectedly.");
             }

             return new KeyValidationResult(true, requestedKeys, resolvedKeys, targetPKs, errors.Count > 0 ? errors : null, warnings.Count > 0 ? warnings : null);
        }
        else if (targetInfo != null && targetInfo.Exists && (targetInfo.PrimaryKeyColumns == null || targetInfo.PrimaryKeyColumns.Count == 0))
        {
            // Table exists but has NO PK.
            // Upserting into a table without PK is dangerous/impossible efficiently.
            warnings.Add("Target table has no primary key defined. Upsert strategy may degrade to Insert or fail.");
             return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, errors.Count > 0 ? errors : null, warnings);
        }

        return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, errors.Count > 0 ? errors : null, null);
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

