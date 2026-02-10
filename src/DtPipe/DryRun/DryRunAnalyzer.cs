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
	KeyValidationResult? KeyValidation = null,
	ConstraintValidationResult? ConstraintValidation = null
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
		var traceSchemas = new List<IReadOnlyList<PipeColumnInfo>>();
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

		// 4. Primary Key Validation
		KeyValidationResult? keyValidation = null;

		// 5. Data Constraint Validation
		ConstraintValidationResult? constraintValidation = null;

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

					// 4. Primary Key Validation
					if (inspector is IKeyValidator keyValidator)
					{
						keyValidation = ValidatePrimaryKeys(keyValidator, finalSourceSchema, targetSchema, dialect);
					}

					// 5. Data Constraint Validation
					if (targetSchema != null && targetSchema.Exists)
					{
						constraintValidation = ValidateDataConstraints(samples, targetSchema, dialect);
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
			// No inspector, but purely validating key presence in schema
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
			keyValidation,
			constraintValidation);
	}

	private ConstraintValidationResult ValidateDataConstraints(
		List<SampleTrace> samples,
		TargetSchemaInfo targetInfo,
		ISqlDialect? dialect)
	{
		var errors = new List<string>();
		var warnings = new List<string>();

		if (samples.Count == 0 || targetInfo.Columns.Count == 0)
		{
			return new ConstraintValidationResult(errors, warnings);
		}

		// We use the FINAL stage of the sample (what is about to be written)
		var sampleSchema = samples[0].Stages.Last().Schema;

		// Cache column indices for faster lookup
		// We match Sample Column Name -> Target Column Name
		// (Using ColumnMatcher with fuzzy matching)

		var colMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < sampleSchema.Count; i++)
		{
			colMap[sampleSchema[i].Name] = i;
		}

		// 1. Validate NOT NULL Constraints against Sample
		foreach (var targetCol in targetInfo.Columns)
		{
			if (!targetCol.IsNullable)
			{
				// Find corresponding source column
				var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(
					targetCol.Name,
					sampleSchema,
					c => c.Name);

				if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
				{
					// Check all samples for NULL in this column
					// If Values is null, the row was filtered out, so it doesn't violate NOT NULL
					bool hasNull = samples.Any(s =>
					{
						var vals = s.Stages.Last().Values;
						if (vals == null) return false;
						return vals[srcIdx] == null || vals[srcIdx] == DBNull.Value;
					});

					if (hasNull)
					{
						errors.Add($"Column '{targetCol.Name}' is NOT NULL in target but contains NULL values in sample data.");
					}
				}
			}
		}

		// 2. Validate UNIQUE Constraints (Duplicates in Sample)
		if (targetInfo.UniqueColumns != null)
		{
			foreach (var uniqueColName in targetInfo.UniqueColumns)
			{
				var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(
					uniqueColName,
					sampleSchema,
					c => c.Name);

				if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
				{
					// Check for duplicates in sample
					var seen = new HashSet<object>();
					bool hasDuplicates = false;
					foreach (var s in samples)
					{
						var vals = s.Stages.Last().Values;
						if (vals == null) continue; // Skip filtered rows

						var val = vals[srcIdx];
						if (val != null && val != DBNull.Value)
						{
							if (!seen.Add(val))
							{
								hasDuplicates = true;
								break;
							}
						}
					}

					if (hasDuplicates)
					{
						warnings.Add($"Column '{uniqueColName}' is UNIQUE in target but sample contains duplicates. Only the first occurrence will succeed (depending on strategy).");
					}
				}
			}
		}

		return new ConstraintValidationResult(errors, warnings);
	}

	private KeyValidationResult ValidatePrimaryKeys(
		IKeyValidator validator,
		IReadOnlyList<PipeColumnInfo> finalSchema,
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

		// 3. Cross-Validate against Target Table
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

			// Warning for extra keys (not present in target PK but provided by user)
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
		List<IReadOnlyList<PipeColumnInfo>> traceSchemas)
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

			if (currentRow == null)
			{
				// Row consumed or filtered out
				stages.Add(new StageTrace(schema, null)); // Indicate no output
				break;
			}

			var nextValues = (object?[])currentRow.Clone();

			stages.Add(new StageTrace(schema, nextValues));
		}

		return new SampleTrace(stages);
	}
}
