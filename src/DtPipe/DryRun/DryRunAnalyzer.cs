using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.DryRun;

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

	public async Task<DryRunResult> AnalyzeAsync(
		IStreamReader reader,
		List<IDataTransformer> pipeline,
		int sampleCount,
		ISchemaInspector? inspector = null,
		CancellationToken ct = default)
	{
		// 1. Initialize schema evolution trace
		var traceSchemas = new List<IReadOnlyList<PipeColumnInfo>>();
		var stepNames = new List<string>();

		var simSchema = reader.Columns ?? throw new InvalidOperationException("Reader columns must be initialized before analysis.");
		traceSchemas.Add(simSchema);

		foreach (var t in pipeline)
		{
			simSchema = await t.InitializeAsync(simSchema, ct);
			traceSchemas.Add(simSchema);
			stepNames.Add(t.GetType().Name.Replace("DataTransformer", ""));
		}

		// 2. Process data samples through pipeline
		var samples = new List<SampleTrace>();
		int collected = 0;
		int batchLimit = Math.Min(sampleCount, DefaultSampleLimit);

		await foreach (var batch in reader.ReadBatchesAsync(batchLimit, ct))
		{
			for (int i = 0; i < batch.Length && collected < sampleCount; i++)
			{
				var row = batch.Span[i].ToArray();
				samples.Add(ProcessRowThroughPipeline(row, pipeline, traceSchemas));
				collected++;
			}
			if (collected >= sampleCount) break;
		}

		// 3. Perform schema and constraint analysis if inspector is available
		SchemaCompatibilityReport? compatibilityReport = null;
		string? schemaInspectionError = null;
		ISqlDialect? dialect = null;
		KeyValidationResult? keyValidation = null;
		ConstraintValidationResult? constraintValidation = null;

		if (inspector != null)
		{
			if (inspector is IHasSqlDialect hasDialect) dialect = hasDialect.Dialect;

			if (samples.Count > 0)
			{
				try
				{
					var targetSchema = await inspector.InspectTargetAsync(ct);
					var finalSourceSchema = traceSchemas.Last();

					compatibilityReport = SchemaCompatibilityAnalyzer.Analyze(finalSourceSchema, targetSchema, dialect);

					if (inspector is IKeyValidator keyValidator)
					{
						keyValidation = ValidatePrimaryKeys(keyValidator, finalSourceSchema, targetSchema, dialect);
					}

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
		else if (inspector is IKeyValidator keyValidator && samples.Count > 0)
		{
			keyValidation = ValidatePrimaryKeys(keyValidator, traceSchemas.Last(), null, dialect);
		}

		return new DryRunResult(samples, stepNames, compatibilityReport, schemaInspectionError, dialect, keyValidation, constraintValidation);
	}

	private ConstraintValidationResult ValidateDataConstraints(List<SampleTrace> samples, TargetSchemaInfo targetInfo, ISqlDialect? dialect)
	{
		var errors = new List<string>();
		var warnings = new List<string>();

		if (samples.Count == 0 || targetInfo.Columns.Count == 0) return new ConstraintValidationResult(errors, warnings);

		var sampleSchema = samples[0].Stages.Last().Schema;
		var colMap = sampleSchema.Select((c, i) => (c.Name, i)).ToDictionary(x => x.Name, x => x.i, StringComparer.OrdinalIgnoreCase);

		foreach (var targetCol in targetInfo.Columns)
		{
			if (!targetCol.IsNullable)
			{
				var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(targetCol.Name, sampleSchema, c => c.Name);
				if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
				{
					if (samples.Any(s => { var vals = s.Stages.Last().Values; return vals != null && (vals[srcIdx] == null || vals[srcIdx] == DBNull.Value); }))
					{
						errors.Add($"Column '{targetCol.Name}' is NOT NULL in target but contains NULL values in sample data.");
					}
				}
			}
		}

		if (targetInfo.UniqueColumns != null)
		{
			foreach (var uniqueColName in targetInfo.UniqueColumns)
			{
				var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(uniqueColName, sampleSchema, c => c.Name);
				if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
				{
					var seen = new HashSet<object>();
					bool hasDuplicates = false;
					foreach (var s in samples)
					{
						var vals = s.Stages.Last().Values;
						if (vals?.GetValue(srcIdx) is object val && val != DBNull.Value)
						{
							if (!seen.Add(val)) { hasDuplicates = true; break; }
						}
					}
					if (hasDuplicates) warnings.Add($"Column '{uniqueColName}' is UNIQUE in target but sample contains duplicates.");
				}
			}
		}

		return new ConstraintValidationResult(errors, warnings);
	}

	private KeyValidationResult ValidatePrimaryKeys(IKeyValidator validator, IReadOnlyList<PipeColumnInfo> finalSchema, TargetSchemaInfo? targetInfo, ISqlDialect? dialect)
	{
		var isRequired = validator.RequiresPrimaryKey();
		var requestedKeys = validator.GetRequestedPrimaryKeys();
		var resolvedKeys = new List<string>();
		var errors = new List<string>();
		var warnings = new List<string>();

		if (!isRequired) return new KeyValidationResult(false, requestedKeys, null, null, null, null);

		if (requestedKeys == null || requestedKeys.Count == 0)
		{
			errors.Add($"Strategy '{validator.GetWriteStrategy()}' requires a primary key. Specify with --key option.");
			return new KeyValidationResult(true, null, null, null, errors, null);
		}

		foreach (var keyName in requestedKeys)
		{
			var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(keyName, finalSchema, c => c.Name);
			if (match != null) resolvedKeys.Add(match.Name);
			else errors.Add($"Key column '{keyName}' not found in final schema. Available columns: {string.Join(", ", finalSchema.Select(c => c.Name))}");
		}

		if (errors.Count > 0) return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, errors, null);

		if (targetInfo != null && targetInfo.Exists && targetInfo.PrimaryKeyColumns?.Count > 0)
		{
			var targetPKs = targetInfo.PrimaryKeyColumns;
			var missingInUser = targetPKs.Where(tpk => !resolvedKeys.Any(rk => Core.Helpers.ColumnMatcher.ResolvePhysicalName(rk, false, dialect).Equals(tpk, StringComparison.OrdinalIgnoreCase))).ToList();

			if (missingInUser.Count > 0) errors.Add($"Target table primary key requires columns: {string.Join(", ", targetPKs)}. Missing: {string.Join(", ", missingInUser)}.");

			var extraInUser = resolvedKeys.Where(rk => !targetPKs.Contains(Core.Helpers.ColumnMatcher.ResolvePhysicalName(rk, false, dialect), StringComparer.OrdinalIgnoreCase)).ToList();
			if (extraInUser.Count > 0) warnings.Add($"User key includes columns not present in target primary key: {string.Join(", ", extraInUser)}.");

			return new KeyValidationResult(true, requestedKeys, resolvedKeys, targetPKs, errors.Count > 0 ? errors : null, warnings.Count > 0 ? warnings : null);
		}
		else if (targetInfo?.Exists == true && (targetInfo.PrimaryKeyColumns == null || targetInfo.PrimaryKeyColumns.Count == 0))
		{
			warnings.Add("Target table has no primary key defined. Upsert strategy may degrade to Insert or fail.");
			return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, null, warnings);
		}

		return new KeyValidationResult(true, requestedKeys, resolvedKeys, null, null, null);
	}

	private SampleTrace ProcessRowThroughPipeline(object?[] inputRow, List<IDataTransformer> pipeline, List<IReadOnlyList<PipeColumnInfo>> traceSchemas)
	{
		var stages = new List<StageTrace>();
		var currentRow = (object?[])inputRow.Clone();
		stages.Add(new StageTrace(traceSchemas[0], currentRow));

		for (int i = 0; i < pipeline.Count; i++)
		{
			var transformer = pipeline[i];
			var schema = traceSchemas[i + 1];
			currentRow = transformer.Transform(currentRow);

			if (currentRow == null)
			{
				stages.Add(new StageTrace(schema, null));
				break;
			}
			stages.Add(new StageTrace(schema, (object?[])currentRow.Clone()));
		}

		return new SampleTrace(stages);
	}
}
