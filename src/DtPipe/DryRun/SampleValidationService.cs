using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.DryRun;

/// <summary>
/// Checks a sample of the pipeline's final rows against the target's declared constraints:
/// the primary key the write strategy needs, and the NOT NULL / UNIQUE columns the target
/// already has.
///
/// This is validation, not execution. It was carried by the old dry-run analyser alongside a
/// second row-walking engine; the engine is gone, and this half moved here unchanged so the
/// suites that cover it keep covering the same behaviour.
/// </summary>
public static class SampleValidationService
{
    public static ConstraintValidationResult ValidateDataConstraints(
        IReadOnlyList<object?[]> finalRows,
        IReadOnlyList<PipeColumnInfo> finalSchema,
        TargetSchemaInfo targetInfo,
        ISqlDialect? dialect)
    {
        var errors = new List<string>();
        var warnings = new List<string>();

        if (finalRows.Count == 0 || targetInfo.Columns.Count == 0) return new ConstraintValidationResult(errors, warnings);

        var colMap = finalSchema.Select((c, i) => (c.Name, i)).ToDictionary(x => x.Name, x => x.i, StringComparer.OrdinalIgnoreCase);

        foreach (var targetCol in targetInfo.Columns)
        {
            if (!targetCol.IsNullable)
            {
                var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(targetCol.Name, finalSchema, c => c.Name);
                if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
                {
                    if (finalRows.Any(vals => vals != null && srcIdx < vals.Length && (vals[srcIdx] == null || vals[srcIdx] == DBNull.Value)))
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
                var match = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(uniqueColName, finalSchema, c => c.Name);
                if (match != null && colMap.TryGetValue(match.Name, out int srcIdx))
                {
                    var seen = new HashSet<object>();
                    bool hasDuplicates = false;
                    foreach (var vals in finalRows)
                    {
                        if (vals != null && srcIdx < vals.Length && vals[srcIdx] is object val && val != DBNull.Value)
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

    public static KeyValidationResult ValidatePrimaryKeys(
        IKeyValidator validator,
        IReadOnlyList<PipeColumnInfo> finalSchema,
        TargetSchemaInfo? targetInfo,
        ISqlDialect? dialect)
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
}
