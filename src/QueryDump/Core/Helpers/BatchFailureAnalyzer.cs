using System.Globalization;
using System.Text;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;

namespace QueryDump.Core.Helpers;

public static class BatchFailureAnalyzer
{
    public static async Task<string?> AnalyzeAsync(
        ISchemaInspector schemaInspector, 
        IReadOnlyList<object?[]> rows, 
        IReadOnlyList<ColumnInfo> sourceColumns, 
        CancellationToken ct = default)
    {
        try 
        {
            // 1. Fetch Target Schema to know what the DB expects
            var targetSchema = await schemaInspector.InspectTargetAsync(ct);
            if (targetSchema == null || targetSchema.Columns.Count == 0) 
            {
                return "Could not retrieve target schema definition from database. Cannot perform deep analysis.";
            }

            // 2. Build Mapping: Source Index -> Target Column Info (By Name)
            // We assume name-based mapping is the standard for safety.
            var columnMap = new TargetColumnInfo?[sourceColumns.Count];
            for (int i = 0; i < sourceColumns.Count; i++)
            {
                var srcCol = sourceColumns[i];
                // Find target column with same name (Case Insensitive)
                columnMap[i] = targetSchema.Columns.FirstOrDefault(tc => 
                    string.Equals(tc.Name, srcCol.Name, StringComparison.OrdinalIgnoreCase));
            }

            for (int r = 0; r < rows.Count; r++)
            {
                var row = rows[r];
                for (int c = 0; c < sourceColumns.Count; c++)
                {
                    var val = row[c];
                    var sourceCol = sourceColumns[c];
                    var targetCol = columnMap[c];

                    if (targetCol == null) 
                    {
                        // Column exists in source but not in target. 
                        // Usually handled by ignoring or erroring depending on provider.
                        // We skip analysis for it.
                        continue; 
                    }
                    
                    if (val == null || val == DBNull.Value) continue;

                    try
                    {
                        var rawTargetType = targetCol.InferredClrType ?? typeof(object);
                        var targetType = Nullable.GetUnderlyingType(rawTargetType) ?? rawTargetType;

                        // Check 1: Is this a String trying to go into a non-String column?
                        if (val is string s)
                        {
                            if (targetType != typeof(string) && targetType != typeof(object))
                            {
                                // Strict check: Try parsing
                                if (targetType == typeof(decimal) || targetType == typeof(double) || targetType == typeof(float) || 
                                    targetType == typeof(int) || targetType == typeof(long))
                                {
                                     Convert.ChangeType(val, targetType, CultureInfo.InvariantCulture);
                                }
                                else if (targetType == typeof(DateTime) || targetType == typeof(DateTimeOffset))
                                {
                                     DateTime.Parse(s, CultureInfo.InvariantCulture);
                                }
                                else if (targetType == typeof(Guid))
                                {
                                     Guid.Parse(s);
                                }
                                else 
                                {
                                     Convert.ChangeType(val, targetType);
                                }
                            }
                        }
                        else
                        {
                            // Check 2: Value is not null, not string. Type Compatibility?
                            if (val.GetType() != targetType && !targetType.IsAssignableFrom(val.GetType()))
                            {
                                 if (val is IConvertible)
                                 {
                                     Convert.ChangeType(val, targetType);
                                 }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Found a failure!
                        var sb = new StringBuilder();
                        sb.AppendLine($"[Error Analysis] Issue detected at Row {r + 1} (in batch).");
                        sb.AppendLine($"Column Mapping: Source '{sourceCol.Name}' -> Target '{targetCol.Name}'");
                        sb.AppendLine($"Source Type: {sourceCol.ClrType.Name}");
                        sb.AppendLine($"Target Type: {targetCol.InferredClrType?.Name ?? targetCol.NativeType}");
                        sb.AppendLine($"Value: '{val}' ({val?.GetType().Name ?? "null"})");
                        sb.AppendLine($"Error Detail: Value could not be converted to Target Type. {ex.Message}");
                        sb.AppendLine();
                        
                        sb.AppendLine("Row Context:");
                        for(int i=0; i < sourceColumns.Count; i++)
                        {
                            var v = row[i];
                            var sVal = v?.ToString() ?? "NULL";
                            if (sVal.Length > 50) sVal = sVal.Substring(0, 47) + "...";
                            
                            var marker = (i == c) ? " <--- ERROR" : "";
                            sb.AppendLine($"  [{i}] {sourceColumns[i].Name}: {sVal}{marker}");
                        }
                        return sb.ToString(); 
                    }
                }
            }
        }
        catch (Exception ex)
        {
            return $"Analysis failed itself: {ex.Message}";
        }

        return null;
    }
}
