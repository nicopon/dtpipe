using System.Globalization;
using System.Text;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Core.Helpers;

public static class BatchFailureAnalyzer
{
	public static async Task<string?> AnalyzeAsync(
		ISchemaInspector schemaInspector,
		IReadOnlyList<object?[]> rows,
		IReadOnlyList<PipeColumnInfo> sourceColumns,
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

			// 2. Build Mapping: Source Index -> Target Column Info (By Fuzzy Name)
			var columnMap = new TargetColumnInfo?[sourceColumns.Count];
			for (int i = 0; i < sourceColumns.Count; i++)
			{
				var srcCol = sourceColumns[i];
				columnMap[i] = targetSchema.Columns.FirstOrDefault(tc => IsFuzzyMatch(srcCol.Name, tc.Name));
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
						// Skip analysis.
						continue;
					}

					if (val == null || val == DBNull.Value) continue;

					try
					{
						var rawTargetType = targetCol.InferredClrType ?? typeof(object);
						var targetType = Nullable.GetUnderlyingType(rawTargetType) ?? rawTargetType;

                        // Use same converter as the writer for consistent analysis
                        ValueConverter.ConvertValue(val, targetType);
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
						for (int i = 0; i < sourceColumns.Count; i++)
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

	private static bool IsFuzzyMatch(string name1, string name2)
	{
		if (string.Equals(name1, name2, StringComparison.OrdinalIgnoreCase)) return true;
		string Normalize(string s) => s.Replace("_", "").Replace(" ", "").ToLowerInvariant();
		return string.Equals(Normalize(name1), Normalize(name2), StringComparison.Ordinal);
	}
}
