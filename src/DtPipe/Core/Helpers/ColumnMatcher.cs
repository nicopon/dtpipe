using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Centralizes column name matching logic with dialect-aware normalization.
/// </summary>
public static class ColumnMatcher
{
	/// <summary>
	/// Resolves a source column name to its physical representation in the target database.
	/// </summary>
	public static string ResolvePhysicalName(string sourceName, bool isCaseSensitive, ISqlDialect? dialect)
	{
		if (dialect == null) return sourceName;

		// Case-sensitive columns or those requiring quoting preserve exact case
		if (isCaseSensitive || dialect.NeedsQuoting(sourceName))
		{
			return sourceName;
		}

		// Unquoted columns are normalized according to dialect rules (e.g. PG: lower, Oracle: UPPER)
		return dialect.Normalize(sourceName);
	}

	/// <summary>
	/// Finds a target column matching the source name using dialect-aware rules.
	/// </summary>
	public static T? FindMatchingColumn<T>(
		string sourceName,
		bool isCaseSensitive,
		IReadOnlyList<T> targetColumns,
		Func<T, string> getTargetName,
		ISqlDialect? dialect) where T : class
	{
		var physicalName = ResolvePhysicalName(sourceName, isCaseSensitive, dialect);

		// Use Ordinal comparison as names are already normalized to physical representation
		return targetColumns.FirstOrDefault(col =>
			getTargetName(col).Equals(physicalName, StringComparison.Ordinal));
	}

	/// <summary>
	/// Case-insensitive fallback matching for scenarios without dialect information.
	/// </summary>
	public static T? FindMatchingColumnCaseInsensitive<T>(
		string sourceName,
		IReadOnlyList<T> targetColumns,
		Func<T, string> getTargetName) where T : class
	{
		return targetColumns.FirstOrDefault(col =>
			getTargetName(col).Equals(sourceName, StringComparison.OrdinalIgnoreCase));
	}
}
