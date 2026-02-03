using DtPipe.Core.Models;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Helper methods for resolving column names and keys.
/// </summary>
public static class ColumnHelper
{
    /// <summary>
    /// Resolves a key column name case-insensitively against available columns.
    /// </summary>
    /// <param name="keyName">Key name from user input (e.g., "Id")</param>
    /// <param name="columns">Available columns (already normalized)</param>
    /// <returns>Matched column name</returns>
    /// <exception cref="InvalidOperationException">If key column is not found</exception>
    public static string ResolveKeyColumn(string keyName, IReadOnlyList<ColumnInfo> columns)
    {
        var match = columns.FirstOrDefault(c => 
            string.Equals(c.Name, keyName, StringComparison.OrdinalIgnoreCase));
        
        if (match == null)
        {
            var available = string.Join(", ", columns.Select(c => c.Name));
            throw new InvalidOperationException(
                $"Key column '{keyName}' does not exist in source columns. " +
                $"Available columns: {available}");
        }
        
        return match.Name; // Return the ACTUAL normalized name
    }
    
    /// <summary>
    /// Resolves multiple key columns case-insensitively.
    /// </summary>
    /// <param name="keySpec">Comma-separated key column names (e.g., "Id,Name")</param>
    /// <param name="columns">Available columns (already normalized)</param>
    /// <returns>List of resolved column names</returns>
    public static List<string> ResolveKeyColumns(string keySpec, IReadOnlyList<ColumnInfo> columns)
    {
        if (string.IsNullOrEmpty(keySpec))
            return new List<string>();
            
        return keySpec.Split(',')
            .Select(k => k.Trim())
            .Where(k => !string.IsNullOrEmpty(k))
            .Select(k => ResolveKeyColumn(k, columns))
            .ToList();
    }
}
