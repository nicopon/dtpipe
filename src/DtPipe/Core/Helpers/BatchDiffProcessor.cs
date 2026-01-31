
using System.Data;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Helper to perform Client-Side Diff (Select -> Partition -> Insert/Update).
/// </summary>
public class BatchDiffProcessor
{
    private readonly ILogger _logger;

    public BatchDiffProcessor(ILogger logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Processes a batch of rows, partitioning them into New (to Insert) and Existing (to Update/Ignore).
    /// </summary>
    /// <param name="rows">The batch of rows to process.</param>
    /// <param name="keyIndices">Indices of the primary key columns in the row array.</param>
    /// <param name="fetchExistingKeysFunc">Function that takes a list of key values and returns the set of existing keys found in the target DB.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A tuple containing lists of NewRows and ExistingRows.</returns>
    public async Task<(List<object?[]> NewRows, List<object?[]> ExistingRows)> PartitionBatchAsync(
        IReadOnlyList<object?[]> rows,
        IReadOnlyList<int> keyIndices,
        Func<IEnumerable<object[]>, CancellationToken, Task<HashSet<string>>> fetchExistingKeysFunc,
        CancellationToken ct)
    {
        if (rows.Count == 0)
        {
            return (new List<object?[]>(), new List<object?[]>());
        }

        // 1. Extract Keys from Batch
        var batchKeys = new List<object[]>(rows.Count);
        var keyMap = new Dictionary<string, List<object?[]>>(); // KeyString -> Rows

        foreach (var row in rows)
        {
            var keyComponents = new object[keyIndices.Count];
            for (int i = 0; i < keyIndices.Count; i++)
            {
                keyComponents[i] = row[keyIndices[i]] ?? DBNull.Value;
            }

            // Create a composite key string for reliable hashing/comparison
            // (Using a simple separator approach, or could use StableHash if available)
            // For now, let's use string.Join. For binary keys this might be weak but acceptable for MVP.
            // A better approach is to rely on the generic 'object' equality if simple types, but array equality is tricky.
            var keyString = GenerateKeyString(keyComponents);
            
            batchKeys.Add(keyComponents);

            if (!keyMap.TryGetValue(keyString, out var mappedRows))
            {
                mappedRows = new List<object?[]>();
                keyMap[keyString] = mappedRows;
            }
            mappedRows.Add(row);
        }

        // 2. Fetch Existing Keys from Target
        // The fetchExistingKeysFunc is responsible for executing "SELECT ID FROM Target WHERE ID IN (...)"
        // It returns a HashSet of KeyStrings that matched.
        var existingKeyStrings = await fetchExistingKeysFunc(batchKeys, ct);

        // 3. Partition
        var newRows = new List<object?[]>();
        var existingRows = new List<object?[]>();

        foreach (var kvp in keyMap)
        {
            var keyString = kvp.Key;
            var sameKeyRows = kvp.Value;

            if (existingKeyStrings.Contains(keyString))
            {
                existingRows.AddRange(sameKeyRows);
            }
            else
            {
                newRows.AddRange(sameKeyRows);
            }
        }

        return (newRows, existingRows);
    }
    
    public static string GenerateKeyString(object[] keyComponents)
    {
        // Simple composite key string generator. 
        // Handles nulls and basic types.
        // Format: "Val1|Val2|Val3"
        return string.Join("|", keyComponents.Select(k => k?.ToString() ?? "NULL"));
    }
}
