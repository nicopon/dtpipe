using System.Collections;

namespace DtPipe.Core.Infrastructure.Discovery;

public static class SchemaDiscoveryHelper
{
    /// <summary>
    /// Recursively merges two dictionaries to build a superset schema.
    /// Used during schema discovery to handle sparse or heterogeneous data.
    /// </summary>
    public static void DeepMergeSchemas(Dictionary<string, object?> target, Dictionary<string, object?> source)
    {
        foreach (var kvp in source)
        {
            if (!target.TryGetValue(kvp.Key, out var existing))
            {
                target[kvp.Key] = kvp.Value;
            }
            else if (existing is Dictionary<string, object?> targetDict && kvp.Value is Dictionary<string, object?> sourceDict)
            {
                DeepMergeSchemas(targetDict, sourceDict);
            }
            else if (existing is List<object?> targetList && kvp.Value is List<object?> sourceList)
            {
                if (targetList.Count == 0 && sourceList.Count > 0)
                {
                    // If target list was empty, take the first item of source to learn the type
                    targetList.AddRange(sourceList);
                }
                else if (targetList.Count > 0 && sourceList.Count > 0)
                {
                    // If both have items, and they are objects, merge them
                    if (targetList[0] is Dictionary<string, object?> t0 && sourceList[0] is Dictionary<string, object?> s0)
                    {
                        DeepMergeSchemas(t0, s0);
                    }
                }
            }
            else if (existing == null && kvp.Value != null)
            {
                target[kvp.Key] = kvp.Value;
            }
            // Heterogeneous promotion: Scalar vs Dictionary
            else if (existing is not Dictionary<string, object?> && kvp.Value is Dictionary<string, object?> sourceDictPromo)
            {
                var promotedTarget = new Dictionary<string, object?>(StringComparer.Ordinal) { ["_value"] = existing };
                DeepMergeSchemas(promotedTarget, sourceDictPromo);
                target[kvp.Key] = promotedTarget;
            }
            else if (existing is Dictionary<string, object?> targetDictPromo && kvp.Value is not Dictionary<string, object?> && kvp.Value != null)
            {
                var promotedSource = new Dictionary<string, object?>(StringComparer.Ordinal) { ["_value"] = kvp.Value };
                DeepMergeSchemas(targetDictPromo, promotedSource);
            }
        }
    }
}
