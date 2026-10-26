using System.Collections;

namespace DtPipe.Core.Infrastructure.Discovery;

public static class SchemaDiscoveryHelper
{
    /// <summary>
    /// Recursively merges two dictionaries to build a superset schema.
    /// Used during schema discovery to handle sparse or heterogeneous data.
    /// Whatever it inserts into <paramref name="target"/> is a copy: a schema that shares an object with a
    /// record writes its keys into that record, which a reader replays after discovery.
    /// </summary>
    public static void DeepMergeSchemas(Dictionary<string, object?> target, Dictionary<string, object?> source)
    {
        foreach (var kvp in source)
        {
            if (!target.TryGetValue(kvp.Key, out var existing))
            {
                target[kvp.Key] = Detach(kvp.Value);
            }
            else if (existing is Dictionary<string, object?> targetDict && kvp.Value is Dictionary<string, object?> sourceDict)
            {
                DeepMergeSchemas(targetDict, sourceDict);
            }
            else if (existing is List<object?> targetList && kvp.Value is List<object?> sourceList)
            {
                MergeLists(targetList, sourceList);
            }
            else if (existing == null && kvp.Value != null)
            {
                target[kvp.Key] = Detach(kvp.Value);
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

    /// <summary>
    /// A list stands in the schema by one element, the one its type is read from. Every object the source's
    /// elements hold adds its keys to that element, so an item that comes after the first still counts, and so
    /// does an item of a list within a list.
    /// </summary>
    private static void MergeLists(List<object?> target, List<object?> source)
    {
        if (source.Count == 0) return;

        if (target.Count == 0)
        {
            target.AddRange(DetachList(source));
            return;
        }

        switch (target[0])
        {
            case Dictionary<string, object?> items:
                foreach (var element in source)
                {
                    if (element is Dictionary<string, object?> sourceItem)
                        DeepMergeSchemas(items, sourceItem);
                }
                break;
            case List<object?> innerItems:
                foreach (var element in source)
                {
                    if (element is List<object?> sourceInner)
                        MergeLists(innerItems, sourceInner);
                }
                break;
        }
    }

    private static object? Detach(object? value) => value switch
    {
        Dictionary<string, object?> dictionary => DetachDictionary(dictionary),
        List<object?> list => DetachList(list),
        _ => value
    };

    private static Dictionary<string, object?> DetachDictionary(Dictionary<string, object?> source)
    {
        var copy = new Dictionary<string, object?>(source.Comparer);
        foreach (var kvp in source)
            copy[kvp.Key] = Detach(kvp.Value);
        return copy;
    }

    private static List<object?> DetachList(List<object?> source)
    {
        var copy = new List<object?>();
        if (source.Count == 0) return copy;

        copy.Add(Detach(source[0]));
        MergeLists(copy, source.GetRange(1, source.Count - 1));
        return copy;
    }
}
