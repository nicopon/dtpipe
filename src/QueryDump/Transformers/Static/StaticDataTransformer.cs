using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Static;

/// <summary>
/// Overwrites specified columns with static values. Priority: 20.
/// </summary>
public class StaticDataTransformer : IDataTransformer, IRequiresOptions<OverwriteOptions>
{
    public int Priority => 20;

    private readonly Dictionary<string, string> _staticMappings = new(StringComparer.OrdinalIgnoreCase);
    private string?[]? _columnValues; // Array matching column count, null if no overwrite for that index

    public StaticDataTransformer(OverwriteOptions options)
    {
        foreach (var mapping in options.Mappings)
        {
            var parts = mapping.Split(':', 2);
            if (parts.Length == 2)
            {
                _staticMappings[parts[0].Trim()] = parts[1]; // Value is kept as string, might need trimming? user might want spaces.
            }
        }
    }

    public ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (_staticMappings.Count == 0)
        {
            _columnValues = null;
            return ValueTask.CompletedTask;
        }

        bool hasMapping = false;
        var values = new string?[columns.Count];
        
        for (var i = 0; i < columns.Count; i++)
        {
            if (_staticMappings.TryGetValue(columns[i].Name, out var val))
            {
                values[i] = val;
                hasMapping = true;
            }
            else
            {
                values[i] = null;
            }
        }

        _columnValues = hasMapping ? values : null;
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<object?[]>> TransformAsync(IReadOnlyList<object?[]> batch, CancellationToken ct = default)
    {
        if (_columnValues == null || batch.Count == 0)
        {
            return new ValueTask<IReadOnlyList<object?[]>>(batch);
        }

        // In-place modification
        foreach (var row in batch)
        {
            for (var i = 0; i < row.Length; i++)
            {
                if (_columnValues[i] != null)
                {
                    row[i] = _columnValues[i];
                }
            }
        }

        return new ValueTask<IReadOnlyList<object?[]>>(batch);
    }
}
