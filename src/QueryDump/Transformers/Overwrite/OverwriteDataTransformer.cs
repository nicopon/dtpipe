using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Overwrite;

/// <summary>
/// Overwrites specified columns with static values. Priority: 20.
/// </summary>
public class OverwriteDataTransformer : IDataTransformer, IRequiresOptions<OverwriteOptions>
{
    public int Priority => 20;

    private readonly Dictionary<string, string> _staticMappings = new(StringComparer.OrdinalIgnoreCase);
    private string?[]? _columnValues; // Array matching column count, null if no overwrite for that index

    public OverwriteDataTransformer(OverwriteOptions options)
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

    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (_staticMappings.Count == 0)
        {
            _columnValues = null;
            return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
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
        return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_columnValues == null)
        {
            return row;
        }

        // In-place modification
        for (var i = 0; i < row.Length; i++)
        {
            if (_columnValues[i] != null)
            {
                row[i] = _columnValues[i];
            }
        }

        return row;
    }
}
