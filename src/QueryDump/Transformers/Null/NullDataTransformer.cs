using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Null;

/// <summary>
/// Sets specified columns to null. Priority: 10 (First).
/// </summary>
public class NullDataTransformer : IDataTransformer, IRequiresOptions<NullOptions>
{
    public int Priority => 10;
    
    private readonly HashSet<string> _nullColumns;
    private int[]? _targetIndices;

    public NullDataTransformer(NullOptions options)
    {
        _nullColumns = new HashSet<string>(options.Columns.Select(c => c.Trim()), StringComparer.OrdinalIgnoreCase);
    }

    public ValueTask InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (_nullColumns.Count == 0)
        {
            _targetIndices = null;
            return ValueTask.CompletedTask;
        }

        var indices = new List<int>();
        for (var i = 0; i < columns.Count; i++)
        {
            if (_nullColumns.Contains(columns[i].Name))
            {
                indices.Add(i);
            }
        }

        _targetIndices = indices.Count > 0 ? indices.ToArray() : null;
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<object?[]>> TransformAsync(IReadOnlyList<object?[]> batch, CancellationToken ct = default)
    {
        if (_targetIndices == null || batch.Count == 0)
        {
            return new ValueTask<IReadOnlyList<object?[]>>(batch);
        }

        // Perform in-place modification for performance
        foreach (var row in batch)
        {
            foreach (var idx in _targetIndices)
            {
                row[idx] = null;
            }
        }

        return new ValueTask<IReadOnlyList<object?[]>>(batch);
    }
}
