using System.Text.RegularExpressions;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Clone;

public sealed partial class CloneDataTransformer : IDataTransformer, IRequiresOptions<CloneOptions>
{
    public int Priority => 100; // Last step

    private readonly Dictionary<string, string> _mappings = new(StringComparer.OrdinalIgnoreCase);
    private int[]? _generationOrder;
    private ColumnProcessor[]? _processors;
    private Dictionary<string, int>? _columnNameToIndex;

    [GeneratedRegex(@"\{\{([^}]+)\}\}", RegexOptions.Compiled)]
    private static partial Regex TemplatePattern();

    public CloneDataTransformer(CloneOptions options)
    {
        foreach (var mapping in options.Mappings)
        {
            var parts = mapping.Split(':', 2);
            if (parts.Length == 2)
            {
                _mappings[parts[0].Trim()] = parts[1]; // Value is template
            }
        }
    }

    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (_mappings.Count == 0)
        {
            return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
        }

        // Input columns already include virtual columns from upstream transformers (e.g., Fake)
        _columnNameToIndex = columns.Select((c, i) => (c.Name, i))
            .ToDictionary(x => x.Name, x => x.i, StringComparer.OrdinalIgnoreCase);
        
        _processors = new ColumnProcessor[columns.Count];
        var targetIndices = new List<int>();

        for (int i = 0; i < columns.Count; i++)
        {
            if (_mappings.TryGetValue(columns[i].Name, out var template))
            {
                var refs = ExtractReferencedColumns(template);
                _processors[i] = new ColumnProcessor(i, template, refs);
                targetIndices.Add(i);
            }
        }

        // Topological Sort for dependency resolution to determine field generation order.
        // We only need to sort the target columns.
        _generationOrder = TopologicalSort(targetIndices, _processors);

        return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_generationOrder == null)
        {
            return row;
        }

        foreach (var idx in _generationOrder)
        {
            var proc = _processors![idx];
            row[idx] = SubstituteTemplate(proc.Template, row);
        }

        return row;
    }

    private int[] TopologicalSort(List<int> targets, ColumnProcessor[] processors)
    {
        // Simple Kahn's algorithm or DFS
        // Nodes are indices in `targets`.
        // Dependency: If A refers to B, and B is in `targets`, B -> A (B must be processed before A)
        
        var visited = new HashSet<int>();
        var sorted = new List<int>();
        var recursionStack = new HashSet<int>();

        void Visit(int u)
        {
            if (recursionStack.Contains(u)) throw new InvalidOperationException($"Cycle detected in clone dependencies for column index {u}");
            if (visited.Contains(u)) return;

            recursionStack.Add(u);
            
            // Depends on...
            if (processors[u].ReferencedColumns != null)
            {
                foreach (var refColName in processors[u].ReferencedColumns!)
                {
                    if (_columnNameToIndex!.TryGetValue(refColName, out var refIdx))
                    {
                        // If the referenced column is also a target, visit it first
                        if (targets.Contains(refIdx))
                        {
                            Visit(refIdx);
                        }
                    }
                }
            }

            recursionStack.Remove(u);
            visited.Add(u);
            sorted.Add(u);
        }

        foreach (var t in targets)
        {
            Visit(t);
        }

        return sorted.ToArray();
    }

    private static HashSet<string> ExtractReferencedColumns(string template)
    {
        var matches = TemplatePattern().Matches(template);
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in matches)
        {
            result.Add(match.Groups[1].Value);
        }
        return result;
    }

    private string SubstituteTemplate(string template, object?[] row)
    {
        return TemplatePattern().Replace(template, match =>
        {
            var colName = match.Groups[1].Value;
            if (_columnNameToIndex!.TryGetValue(colName, out var idx))
            {
                return row[idx]?.ToString() ?? string.Empty;
            }
            return match.Value;
        });
    }

    private readonly struct ColumnProcessor
    {
        public readonly int Index;
        public readonly string Template;
        public readonly HashSet<string> ReferencedColumns;

        public ColumnProcessor(int index, string template, HashSet<string> referencedColumns)
        {
            Index = index;
            Template = template;
            ReferencedColumns = referencedColumns;
        }
    }
}
