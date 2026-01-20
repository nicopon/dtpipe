using System.Text.RegularExpressions;
using QueryDump.Core;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Format;

public sealed partial class FormatDataTransformer : IDataTransformer, IRequiresOptions<FormatOptions>
{
    private readonly Dictionary<string, string> _mappings = new(StringComparer.OrdinalIgnoreCase);
    private int[]? _generationOrder;
    private ColumnProcessor[]? _processors;
    private Dictionary<string, int>? _columnNameToIndex;

    // Unified pattern for {COLUMN} or {COLUMN:format}
    [GeneratedRegex(@"\{([^{}:]+)(?::([^{}]+))?\}", RegexOptions.Compiled)]
    private static partial Regex PlaceholderPattern();

    public FormatDataTransformer(FormatOptions options)
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
                var segments = ParseTemplate(template);
                var refs = ExtractReferencedColumns(segments);
                _processors[i] = new ColumnProcessor(i, segments, refs);
                targetIndices.Add(i);
            }
        }

        // Topological Sort for dependency resolution to determine field generation order.
        // We only need to sort the target columns.
        _generationOrder = TopologicalSort(targetIndices, _processors);

        // Update schema: Transformed columns become Strings
        var newColumns = new List<ColumnInfo>(columns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
             if (_mappings.ContainsKey(columns[i].Name))
             {
                 newColumns.Add(new ColumnInfo(columns[i].Name, typeof(string), columns[i].IsNullable));
             }
             else
             {
                 newColumns.Add(columns[i]);
             }
        }

        return new ValueTask<IReadOnlyList<ColumnInfo>>(newColumns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_generationOrder == null)
        {
            return row;
        }

        // Avoid allocation of StringBuilder if possible? 
        // We use a shared StringBuilder/Buffer? Thread safety valid here?
        // Method Transform is called serially per row in pipeline (single thread per pipeline stage usually).
        // BUT ExportService uses parallelism if stages are parallel. 
        // ExportService.TransformRowsAsync calls pipeline serially.
        // So we can allocate one StringBuilder per instance potentially?
        // But let's verify reuse. `FormatDataTransformer` instance is created once.
        // `Transform` is called in loop.
        // If we make `_sb` a field, it's not thread safe if `Transform` is called concurrently.
        // Pipeline seems serial: `foreach (var row in input.ReadAllAsync...` one by one.
        // So reuse is safe IF strict single thread.
        // To be safe, local StringBuilder or simple string concat (allocations).
        // Since we optimize for PERF, let's use `string.Join`/`Concat` on segments logic?
        // Or `StringBuilder` pool. 
        // Simple StringBuilder per call is cleaner than Regex. 
        
        var sb = new System.Text.StringBuilder(128); // Small allocation

        foreach (var idx in _generationOrder)
        {
            var proc = _processors![idx];
            sb.Clear();
            
            foreach (var segment in proc.Segments)
            {
                if (segment.IsLiteral)
                {
                    sb.Append(segment.Value);
                }
                else
                {
                    // Column Reference
                    var val = row[segment.ColumnIndex];
                    if (val != null)
                    {
                        if (segment.Format != null)
                        {
                            try 
                            { 
                                sb.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, $"{{0:{segment.Format}}}", val);
                            }
                            catch (FormatException)
                            {
                                sb.Append(val);
                            }
                        }
                        else
                        {
                            sb.Append(val);
                        }
                    }
                }
            }
            row[idx] = sb.ToString();
        }

        return row;
    }

    private TemplateSegment[] ParseTemplate(string template)
    {
        var segments = new List<TemplateSegment>();
        var matches = PlaceholderPattern().Matches(template);
        int lastIndex = 0;

        foreach (Match match in matches)
        {
            if (match.Index > lastIndex)
            {
                // Literal before match
                segments.Add(TemplateSegment.CreateLiteral(template.Substring(lastIndex, match.Index - lastIndex)));
            }
            
            var colName = match.Groups[1].Value;
            var format = match.Groups[2].Success ? match.Groups[2].Value : null;

            if (_columnNameToIndex!.TryGetValue(colName, out var idx))
            {
                segments.Add(TemplateSegment.CreateColumn(idx, format, colName));
            }
            else
            {
                // Unresolved column -> Treat as literal text (original match)
                segments.Add(TemplateSegment.CreateLiteral(match.Value));
            }

            lastIndex = match.Index + match.Length;
        }

        if (lastIndex < template.Length)
        {
            segments.Add(TemplateSegment.CreateLiteral(template.Substring(lastIndex)));
        }

        return segments.ToArray();
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
            if (recursionStack.Contains(u)) throw new InvalidOperationException($"Cycle detected in format dependencies for column index {u}");
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
                        // Ignore self-references (A -> A) as they simply use the original value
                        if (targets.Contains(refIdx) && refIdx != u)
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

    private static HashSet<string> ExtractReferencedColumns(TemplateSegment[] segments)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var segment in segments)
        {
            // If it has a SourceColumnName (we stored it for dependency resolution)
            if (!segment.IsLiteral && segment.SourceColumnName != null)
            {
                result.Add(segment.SourceColumnName);
            }
        }
        return result;
    }

    // Removed SubstituteTemplate as it's replaced by loop in Transform
    
    private readonly struct TemplateSegment
    {
        public readonly bool IsLiteral;
        public readonly string? Value; // Literal text OR Format specifier
        public readonly int ColumnIndex;
        public readonly string? SourceColumnName; // For dependency resolution
        public string? Format => !IsLiteral ? Value : null;

        private TemplateSegment(bool isLiteral, string? value, int columnIndex, string? sourceColumnName)
        {
            IsLiteral = isLiteral;
            Value = value;
            ColumnIndex = columnIndex;
            SourceColumnName = sourceColumnName;
        }

        public static TemplateSegment CreateLiteral(string text) => new(true, text, -1, null);
        public static TemplateSegment CreateColumn(int index, string? format, string colName) => new(false, format, index, colName);
    }

    private readonly struct ColumnProcessor
    {
        public readonly int Index;
        public readonly TemplateSegment[] Segments;
        public readonly HashSet<string> ReferencedColumns;

        public ColumnProcessor(int index, TemplateSegment[] segments, HashSet<string> referencedColumns)
        {
            Index = index;
            Segments = segments;
            ReferencedColumns = referencedColumns;
        }
    }
}
