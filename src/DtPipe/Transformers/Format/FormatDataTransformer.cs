using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Format;

public sealed partial class FormatDataTransformer : IDataTransformer, IRequiresOptions<FormatOptions>
{
    private readonly Dictionary<string, string> _mappings = new(StringComparer.OrdinalIgnoreCase);
    private readonly bool _skipNull;
    private int[]? _generationOrder;
    private ColumnProcessor[]? _processors;
    private Dictionary<string, int>? _columnNameToIndex;
    private int _realColumnCount;

    // Unified pattern for {COLUMN} or {COLUMN:format}
    [GeneratedRegex(@"\{([^{}:]+)(?::([^{}]+))?\}", RegexOptions.Compiled)]
    private static partial Regex PlaceholderPattern();

    public FormatDataTransformer(FormatOptions options)
    {
        _skipNull = options.SkipNull;
        foreach (var mapping in options.Format)
        {
            var parts = mapping.Split(':', 2);
            if (parts.Length == 2)
            {
                _mappings[parts[0].Trim()] = parts[1]; // Value is template
            }
        }
    }

    private int _virtualColumnCount;

    public bool HasFormat => _mappings.Count > 0;

    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (!HasFormat)
        {
            return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
        }

        var inputNames = columns.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        // Identify virtual columns (mappings key NOT in input)
        var virtualColumns = _mappings.Keys.Where(k => !inputNames.Contains(k)).ToList();
        
        _virtualColumnCount = virtualColumns.Count;
        _realColumnCount = columns.Count;
        var totalCount = columns.Count + _virtualColumnCount;

        // Map names to indices (Inputs + Virtuals)
        _columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columns.Count; i++) _columnNameToIndex[columns[i].Name] = i;
        for (int i = 0; i < virtualColumns.Count; i++) _columnNameToIndex[virtualColumns[i]] = columns.Count + i;
        
        _processors = new ColumnProcessor[totalCount];
        var targetIndices = new List<int>();

        // Register processors for Input columns (Overwrite)
        for (int i = 0; i < columns.Count; i++)
        {
            if (_mappings.TryGetValue(columns[i].Name, out var template))
            {
                var segments = ParseTemplate(template);
                var refs = ExtractReferencedColumns(segments);
                var refIndices = ResolveReferencedIndices(refs, _columnNameToIndex);
                _processors[i] = new ColumnProcessor(i, segments, refs, refIndices);
                targetIndices.Add(i);
            }
        }
        
        // Register processors for Virtual columns (Creation)
        for (int i = 0; i < virtualColumns.Count; i++)
        {
            var name = virtualColumns[i];
            var template = _mappings[name];
            var idx = columns.Count + i;
            
            var segments = ParseTemplate(template);
            var refs = ExtractReferencedColumns(segments);
            var refIndices = ResolveReferencedIndices(refs, _columnNameToIndex);
            _processors[idx] = new ColumnProcessor(idx, segments, refs, refIndices);
            targetIndices.Add(idx);
        }

        // Topological Sort
        _generationOrder = TopologicalSort(targetIndices, _processors);

        // Build Output Schema
        var newColumns = new List<ColumnInfo>(totalCount);
        
        // Add Inputs (changed to string if overwritten)
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
        
        // Add Virtuals (always String, assuming nullable for safety)
        foreach(var name in virtualColumns)
        {
            newColumns.Add(new ColumnInfo(name, typeof(string), true));
        }

        return new ValueTask<IReadOnlyList<ColumnInfo>>(newColumns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_generationOrder == null)
        {
            return row;
        }

        // Handle resizing for virtual columns
        if (_virtualColumnCount > 0)
        {
            var newRow = new object?[row.Length + _virtualColumnCount];
            Array.Copy(row, newRow, row.Length);
            row = newRow;
        }

        var sb = new System.Text.StringBuilder(128); // Small allocation

        foreach (var idx in _generationOrder)
        {
            // Skip check based on source columns if SkipNull is enabled
            var proc = _processors![idx];
            bool shouldSkip = false;

            if (_skipNull && proc.ReferencedIndices.Length > 0)
            {
                shouldSkip = true;
                foreach (var refIdx in proc.ReferencedIndices)
                {
                    // If ANY referenced column is NOT null, we do NOT skip
                    if (row[refIdx] is not null)
                    {
                        shouldSkip = false;
                        break;
                    }
                }
            }

            if (shouldSkip)
            {
                row[idx] = null;
                continue;
            }
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
                    // Ensure we don't access out of bounds if bad config
                    if (segment.ColumnIndex < row.Length)
                    {
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

    private static int[] ResolveReferencedIndices(HashSet<string> refs, Dictionary<string, int> nameToIndex)
    {
        var indices = new List<int>(refs.Count);
        foreach (var r in refs)
        {
            if (nameToIndex.TryGetValue(r, out var idx))
            {
                indices.Add(idx);
            }
        }
        return indices.ToArray();
    }

    // Private helper methods

    
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

        public readonly int[] ReferencedIndices;

        public ColumnProcessor(int index, TemplateSegment[] segments, HashSet<string> referencedColumns, int[] referencedIndices)
        {
            Index = index;
            Segments = segments;
            ReferencedColumns = referencedColumns;
            ReferencedIndices = referencedIndices;
        }
    }
}
