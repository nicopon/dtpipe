using Jint;
using Jint.Native;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Script;

/// <summary>
/// Transforms data rows using Javascript scripts via Jint.
/// </summary>
public sealed class ScriptDataTransformer : IDataTransformer, IRequiresOptions<ScriptOptions>
{
    private readonly Engine _engine;
    private readonly Dictionary<string, string> _mappings;
    
    // State initialized in InitializeAsync
    private Dictionary<string, int>? _columnNameToIndex;
    private ScriptColumnProcessor[]? _processors;
    private int[]? _processingOrder;
    
    // Lock for thread safety of Jint Engine
    private readonly object _engineLock = new();

    private readonly bool _skipNull;

    public ScriptDataTransformer(ScriptOptions options)
    {
        // Parse mappings: "COLUMN:script"
        _mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in options.Mappings)
        {
            var parts = mapping.Split(':', 2);
            if (parts.Length == 2)
            {
                _mappings[parts[0]] = parts[1];
            }
        }

        // Initialize Jint in strict sandbox mode
        _engine = new Engine(cfg => cfg
            .Strict(true)
            .LimitMemory(20_000_000) // 20MB limit for safety
            .TimeoutInterval(TimeSpan.FromSeconds(2)) // 2s timeout per script execution
        );
        _skipNull = options.SkipNull;
    }

    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (_mappings.Count == 0)
        {
            return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
        }

        _columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            _columnNameToIndex[columns[i].Name] = i;
        }

        var processors = new List<ScriptColumnProcessor>();
        var processingOrder = new List<int>();

        // We prepare a JS function for each mapped column
        // function col_INDEX(value) { return ...userScript...; }
        
        foreach (var (colName, script) in _mappings)
        {
            if (_columnNameToIndex.TryGetValue(colName, out var colIndex))
            {
                // Wrap user script in a function to allow return value capture
                // If user scipt is an expression like "value.slice(0,2)", we return it.
                // If it's complex code block, user must handle return? 
                // Requirement said: "return {script_to_process}"
                // So we wrap it as: 
                var functionName = $"proc_{colIndex}";
                var wrappedScript = $"function {functionName}(value) {{ return {script}; }}";
                
                lock(_engineLock)
                {
                    _engine.Execute(wrappedScript);
                }

                processors.Add(new ScriptColumnProcessor(colIndex, functionName));
                processingOrder.Add(colIndex);
            }
        }

        _processors = processors.ToArray();
        _processingOrder = processingOrder.ToArray();

        return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_processors == null || _processors.Length == 0) return row;

        lock (_engineLock)
        {
            foreach (var processor in _processors)
            {
                var originalValue = row[processor.ColumnIndex];
                
                if (_skipNull && originalValue is null)
                {
                    continue;
                }
                
                // Convert .NET value to JS value if needed, or let Jint handle it
                // Jint handles basic types well.
                
                // Call the pre-compiled function
                var result = _engine.Invoke(processor.FunctionName, originalValue);

                // Convert back to .NET
                // Depending on type, we might need specific handling. 
                // For now, assume simple types (string, number, bool) or ToString().
                
                if (result.IsString())
                {
                    row[processor.ColumnIndex] = result.AsString();
                }
                else if (result.IsNumber())
                {
                     row[processor.ColumnIndex] = result.AsNumber();
                }
                else if (result.IsBoolean())
                {
                    row[processor.ColumnIndex] = result.AsBoolean();
                }
                else if (result.IsNull() || result.IsUndefined())
                {
                    row[processor.ColumnIndex] = null;
                }
                else 
                {
                    // Fallback for objects/arrays -> JSON/String? 
                    // Or simply ToString()
                    row[processor.ColumnIndex] = result.ToString();
                }
            }
        }

        return row;
    }

    private record struct ScriptColumnProcessor(int ColumnIndex, string FunctionName);
}
