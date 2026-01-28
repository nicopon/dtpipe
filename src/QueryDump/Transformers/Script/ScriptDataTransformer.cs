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
    private string[]? _columnNames;
    private ScriptColumnProcessor[]? _processors;
    
    // Lock for thread safety of Jint Engine
    private readonly object _engineLock = new();

    private readonly bool _skipNull;

    public ScriptDataTransformer(ScriptOptions options)
    {
        // Parse mappings: "COLUMN:script"
        _mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in options.Script)
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

    public bool HasScript => _mappings.Count > 0;

    public ValueTask<IReadOnlyList<ColumnInfo>> InitializeAsync(IReadOnlyList<ColumnInfo> columns, CancellationToken ct = default)
    {
        if (!HasScript)
        {
            return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
        }

        _columnNames = columns.Select(c => c.Name).ToArray();
        _columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            _columnNameToIndex[columns[i].Name] = i;
        }

        var processors = new List<ScriptColumnProcessor>();

        foreach (var (colName, script) in _mappings)
        {
            if (_columnNameToIndex.TryGetValue(colName, out var colIndex))
            {
                var functionName = $"proc_{colIndex}";
                
                // If the script contains a return statement, use it as is.
                // Otherwise, wrap it to return the expression result.
                string body = script.Trim();
                if (!body.Contains("return ") && !body.EndsWith(";"))
                {
                    body = "return " + body + ";";
                }
                
                var wrappedScript = $"function {functionName}(row) {{ {body} }}";
                
                lock(_engineLock)
                {
                    _engine.Execute(wrappedScript);
                }

                processors.Add(new ScriptColumnProcessor(colIndex, functionName));
            }
        }

        _processors = processors.ToArray();

        return new ValueTask<IReadOnlyList<ColumnInfo>>(columns);
    }

    public object?[] Transform(object?[] row)
    {
        if (_processors == null || _processors.Length == 0 || _columnNames == null) return row;

        lock (_engineLock)
        {
            // Build a JS object representing the current row
            var jsRow = new JsObject(_engine);
            for (int i = 0; i < row.Length; i++)
            {
                jsRow.Set(_columnNames[i], JsValue.FromObject(_engine, row[i]));
            }

            foreach (var processor in _processors)
            {
                var originalValue = row[processor.ColumnIndex];
                
                if (_skipNull && originalValue is null)
                {
                    continue;
                }
                
                // Convert .NET value to JS value if needed, or let Jint handle it
                // Jint handles basic types well.
                
                // Call the pre-compiled function with full row
                var result = _engine.Invoke(processor.FunctionName, jsRow);

                // Convert back to .NET types (simple types or string fallback)
                
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
                    // Fallback for complex objects
                    row[processor.ColumnIndex] = result.ToString();
                }
            }
        }

        return row;
    }

    private record struct ScriptColumnProcessor(int ColumnIndex, string FunctionName);
}
