using Jint;
using Jint.Native;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Services;

namespace DtPipe.Transformers.Script;

/// <summary>
/// Transforms data rows using Javascript scripts via Jint.
/// </summary>
public sealed class ComputeDataTransformer : IDataTransformer, IRequiresOptions<ComputeOptions>
{
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly List<string> _initializationScripts = new();
    private readonly Dictionary<string, string> _mappings;
    
    // State initialized in InitializeAsync
    private Dictionary<string, int>? _columnNameToIndex;
    private string[]? _columnNames;
    private ScriptColumnProcessor[]? _processors;
    
    private readonly bool _skipNull;

    public ComputeDataTransformer(ComputeOptions options, IJsEngineProvider jsEngineProvider)
    {
        _jsEngineProvider = jsEngineProvider;
        
        // Parse mappings: "COLUMN:script"
        _mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var mapping in options.Compute)
        {
            var parts = mapping.Split(':', 2);
            if (parts.Length == 2)
            {
                _mappings[parts[0]] = parts[1];
            }
        }
        _skipNull = options.SkipNull;
    }

    public bool HasScript => _mappings.Count > 0;

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        if (!HasScript)
        {
            return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
        }

        _columnNames = columns.Select(c => c.Name).ToArray();
        _columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
        {
            _columnNameToIndex[columns[i].Name] = i;
        }

        var processors = new List<ScriptColumnProcessor>();
        var engine = _jsEngineProvider.GetEngine();

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
                
                // Validate syntax immediately (and register function in the shared engine!)
                // NOTE: Since engine is shared/scoped, we register functions directly.
                // BUT: If multiple transformers use same function names, we have a collision issue.
                // Solution: Make function names unique per transformer instance? 
                // Or just use anonymous execution? 
                // Using named functions is faster for repeated calls.
                // We should make function name unique.
                
                var uniqueFunctionName = $"{functionName}_{Guid.NewGuid().ToString("N")}";
                wrappedScript = $"function {uniqueFunctionName}(row) {{ {body} }}";
                
                engine.Execute(wrappedScript);
                
                processors.Add(new ScriptColumnProcessor(colIndex, uniqueFunctionName));
            }
        }

        _processors = processors.ToArray();

        return new ValueTask<IReadOnlyList<PipeColumnInfo>>(columns);
    }

    public object?[]? Transform(object?[] row)
    {
        if (_processors == null || _processors.Length == 0 || _columnNames == null) return row;

        var engine = _jsEngineProvider.GetEngine();

        // Build a JS object representing the current row
        var jsRow = new JsObject(engine);
        for (int i = 0; i < row.Length; i++)
        {
            jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
        }

        foreach (var processor in _processors)
        {
            var originalValue = row[processor.ColumnIndex];
            
            if (_skipNull && originalValue is null)
            {
                continue;
            }
            
            // Call the pre-compiled function with full row
            var result = engine.Invoke(processor.FunctionName, jsRow);

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

        return row;
    }
    
    // Dispose handled by DI scope? No, provider is singleton/scoped. 
    // ComputeDataTransformer does not own the engine anymore.

    private record struct ScriptColumnProcessor(int ColumnIndex, string FunctionName);
}
