using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Core.Options;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Filter;

public class FilterDataTransformer : IDataTransformer, IRequiresOptions<FilterTransformerOptions>
{
    private readonly FilterTransformerOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly List<JsValue> _compiledFilters = new();

    public FilterDataTransformer(FilterTransformerOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    private string[]? _columnNames;

    // Cache wrapped scripts for fast re-compilation
    private readonly List<string> _wrappedScripts = new();

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
    {
        if (_options.Filters == null || _options.Filters.Length == 0)
        {
            return ValueTask.FromResult(sourceColumns);
        }

        _columnNames = sourceColumns.Select(c => c.Name).ToArray();
        var engine = _jsEngineProvider.GetEngine();

        // Compile filters
        for (int i = 0; i < _options.Filters.Length; i++)
        {
            var filterScript = _options.Filters[i];
            var funcName = $"__filter_{Guid.NewGuid():N}"; 
            
            string body = filterScript.Trim();
            if (!body.Contains("return ") && !body.EndsWith(";"))
            {
               body = "return " + body + ";";
            }
            
            // Wrap in function expression
            var wrappedScript = $"function(row) {{ {body} }}";
            _wrappedScripts.Add(wrappedScript);
            
            // Compile in initial engine (validation)
            // Use SetValue/Evaluate pattern here too
            engine.SetValue(funcName, engine.Evaluate($"({wrappedScript})"));
            
            _compiledFilters.Add(new JsString(funcName));
        }

        return ValueTask.FromResult(sourceColumns);
    }

    public object?[]? Transform(object?[] row)
    {
        if (_compiledFilters.Count == 0 || _columnNames == null) return row;

        var engine = _jsEngineProvider.GetEngine();
        EnsureFiltersCompiled(engine);
        
        // Build JS Context
        var jsRow = new JsObject(engine);
        for (int i = 0; i < row.Length; i++)
        {
            // Note: Optimizing this to reuse object or array would be better, but tricky with Jint
            jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
        }

        // Set 'row' in global scope for Evaluate Call
        engine.SetValue("row", jsRow);

        foreach (var funcName in _compiledFilters)
        {
             // Direct evaluation of function call
             try 
             {
                 var result = engine.Evaluate($"{funcName}(row)");
                 if (!result.AsBoolean())
                 {
                     return null; // Drop row
                 }
             }
             catch (Exception ex)
             {
                 throw new InvalidOperationException($"Error evaluating filter '{funcName}': {ex.Message}", ex);
             }
        }


        return row; 
    }

    private void EnsureFiltersCompiled(Engine engine)
    {
        if (_compiledFilters.Count > 0)
        {
            var firstFunc = _compiledFilters[0].ToString();
            var val = engine.GetValue(firstFunc);
            
            if (val.IsUndefined() || val.IsNull())
            {
                for (int i = 0; i < _compiledFilters.Count; i++)
                {
                    // Evaluate function expression
                    var script = _wrappedScripts[i]; // "function(row) { ... }"
                    // Wrap in parens to ensure expression evaluation
                    var funcVal = engine.Evaluate($"({script})");
                    engine.SetValue(_compiledFilters[i].ToString(), funcVal);
                }
            }
        }
    }
}
