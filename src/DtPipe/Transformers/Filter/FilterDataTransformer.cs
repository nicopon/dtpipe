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
            var funcName = $"__filter_{GetHashCode()}_{i}_{Guid.NewGuid().ToString("N")}"; 
            
            string body = filterScript.Trim();
            if (!body.Contains("return ") && !body.EndsWith(";"))
            {
               body = "return " + body + ";";
            }
            
            // Wrap in function
            var wrappedScript = $"function {funcName}(row) {{ {body} }}";
            engine.Execute(wrappedScript);
            
            _compiledFilters.Add(new JsString(funcName));
        }

        return ValueTask.FromResult(sourceColumns);
    }

    public object?[]? Transform(object?[] row)
    {
        if (_compiledFilters.Count == 0 || _columnNames == null) return row;

        var engine = _jsEngineProvider.GetEngine();
        
        // Build JS Context
        var jsRow = new JsObject(engine);
        for (int i = 0; i < row.Length; i++)
        {
            // Note: Optimizing this to reuse object or array would be better, but tricky with Jint
            jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
        }

        foreach (var funcName in _compiledFilters)
        {
            var result = engine.Invoke(funcName.ToString(), jsRow);
            
            // If result is falsy (false, null, undefined, 0, ""), drop row
            // Jint's AsBoolean rules:
            // boolean: value
            // number: != 0 && !NaN
            // string: length > 0
            // object: true
            // null/undefined: false
            
            if (!result.AsBoolean())
            {
                return null; // Drop row
            }
        }

        return row; 
    }
}
