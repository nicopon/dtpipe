using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Core.Options;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Window;

public class WindowDataTransformer : IMultiRowTransformer, IRequiresOptions<WindowOptions>
{
    private readonly WindowOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private string[]? _columnNames;
    private string? _funcName;
    private int _keyColumnIndex = -1;

    private readonly List<object?[]> _buffer = new();
    private object? _lastKey = null;

    public WindowDataTransformer(WindowOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    private string? _wrappedScript;

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
    {
        _columnNames = sourceColumns.Select(c => c.Name).ToArray();

        // Compile script if provided
        if (!string.IsNullOrWhiteSpace(_options.Script))
        {
            var engine = _jsEngineProvider.GetEngine();
            var script = _options.Script.Trim();
            
            // Allow statement or expression
            if (!script.Contains("return ") && !script.EndsWith(";"))
            {
                script = "return " + script + ";";
            }

            _funcName = $"__window_{Guid.NewGuid():N}";
            // Function takes 'rows' array
            // Wrap in function expression
            var wrappedScript = $"function(rows) {{ {script} }}";
            _wrappedScript = wrappedScript;
            
        }

        // Key column index
        if (!string.IsNullOrWhiteSpace(_options.Key))
        {
            var col = sourceColumns.FirstOrDefault(c => c.Name.Equals(_options.Key, StringComparison.OrdinalIgnoreCase));
            if (col == null)
            {
                throw new ArgumentException($"Window key column '{_options.Key}' not found.");
            }
            _keyColumnIndex = sourceColumns.ToList().IndexOf(col);
        }

        return ValueTask.FromResult(sourceColumns);
    }

    public object?[]? Transform(object?[] row)
    {
        // Fallback or explicit single-row usage not supported well here.
        // Return null or first row of window?
        // Since this is a window aggregator, calling Transform() sequentially row-by-row without getting results is wrong.
        // The pipeline infrastructure should call TransformMany.
        // If we adhere to IMultiRowTransformer, Transform() might be ignored or used for 1:1 checks.
        // Let's return null.
        return null;
    }

    public IEnumerable<object?[]> TransformMany(object?[] row)
    {
        bool flush = false;
        
        // Check Key Change
        if (_keyColumnIndex >= 0)
        {
            var currentKey = row[_keyColumnIndex];
            if (_buffer.Count > 0 && !Equals(currentKey, _lastKey))
            {
                // Key changed, flush PREVIOUS window
                flush = true;
            }
            _lastKey = currentKey;
        }
        
        // If we needed to flush due to key change, we need to process buffer BEFORE adding current row
        if (flush)
        {
            foreach (var r in ProcessBuffer()) yield return r;
        }
        
        // Add current row
        _buffer.Add(row);
        
        // Check Count
        if (_options.Count.HasValue && _buffer.Count >= _options.Count.Value)
        {
            foreach (var r in ProcessBuffer()) yield return r;
        }
    }
    
    private IEnumerable<object?[]> ProcessBuffer()
    {
        if (_buffer.Count == 0) yield break;
        
        if (_funcName == null)
        {
        }

        var engine = _jsEngineProvider.GetEngine();
        EnsureFunctionCompiled(engine);
        
        // Convert buffer to JS array of objects
        var jsRows = new Jint.Native.JsArray(engine);
        foreach (var r in _buffer)
        {
            var jsObj = new JsObject(engine);
            for (int i = 0; i < r.Length; i++)
            {
                jsObj.Set(_columnNames![i], JsValue.FromObject(engine, r[i]));
            }
            jsRows.Push(jsObj);
        }
        
        // Set 'rows' in global scope for Evaluate Call
        engine.SetValue("rows", jsRows);

        // Execute script
        JsValue result;
        try
        {
             result = engine.Evaluate($"{_funcName}(rows)");
        }
        catch (Exception ex)
        {
             throw new InvalidOperationException($"Error evaluating window script: {ex.Message}", ex);
        }
        
        // Result should be array of rows
        if (result.IsArray())
        {
            var array = result.AsArray();
            
            foreach (var item in array)
            {
                if (item.IsObject())
                {
                    var newRow = new object?[_columnNames!.Length];
                    var obj = item.AsObject();
                    
                    for (int c = 0; c < _columnNames.Length; c++)
                    {
                        var val = obj.Get(_columnNames[c]);
                        if (val.IsUndefined() || val.IsNull()) newRow[c] = null;
                        else if (val.IsString()) newRow[c] = val.AsString();
                        else if (val.IsNumber()) newRow[c] = val.AsNumber();
                        else if (val.IsBoolean()) newRow[c] = val.AsBoolean();
                        else newRow[c] = val.ToString();
                    }
                    yield return newRow;
                }
            }
        }
        
        _buffer.Clear();
    }

    public IEnumerable<object?[]> Flush()
    {
        // Process remaining buffer
        foreach (var r in ProcessBuffer()) yield return r;
    }

    private void EnsureFunctionCompiled(Engine engine)
    {
        if (_funcName != null && _wrappedScript != null)
        {
            if (engine.GetValue(_funcName).IsUndefined())
            {
                 // Re-register using SetValue/Evaluate
                 engine.SetValue(_funcName, engine.Evaluate($"({_wrappedScript})"));
            }
        }
    }
}
