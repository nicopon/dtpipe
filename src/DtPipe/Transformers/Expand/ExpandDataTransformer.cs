using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Services;
using DtPipe.Core.Options;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Expand;

public class ExpandDataTransformer : IMultiRowTransformer, IRequiresOptions<ExpandOptions>
{
    private readonly ExpandOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly List<JsValue> _compiledExpands = new();
    private readonly List<string> _wrappedScripts = new();
    private string[]? _columnNames;

    public ExpandDataTransformer(ExpandOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> sourceColumns, CancellationToken cancellationToken = default)
    {
        if (_options.Expand == null || _options.Expand.Length == 0)
        {
            return ValueTask.FromResult(sourceColumns);
        }

        _columnNames = sourceColumns.Select(c => c.Name).ToArray();
        var engine = _jsEngineProvider.GetEngine();

        for (int i = 0; i < _options.Expand.Length; i++)
        {
            var expandScript = _options.Expand[i];
            var funcName = $"__expand_{Guid.NewGuid():N}";
            
            string body = expandScript.Trim();
            if (!body.Contains("return ") && !body.EndsWith(";"))
            {
               body = "return " + body + ";";
            }
            
            // Wrap in function expression
            var wrappedScript = $"function(row) {{ {body} }}";
            
            // Register globally
            engine.SetValue(funcName, engine.Evaluate($"({wrappedScript})"));
            
            _wrappedScripts.Add(wrappedScript);
            
            _compiledExpands.Add(new JsString(funcName));
        }

        return ValueTask.FromResult(sourceColumns);
    }

    // Required by IDataTransformer (base interface)
    public object?[]? Transform(object?[] row)
    {
        var results = TransformMany(row);
        return results.FirstOrDefault();
    }

    public IEnumerable<object?[]> TransformMany(object?[] row)
    {
        if (_compiledExpands.Count == 0 || _columnNames == null) 
        {
            yield return row;
            yield break;
        }

        var engine = _jsEngineProvider.GetEngine();
        EnsureFunctionsCompiled(engine);
        
        // Build JS Context (Same as Filter/Compute - should refactor!)
        var jsRow = new JsObject(engine);
        for (int i = 0; i < row.Length; i++)
        {
            jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
        }

        // Helper to process a list of rows through a specific expand function
        IEnumerable<object?[]> currentRows = new[] { row };

        foreach (var funcName in _compiledExpands)
        {
            var nextRows = new List<object?[]>();
            
            foreach (var r in currentRows)
            {
                JsValue currentJsRow;
                if (_compiledExpands.Count == 1) 
                {
                    currentJsRow = jsRow; // Use the one built above
                }
                else
                {
                    // Rebuild for intermediate rows
                     currentJsRow = new JsObject(engine);
                     for (int k = 0; k < r.Length; k++)
                     {
                         currentJsRow.AsObject().Set(_columnNames[k], JsValue.FromObject(engine, r[k]));
                     }
                }

                // Set 'row' in global scope for Evaluate Call
                engine.SetValue("row", currentJsRow);

                try 
                {
                    var result = engine.Evaluate($"{funcName}(row)");
    
                    // Result should be array of rows
                    if (result.IsArray())
                    {
                        var array = result.AsArray();
                        // Console.Error.WriteLine($"[Window] Result array length: {array.Length}");
                        foreach (var item in array)
                        {
                            if (item.IsObject())
                            {
                                var newRow = new object?[_columnNames.Length];
                                var obj = item.AsObject();
                                
                                // Console.Error.WriteLine($"[Window] Item Value: {obj.Get("Value")}");

                                // Map by column name
                                for (int c = 0; c < _columnNames.Length; c++)
                                {
                                    var val = obj.Get(_columnNames[c]);
                                    if (val.IsUndefined()) 
                                    {
                                        newRow[c] = null;
                                    }
                                    else
                                    {
                                        // Convert JsValue to primitive
                                        if(val.IsString()) newRow[c] = val.AsString();
                                        else if(val.IsNumber()) newRow[c] = val.AsNumber();
                                        else if(val.IsBoolean()) newRow[c] = val.AsBoolean();
                                        else if(val.IsNull()) newRow[c] = null;
                                        else newRow[c] = val.ToString();
                                    }
                                }
                                nextRows.Add(newRow);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                     throw new InvalidOperationException($"Error evaluating expand script '{funcName}': {ex.Message}", ex);
                }
            }
            currentRows = nextRows;
        }

        foreach (var r in currentRows)
        {
            yield return r;
        }
    }

    private void EnsureFunctionsCompiled(Engine engine)
    {
        if (_compiledExpands.Count > 0)
        {
            var firstFunc = _compiledExpands[0].ToString();
            var val = engine.GetValue(firstFunc);
            if (val.IsUndefined())
            {
                for (int i=0; i < _compiledExpands.Count; i++)
                {
                    var script = _wrappedScripts[i];
                    var name = _compiledExpands[i].ToString();
                    engine.SetValue(name, engine.Evaluate($"({script})"));
                }
            }
        }
    }
}
