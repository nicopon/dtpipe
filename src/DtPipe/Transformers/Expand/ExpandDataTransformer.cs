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
            var funcName = $"__expand_{GetHashCode()}_{i}_{Guid.NewGuid().ToString("N")}";
            
            string body = expandScript.Trim();
            if (!body.Contains("return ") && !body.EndsWith(";"))
            {
               body = "return " + body + ";";
            }
            
            // Wrap in function
            var wrappedScript = $"function {funcName}(row) {{ {body} }}";
            engine.Execute(wrappedScript);
            
            _compiledExpands.Add(new JsString(funcName));
        }

        return ValueTask.FromResult(sourceColumns);
    }

    // Required by IDataTransformer (base interface), but for MultiRow usage we prefer TransformMany.
    // However, if pipeline calls Transform() on us, we should return the FIRST row or throw?
    // Or return null if we want to force usage of TransformMany?
    // Given the pipeline logic will check for IMultiRowTransformer, this might not be called.
    // But to be safe, return null or implement partial logic?
    // Let's return null to signify "Not a simple transform".
    // Or implemented as "Take first expanded row"? No, dangerous.
    public object?[]? Transform(object?[] row)
    {
        // Fallback: Return first expanded row?
        // Or throw NotSupportedException?
        // Ideally, the pipeline runner handles IMultiRowTransformer specifically.
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
        
        // Build JS Context (Same as Filter/Compute - should refactor!)
        var jsRow = new JsObject(engine);
        for (int i = 0; i < row.Length; i++)
        {
            jsRow.Set(_columnNames[i], JsValue.FromObject(engine, row[i]));
        }

        // We only support ONE expand step per transformer instance logically?
        // If multiple expands are provided, do we chain them?
        // Step 1 expands to N rows. Step 2 expands each of N rows to M rows?
        // If so, we need recursion here or just flat loop.
        
        // Helper to process a list of rows through a specific expand function
        IEnumerable<object?[]> currentRows = new[] { row };

        foreach (var funcName in _compiledExpands)
        {
            var nextRows = new List<object?[]>();
            
            foreach (var r in currentRows)
            {
                // We need to re-bind 'row' if we are chaining expands!
                // Because 'row' variable in JS context refers to the INPUT row of the transformer.
                // If we have multiple expands in one transformer, intermediate rows need to be bound.
                // This means rebuilding jsRow for every intermediate row. Expensive.
                
                // Optimization: If only 1 expand script (common case), we skip re-binding.
                
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

                var result = engine.Invoke(funcName.ToString(), currentJsRow);

                if (result.IsArray())
                {
                    var array = result.AsArray();
                    foreach (var item in array)
                    {
                        // Convert JS object back to row array?
                        // Or if script returns array of objects?
                        // "return [ { AGE: 1 }, { AGE: 2 } ]"
                        // Or "return [ row, row ]" (duplicates)
                        
                        // We expect the script to return an Array of Rows (objects).
                        // We need to map these objects back to object?[].
                        
                        if (item.IsObject())
                        {
                            var newRow = new object?[_columnNames.Length];
                            var obj = item.AsObject();
                            
                            // Map by column name
                            for (int c = 0; c < _columnNames.Length; c++)
                            {
                                var val = obj.Get(_columnNames[c]);
                                if (val.IsUndefined()) 
                                {
                                    // Keep original value? Or null?
                                    // If we are expanding, usually we modify some fields.
                                    // If script returns partial object, should we merge with method row?
                                    // JS 'row' is immutable-ish.
                                    // Common pattern: return [ {...row, copy: 1}, {...row, copy: 2} ]
                                    
                                    // If undefined, let's look at original row?
                                    // But original 'r' is accessible.
                                    // Actually, if user returns partial object, missing keys are undefined.
                                    // We should probably rely on user to return full objects or we implement merge.
                                    // Jint doesn't auto-merge.
                                    
                                    // Use 'r' (current input row) for missing values?
                                    // This matches 'project' logic where missing targets are null?
                                    // Let's assume user returns full objects or we treat missing as null.
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
            currentRows = nextRows;
        }

        foreach (var r in currentRows)
        {
            yield return r;
        }
    }
}
