using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using JsTypes = Jint.Runtime.Types;

namespace DtPipe.Transformers.Row.Expand;

public class ExpandDataTransformer : BaseColumnarTransformer, IMultiRowTransformer, IRequiresOptions<DtPipe.Transformers.Row.Expand.ExpandOptions>
{
    private readonly ExpandOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly ConditionalWeakTable<Engine, CompiledExpandState> _engineCache = new();
    private readonly HashSet<string> _reportedUnknownKeys = new(StringComparer.OrdinalIgnoreCase);

    private string[]? _columnNames;
    private HashSet<string>? _knownColumnSet;
    private IReadOnlyList<PipeColumnInfo>? _outputColumns;
    private HashSet<int>? _retypedInputIndices;
    private int _inputColumnCount;
    private ExpandStage[] _stages = [];
    private JsAstAnalyzer.ExpandUnnestPlan? _unnestPlan;

    public override bool CanProcessColumnar => true;
    internal bool UsesNativeFastPath { get; private set; }

    public ExpandDataTransformer(ExpandOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> sourceColumns,
        CancellationToken cancellationToken = default)
    {
        await base.InitializeAsync(sourceColumns, cancellationToken);

        if (_options.Expand == null || _options.Expand.Length == 0)
        {
            _stages = [];
            _outputColumns = sourceColumns;
            UsesNativeFastPath = true;
            return sourceColumns;
        }

        _inputColumnCount = sourceColumns.Count;
        var engine = _jsEngineProvider.GetEngine();

        var stages = new ExpandStage[_options.Expand.Length];
        var initialFunctions = new JsValue[_options.Expand.Length];

        for (int i = 0; i < _options.Expand.Length; i++)
        {
            var expandScript = _options.Expand[i];
            var (function, compiledBody) = JsScriptCompiler.Compile(engine, "row", expandScript);
            initialFunctions[i] = function;
            stages[i] = new ExpandStage(i, expandScript.Trim(), compiledBody);
        }

        var updatedColumns = new List<PipeColumnInfo>(sourceColumns);
        var retypedInputIndices = new HashSet<int>();

        foreach (var entry in _options.ExpandTypes)
        {
            string col = entry.Key;
            Type type = ParseDeclaredType(col, entry.Value);

            var idx = updatedColumns.FindIndex(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase));
            if (idx >= 0)
            {
                updatedColumns[idx] = updatedColumns[idx] with { ClrType = type };
                if (idx < _inputColumnCount) retypedInputIndices.Add(idx);
            }
            else
            {
                updatedColumns.Add(new PipeColumnInfo(col, type, IsNullable: true));
            }
        }

        // Appended, never inserted: TransformMany indexes _columnNames by the incoming row's
        // position, so a declared column has to sit past the last source column.
        _columnNames = updatedColumns.Select(c => c.Name).ToArray();
        _knownColumnSet = new HashSet<string>(_columnNames, StringComparer.OrdinalIgnoreCase);
        _outputColumns = updatedColumns;
        _retypedInputIndices = retypedInputIndices;
        _stages = stages;

        _unnestPlan = stages.Length == 1
            ? JsAstAnalyzer.TryMatchExpandUnnest(_options.Expand[0], sourceColumns, updatedColumns)
            : null;

        // If an input column other than the target was explicitly retyped, keep the general builder path.
        if (_unnestPlan != null && retypedInputIndices.Any(idx => idx != _unnestPlan.TargetColumnIndex))
            _unnestPlan = null;

        UsesNativeFastPath = _unnestPlan != null;
        _engineCache.AddOrUpdate(engine, CreateState(engine, initialFunctions));

        return updatedColumns;
    }

    public override object?[]? Transform(IReadOnlyList<object?> row)
    {
        var results = TransformMany(row);
        return results.FirstOrDefault();
    }

    public IEnumerable<object?[]> TransformMany(IReadOnlyList<object?> row)
    {
        if (_stages.Length == 0 || _columnNames == null)
        {
            yield return row as object?[] ?? row.ToArray();
            yield break;
        }

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
        state.RowView.SetRow(row);

        IEnumerable<object?[]> currentRows = [row as object?[] ?? row.ToArray()];

        for (int e = 0; e < _stages.Length; e++)
        {
            var stage = _stages[e];
            var nextRows = new List<object?[]>();

            foreach (var r in currentRows)
            {
                JsValue[] callArgs;
                // The first stage reads the input row, which has the input columns only; a later stage reads a
                // row of every column, those the earlier stages declared included.
                if (e == 0)
                {
                    callArgs = state.SingleArg;
                }
                else
                {
                    state.IntermediateRowView.SetRow(r);
                    callArgs = state.IntermediateArg;
                }

                JsValue result;
                try
                {
                    result = engine.CallAndSettle(state.Functions[e], JsValue.Undefined, callArgs);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"--expand could not evaluate {Quote(stage.SourceExpression)}: {ex.Message}. "
                      + "It takes a JavaScript expression over 'row' — a bare column name is not one, "
                      + "write 'row.<column>'.", ex);
                }

                if (!TryGetElements(engine, result, out var elements))
                {
                    throw new InvalidOperationException(
                        $"--expand expects {Quote(stage.SourceExpression)} to return an array of row objects, "
                      + $"but it returned {Describe(result)}.");
                }

                foreach (var item in elements)
                {
                    if (!item.IsObject())
                    {
                        throw new InvalidOperationException(
                            $"--expand expects {Quote(stage.SourceExpression)} to return an array of row objects, "
                          + $"but an element of the array is {Describe(item)}. Map each element onto a row — "
                          + "'row.tags.map(t => ({ ...row, tag: t }))' — and declare the column it adds with "
                          + "--expand-types \"tag:string\".");
                    }

                    var obj = item.AsObject();
                    ReportKeysWithNoColumn(obj, stage.SourceExpression);

                    var newRow = new object?[_columnNames.Length];
                    for (int c = 0; c < state.ColumnJsKeys.Length; c++)
                        newRow[c] = JsValueConversion.JsValueToClr(obj.Get(state.ColumnJsKeys[c]));

                    nextRows.Add(newRow);
                }
            }
            currentRows = nextRows;
        }

        foreach (var r in currentRows)
            yield return r;
    }

    protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_stages.Length == 0 || _columnNames == null || _outputColumns == null)
            return new ValueTask<RecordBatch?>(batch);

        if (batch.Length == 0)
            return new ValueTask<RecordBatch?>(result: null);

        int outColCount = _outputColumns.Count;
        var outFields = new Field[outColCount];

        for (int c = 0; c < _inputColumnCount; c++)
        {
            if (_retypedInputIndices!.Contains(c))
            {
                var colInfo = _outputColumns[c];
                outFields[c] = ArrowTypeMapper.GetField(colInfo.Name, colInfo.ClrType, isNullable: true, colInfo.Precision, colInfo.Scale);
            }
            else
            {
                outFields[c] = batch.Schema.GetFieldByIndex(c);
            }
        }
        for (int c = _inputColumnCount; c < outColCount; c++)
        {
            var colInfo = _outputColumns[c];
            outFields[c] = ArrowTypeMapper.GetField(colInfo.Name, colInfo.ClrType, isNullable: true, colInfo.Precision, colInfo.Scale);
        }

        // Structural columnar unnest when the column is a non-null ListArray whose element field is
        // exactly the target output field.
        if (_unnestPlan is { } plan
            && batch.Column(plan.ListColumnIndex) is ListArray listArr
            && listArr.NullCount == 0
            && MovesAsIs(((ListType)listArr.Data.DataType).ValueField, outFields[plan.TargetColumnIndex]))
        {
            return new ValueTask<RecordBatch?>(ExecuteColumnarUnnest(batch, listArr, plan, outFields));
        }

        return new ValueTask<RecordBatch?>(ExecuteColumnarJint(batch, outFields));
    }

    // The unnest hands the list's values over as they are, so the target field must describe them exactly.
    // A TypeId alone leaves a decimal's precision and scale, a timestamp's unit and zone and a fixed width
    // unchecked: the batch's schema would then misdescribe the buffer, and every reader would misread it.
    // A type not named here, nested or parameterised, takes the JavaScript path.
    private static bool MovesAsIs(Field values, Field target)
    {
        var (a, b) = (values.DataType, target.DataType);
        if (a.TypeId != b.TypeId || !SameMetadata(values, target))
            return false;

        return (a, b) switch
        {
            (Decimal32Type x, Decimal32Type y) => (x.Precision, x.Scale) == (y.Precision, y.Scale),
            (Decimal64Type x, Decimal64Type y) => (x.Precision, x.Scale) == (y.Precision, y.Scale),
            (Decimal128Type x, Decimal128Type y) => (x.Precision, x.Scale) == (y.Precision, y.Scale),
            (Decimal256Type x, Decimal256Type y) => (x.Precision, x.Scale) == (y.Precision, y.Scale),
            (FixedSizeBinaryType x, FixedSizeBinaryType y) => x.ByteWidth == y.ByteWidth,
            (TimestampType x, TimestampType y) => x.Unit == y.Unit
                && string.Equals(x.Timezone ?? "", y.Timezone ?? "", StringComparison.Ordinal),
            (Time32Type x, Time32Type y) => x.Unit == y.Unit,
            (Time64Type x, Time64Type y) => x.Unit == y.Unit,
            (DurationType x, DurationType y) => x.Unit == y.Unit,
            _ => a.TypeId is ArrowTypeId.Boolean
                or ArrowTypeId.Int8 or ArrowTypeId.Int16 or ArrowTypeId.Int32 or ArrowTypeId.Int64
                or ArrowTypeId.UInt8 or ArrowTypeId.UInt16 or ArrowTypeId.UInt32 or ArrowTypeId.UInt64
                or ArrowTypeId.HalfFloat or ArrowTypeId.Float or ArrowTypeId.Double
                or ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.Binary or ArrowTypeId.LargeBinary
                or ArrowTypeId.Date32 or ArrowTypeId.Date64,
        };
    }

    // An extension name or a decimal's undeclared mark changes how a cell reads, so the fields carry the same metadata.
    private static bool SameMetadata(Field a, Field b)
    {
        if (!a.HasMetadata || !b.HasMetadata)
            return a.HasMetadata == b.HasMetadata;
        return a.Metadata.Count == b.Metadata.Count
            && a.Metadata.All(kv => b.Metadata.TryGetValue(kv.Key, out var value) && value == kv.Value);
    }

    private RecordBatch? ExecuteColumnarUnnest(RecordBatch batch, ListArray listArr, JsAstAnalyzer.ExpandUnnestPlan plan, Field[] outFields)
    {
        int inLen = batch.Length;
        ReadOnlySpan<int> offsets = listArr.ValueOffsets;
        int outLen = offsets[inLen] - offsets[0];

        if (outLen == 0)
            return null;

        int outColCount = outFields.Length;
        var outArrays = new IArrowArray[outColCount];

        try
        {
            for (int c = 0; c < _inputColumnCount; c++)
            {
                if (c == plan.TargetColumnIndex) continue;

                if (c == plan.ListColumnIndex)
                {
                    outArrays[c] = ArrowColumnOperations.GatherListArray(listArr, offsets, inLen, outLen);
                }
                else
                {
                    outArrays[c] = ArrowColumnOperations.GatherScalarArray(
                        batch.Column(c), batch.Schema.GetFieldByIndex(c), offsets, inLen, outLen);
                }
            }

            outArrays[plan.TargetColumnIndex] = ArrowColumnOperations.SliceArray(listArr.Values, offsets[0], outLen);

            for (int c = _inputColumnCount; c < outColCount; c++)
            {
                if (c == plan.TargetColumnIndex) continue;
                var b = ArrowTypeMapper.CreateBuilder(outFields[c].DataType);
                for (int r = 0; r < outLen; r++) JsValueConversion.AppendNullFast(b);
                outArrays[c] = ArrowTypeMapper.BuildArray(b);
            }

            return new RecordBatch(new Schema(outFields, batch.Schema.Metadata), outArrays, outLen);
        }
        catch
        {
            for (int i = 0; i < outArrays.Length; i++)
                outArrays[i]?.Dispose();
            throw;
        }
    }

    private RecordBatch? ExecuteColumnarJint(RecordBatch batch, Field[] outFields)
    {
        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
        state.ArrowRowView.BindBatch(batch);

        int colCount = outFields.Length;
        var builders = new IArrowArrayBuilder[colCount];
        var appenders = new Action<object?>[colCount];

        for (int c = 0; c < colCount; c++)
        {
            builders[c] = ArrowTypeMapper.CreateBuilder(outFields[c].DataType);
            appenders[c] = ArrowTypeMapper.ResolveAppender(builders[c]);
        }

        int outRowCount = 0;
        var stage0 = _stages[0];

        for (int rowIdx = 0; rowIdx < batch.Length; rowIdx++)
        {
            state.ArrowRowView.SetRowIndex(rowIdx);

            JsValue result0;
            try
            {
                result0 = engine.CallAndSettle(state.Functions[0], JsValue.Undefined, state.ArrowArg);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"--expand could not evaluate {Quote(stage0.SourceExpression)}: {ex.Message}. "
                  + "It takes a JavaScript expression over 'row' — a bare column name is not one, "
                  + "write 'row.<column>'.", ex);
            }

            if (!TryGetElements(engine, result0, out var elements0))
            {
                throw new InvalidOperationException(
                    $"--expand expects {Quote(stage0.SourceExpression)} to return an array of row objects, "
                  + $"but it returned {Describe(result0)}.");
            }

            if (_stages.Length == 1)
            {
                foreach (var item in elements0)
                {
                    if (!item.IsObject())
                    {
                        throw new InvalidOperationException(
                            $"--expand expects {Quote(stage0.SourceExpression)} to return an array of row objects, "
                          + $"but an element of the array is {Describe(item)}. Map each element onto a row — "
                          + "'row.tags.map(t => ({ ...row, tag: t }))' — and declare the column it adds with "
                          + "--expand-types \"tag:string\".");
                    }

                    var obj = item.AsObject();
                    ReportKeysWithNoColumn(obj, stage0.SourceExpression);

                    for (int c = 0; c < colCount; c++)
                        JsValueConversion.AppendJsValueToBuilder(builders[c], obj.Get(state.ColumnJsKeys[c]), appenders[c]);

                    outRowCount++;
                }
            }
            else
            {
                var currentRows = ExtractStageObjects(elements0, stage0.SourceExpression, state.ColumnJsKeys);

                for (int e = 1; e < _stages.Length; e++)
                {
                    var stage = _stages[e];
                    bool isLast = e == _stages.Length - 1;
                    var nextRows = isLast ? null : new List<object?[]>(currentRows.Count);

                    foreach (var r in currentRows)
                    {
                        state.IntermediateRowView.SetRow(r);
                        JsValue res;
                        try
                        {
                            res = engine.CallAndSettle(state.Functions[e], JsValue.Undefined, state.IntermediateArg);
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException(
                                $"--expand could not evaluate {Quote(stage.SourceExpression)}: {ex.Message}. "
                              + "It takes a JavaScript expression over 'row' — a bare column name is not one, "
                              + "write 'row.<column>'.", ex);
                        }

                        if (!TryGetElements(engine, res, out var elems))
                        {
                            throw new InvalidOperationException(
                                $"--expand expects {Quote(stage.SourceExpression)} to return an array of row objects, "
                              + $"but it returned {Describe(res)}.");
                        }

                        if (isLast)
                        {
                            foreach (var item in elems)
                            {
                                if (!item.IsObject())
                                {
                                    throw new InvalidOperationException(
                                        $"--expand expects {Quote(stage.SourceExpression)} to return an array of row objects, "
                                      + $"but an element of the array is {Describe(item)}. Map each element onto a row — "
                                      + "'row.tags.map(t => ({ ...row, tag: t }))' — and declare the column it adds with "
                                      + "--expand-types \"tag:string\".");
                                }
                                var obj = item.AsObject();
                                ReportKeysWithNoColumn(obj, stage.SourceExpression);
                                for (int c = 0; c < colCount; c++)
                                    JsValueConversion.AppendJsValueToBuilder(builders[c], obj.Get(state.ColumnJsKeys[c]), appenders[c]);
                                outRowCount++;
                            }
                        }
                        else
                        {
                            nextRows!.AddRange(ExtractStageObjects(elems, stage.SourceExpression, state.ColumnJsKeys));
                        }
                    }

                    if (!isLast) currentRows = nextRows!;
                }
            }
        }

        if (outRowCount == 0)
            return null;

        var outArrays = new IArrowArray[colCount];
        try
        {
            for (int c = 0; c < colCount; c++)
                outArrays[c] = ArrowTypeMapper.BuildArray(builders[c]);

            return new RecordBatch(new Schema(outFields, batch.Schema.Metadata), outArrays, outRowCount);
        }
        catch
        {
            for (int i = 0; i < outArrays.Length; i++)
                outArrays[i]?.Dispose();
            throw;
        }
    }

    private List<object?[]> ExtractStageObjects(IEnumerable<JsValue> elements, string expression, JsString[] colJsKeys)
    {
        var list = new List<object?[]>();
        foreach (var item in elements)
        {
            if (!item.IsObject())
            {
                throw new InvalidOperationException(
                    $"--expand expects {Quote(expression)} to return an array of row objects, "
                  + $"but an element of the array is {Describe(item)}. Map each element onto a row — "
                  + "'row.tags.map(t => ({ ...row, tag: t }))' — and declare the column it adds with "
                  + "--expand-types \"tag:string\".");
            }
            var obj = item.AsObject();
            ReportKeysWithNoColumn(obj, expression);
            var row = new object?[colJsKeys.Length];
            for (int c = 0; c < colJsKeys.Length; c++)
                row[c] = JsValueConversion.JsValueToClr(obj.Get(colJsKeys[c]));
            list.Add(row);
        }
        return list;
    }

    /// <summary>
    /// A key the output schema does not carry is dropped: the schema is fixed before the first
    /// row, so there is nowhere to put it. Say it once per key rather than per row — the run
    /// stays valid, but nothing disappears without a word.
    /// </summary>
    private void ReportKeysWithNoColumn(ObjectInstance obj, string expression)
    {
        if (obj is IRowObjectView { HasOverrides: false })
            return;

        foreach (var property in obj.GetOwnProperties())
        {
            var key = property.Key.ToString();
            if (_knownColumnSet!.Contains(key)) continue;
            if (!_reportedUnknownKeys.Add(key)) continue;

            Console.Error.WriteLine(
                $"[dtpipe] Warning: --expand {Quote(expression)} sets '{key}', which is not a column of the "
              + $"output, so it is dropped. Declare it with --expand-types \"{key}:string\" to keep it.");
        }
    }

    private static bool TryGetElements(Engine engine, JsValue value, out IEnumerable<JsValue> elements)
    {
        if (value.IsArray())
        {
            elements = value.AsArray();
            return true;
        }

        if (value.IsObject() && value.ToObject() is System.Collections.IEnumerable clr and not string)
        {
            elements = clr.Cast<object?>().Select(o => JsValue.FromObject(engine, o)).ToList();
            return true;
        }

        elements = System.Array.Empty<JsValue>();
        return false;
    }

    private static string Describe(JsValue value) => value.Type switch
    {
        JsTypes.String => "a string",
        JsTypes.Number => "a number",
        JsTypes.Boolean => "a boolean",
        JsTypes.Undefined => "undefined",
        JsTypes.Null => "null",
        JsTypes.Object => "a single object",
        _ => "a value of another kind"
    };

    private static string Quote(string expression)
        => expression.Length <= 60 ? $"'{expression}'" : $"'{expression[..57]}...'";

    private static Type ParseDeclaredType(string column, string? hint)
        => string.IsNullOrWhiteSpace(hint)
            ? typeof(string)
            : TypeHelper.RequireTypeHint("--expand-types", column, hint);

    private CompiledExpandState GetOrCreateState(Engine engine)
        => _engineCache.GetValue(engine, e =>
        {
            var functions = new JsValue[_stages.Length];
            for (int i = 0; i < _stages.Length; i++)
                functions[i] = JsScriptCompiler.CompilePrepared(e, "row", _stages[i].CompiledBody);
            return CreateState(e, functions);
        });

    private CompiledExpandState CreateState(Engine engine, JsValue[] functions)
    {
        var refErr = JsValueConversion.GetReferenceErrorConstructor(engine);
        var rowView = new JsRowObjectInstance(engine, _columnNames!, _inputColumnCount, refErr);
        var intermediateView = new JsRowObjectInstance(engine, _columnNames!, _columnNames!.Length, refErr);
        var arrowRowView = new ArrowRowObjectInstance(engine, _columnNames!, _inputColumnCount, refErr);
        var keys = _columnNames!.Select(JsString.Create).ToArray();
        return new CompiledExpandState(functions, rowView, intermediateView, arrowRowView, keys);
    }

    private sealed class CompiledExpandState(
        JsValue[] functions,
        JsRowObjectInstance rowView,
        JsRowObjectInstance intermediateRowView,
        ArrowRowObjectInstance arrowRowView,
        JsString[] columnJsKeys)
    {
        public JsValue[] Functions { get; } = functions;
        public JsRowObjectInstance RowView { get; } = rowView;
        public JsRowObjectInstance IntermediateRowView { get; } = intermediateRowView;
        public ArrowRowObjectInstance ArrowRowView { get; } = arrowRowView;
        public JsValue[] SingleArg { get; } = [rowView];
        public JsValue[] IntermediateArg { get; } = [intermediateRowView];
        public JsValue[] ArrowArg { get; } = [arrowRowView];
        public JsString[] ColumnJsKeys { get; } = columnJsKeys;
    }

    private sealed record ExpandStage(int Index, string SourceExpression, string CompiledBody);
}
