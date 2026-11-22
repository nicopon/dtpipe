using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;

namespace DtPipe.Transformers.Arrow.Filter;

public partial class FilterDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Arrow.Filter.FilterOptions>
{
    private readonly FilterOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly ConditionalWeakTable<Engine, CompiledFilterState> _engineCache = new();

    private string[]? _columnNames;
    private FilterStage[] _stages = [];

    public override bool CanProcessColumnar { get; protected set; }

    public FilterDataTransformer(FilterOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> sourceColumns,
        CancellationToken cancellationToken = default)
    {
        await base.InitializeAsync(sourceColumns, cancellationToken);

        CanProcessColumnar = true;

        if (_options.Filters == null || _options.Filters.Length == 0)
        {
            _stages = [];
            return sourceColumns;
        }

        _columnNames = sourceColumns.Select(c => c.Name).ToArray();
        var engine = _jsEngineProvider.GetEngine();

        var stages = new FilterStage[_options.Filters.Length];
        var initialFunctions = new JsValue[_options.Filters.Length];

        for (int i = 0; i < _options.Filters.Length; i++)
        {
            var rawExpr = _options.Filters[i];
            var filterScript = rawExpr.Trim();

            string body = filterScript;
            if (!body.Contains("return ") && !body.EndsWith(";"))
                body = "return " + body + ";";

            var wrappedScript = $"function(row) {{ {body} }}";
            initialFunctions[i] = engine.Evaluate($"({wrappedScript})");
            stages[i] = new FilterStage(i, rawExpr, wrappedScript);
        }

        _stages = stages;
        _engineCache.AddOrUpdate(engine, CreateState(engine, initialFunctions));

        return sourceColumns;
    }

    protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (!CanProcessColumnar || _stages.Length == 0 || batch.Length == 0)
            return new ValueTask<RecordBatch?>(batch);

        int len = batch.Length;
        var selectionMask = new bool[len];
        System.Array.Fill(selectionMask, true);

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
        state.ArrowRowView.BindBatch(batch);

        for (int s = 0; s < _stages.Length; s++)
        {
            var stage = _stages[s];
            var fn = state.Functions[stage.Index];
            for (int i = 0; i < len; i++)
            {
                if (!selectionMask[i]) continue;
                state.ArrowRowView.SetRowIndex(i);
                try
                {
                    var result = engine.Call(fn, JsValue.Undefined, state.ArrowArg);
                    selectionMask[i] = result.IsBoolean() && result.AsBoolean();
                }
                catch (JavaScriptException jsEx) when (IsMissingColumn(jsEx))
                {
                    throw new InvalidOperationException(
                        $"Error evaluating filter '{stage.RawExpression}': {jsEx.Message}. "
                        + "A filter reads its columns off 'row' — write row.<column>.", jsEx);
                }
                catch (Exception)
                {
                    // Property access on a null value: the filter reads it as "no match", on purpose.
                    selectionMask[i] = false;
                }
            }
        }

        int selectedCount = 0;
        for (int i = 0; i < len; i++)
            if (selectionMask[i]) selectedCount++;

        if (selectedCount == 0) return new ValueTask<RecordBatch?>(result: null);
        if (selectedCount == len) return new ValueTask<RecordBatch?>(batch);

        return new ValueTask<RecordBatch?>(ArrowColumnOperations.CompactBatch(batch, selectionMask, selectedCount));
    }

    public override object?[]? Transform(IReadOnlyList<object?> row)
    {
        if (_stages.Length == 0 || _columnNames == null)
            return row as object?[] ?? row.ToArray();

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
        state.RowView.SetRow(row);

        for (int i = 0; i < _stages.Length; i++)
        {
            var stage = _stages[i];
            try
            {
                var result = engine.Call(state.Functions[i], JsValue.Undefined, state.SingleArg);
                if (!result.IsBoolean() || !result.AsBoolean())
                    return null;
            }
            catch (JavaScriptException jsEx) when (IsMissingColumn(jsEx))
            {
                throw new InvalidOperationException(
                    $"Error evaluating filter '{stage.RawExpression}': {jsEx.Message}. "
                    + "A filter reads its columns off 'row' — write row.<column>.", jsEx);
            }
            catch (Exception)
            {
                // Property access on a null value: the filter reads it as "no match", on purpose.
                return null;
            }
        }

        return row as object?[] ?? row.ToArray();
    }

    /// <summary>
    /// A column absent from the schema and a property read on a null value are different failures,
    /// and the filter must not answer both with "no match".
    /// </summary>
    private static bool IsMissingColumn(JavaScriptException ex) =>
        ex.Error is ObjectInstance error && error.Get("name").ToString() == "ReferenceError";

    private CompiledFilterState GetOrCreateState(Engine engine)
        => _engineCache.GetValue(engine, e =>
        {
            var functions = new JsValue[_stages.Length];
            for (int i = 0; i < _stages.Length; i++)
                functions[i] = e.Evaluate($"({_stages[i].WrappedScript})");
            return CreateState(e, functions);
        });

    private CompiledFilterState CreateState(Engine engine, JsValue[] functions)
    {
        var refErr = JsValueConversion.GetReferenceErrorConstructor(engine);
        var rowView = new JsRowObjectInstance(engine, _columnNames!, _columnNames!.Length, refErr);
        var arrowRowView = new ArrowRowObjectInstance(engine, _columnNames!, _columnNames!.Length, refErr);
        return new CompiledFilterState(functions, rowView, arrowRowView);
    }

    private sealed class CompiledFilterState(JsValue[] functions, JsRowObjectInstance rowView, ArrowRowObjectInstance arrowRowView)
    {
        public JsValue[] Functions { get; } = functions;
        public JsRowObjectInstance RowView { get; } = rowView;
        public ArrowRowObjectInstance ArrowRowView { get; } = arrowRowView;
        public JsValue[] SingleArg { get; } = [rowView];
        public JsValue[] ArrowArg { get; } = [arrowRowView];
    }

    private sealed record FilterStage(
        int Index,
        string RawExpression,
        string WrappedScript);
}
