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

        // A row's stages run together, as on the row path: the cursor moves once per row, so a stage
        // reads what the one before wrote to 'row', and a row a stage drops reaches no later stage.
        state.ArrowRowView.BindBatch(batch);
        for (int i = 0; i < len; i++)
        {
            state.ArrowRowView.SetRowIndex(i);
            for (int s = 0; s < _stages.Length; s++)
            {
                if (Matches(engine, state, s, state.ArrowArg)) continue;
                selectionMask[i] = false;
                break;
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
            if (!Matches(engine, state, i, state.SingleArg))
                return null;
        }

        return row as object?[] ?? row.ToArray();
    }

    /// <summary>
    /// Whether the stage keeps the row. A null or undefined value used as an object is "no match", on
    /// purpose; any other error of the script, a timeout or a memory limit included, fails the run naming
    /// the filter.
    /// </summary>
    private bool Matches(Engine engine, CompiledFilterState state, int stageIndex, JsValue[] args)
    {
        var stage = _stages[stageIndex];
        try
        {
            var result = engine.CallAndSettle(state.Functions[stage.Index], JsValue.Undefined, args);
            return result.IsBoolean() && result.AsBoolean();
        }
        catch (JavaScriptException jsEx) when (IsNullPropertyRead(jsEx))
        {
            return false;
        }
        catch (JavaScriptException jsEx) when (IsMissingColumn(jsEx))
        {
            throw new InvalidOperationException(
                $"Error evaluating filter '{stage.RawExpression}': {jsEx.Message}. "
                + "A filter reads its columns off 'row' — write row.<column>.", jsEx);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Error evaluating filter '{stage.RawExpression}': {ex.Message}", ex);
        }
    }

    /// <summary>
    /// A column absent from the schema and a property read on a null value are different failures,
    /// and the filter must not answer both with "no match".
    /// </summary>
    private static bool IsMissingColumn(JavaScriptException ex) =>
        ex.Error is ObjectInstance error && error.Get("name").ToString() == "ReferenceError";

    /// <summary>
    /// A null or undefined value used as an object. The engine raises it as a plain TypeError with no
    /// structured kind, so its wording is the only handle: <c>Cannot read properties of null (reading 'x')</c>
    /// for a property, <c>Cannot read property 'x' of null</c> for a method call, <c>Cannot convert undefined
    /// or null to object</c> for a destructuring, a spread, an iteration or <c>Object.keys</c>, and
    /// <c>Cannot use 'in' operator to search for 'x' in null</c> for <c>in</c>. The tests of each form break
    /// if a newer engine words them otherwise. The wording cannot tell a null column from a null the script
    /// made itself: both are "no match".
    /// </summary>
    private static bool IsNullPropertyRead(JavaScriptException ex)
    {
        if (ex.Error is not ObjectInstance error || error.Get("name").ToString() != "TypeError")
            return false;

        var message = error.Get("message").ToString();
        if (message.StartsWith("Cannot read propert", StringComparison.Ordinal))
            return message.Contains(" of null", StringComparison.Ordinal) || message.Contains(" of undefined", StringComparison.Ordinal);

        return message == "Cannot convert undefined or null to object"
            || (message.StartsWith("Cannot use 'in' operator to search for ", StringComparison.Ordinal)
                && (message.EndsWith(" in null", StringComparison.Ordinal) || message.EndsWith(" in undefined", StringComparison.Ordinal)));
    }

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
