using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;
using Jint.Runtime;

namespace DtPipe.Transformers.Row.Compute;

/// <summary>
/// Transforms data rows and Arrow batches using Jint.
/// </summary>
public sealed class ComputeDataTransformer : BaseColumnarTransformer, IRequiresOptions<DtPipe.Transformers.Row.Compute.ComputeOptions>
{
    private const int ParallelBatchThreshold = 1024;

    private readonly ComputeOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly Dictionary<string, string> _mappings;
    private readonly Dictionary<string, Type> _typeHints;
    private readonly bool _skipNull;

    private readonly ConditionalWeakTable<Engine, CompiledComputeState> _engineCache = new();

    private string[]? _columnNames;
    private IReadOnlyList<PipeColumnInfo>? _outputColumns;
    private ScriptColumnProcessor[]? _processors;
    private HashSet<int>? _writtenColumnIndices;
    private int _inputColumnCount;

    public override bool CanProcessColumnar => true;

    /// <summary>
    /// Every script gets the view the transformer moves from row to row; false when one may keep its row,
    /// and each row then gets a view of its own (<see cref="JsRowRetention"/>).
    /// </summary>
    internal bool SharesRowView { get; private set; } = true;

    public ComputeDataTransformer(ComputeOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
        _mappings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _typeHints = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

        foreach (var (col, typeStr) in options.ComputeTypes)
        {
            _typeHints[col] = DtPipe.Core.Helpers.TypeHelper.RequireTypeHint("--compute-types", col, typeStr);
        }

        foreach (var mapping in options.Compute)
        {
            var parts = mapping.Split(':', 3);
            if (parts.Length == 3 && DtPipe.Core.Helpers.TypeHelper.ParseTypeHint(parts[1]) is { } t)
            {
                _typeHints[parts[0]] = t;
                _mappings[parts[0]] = parts[2];
            }
            else if (mapping.Split(':', 2) is { Length: 2 } p2)
            {
                _mappings[p2[0]] = p2[1];
            }
        }

        _skipNull = options.SkipNull;
    }

    public bool HasScript => _mappings.Count > 0;

    public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> columns,
        CancellationToken ct = default)
    {
        await base.InitializeAsync(columns, ct);

        if (!HasScript)
        {
            _outputColumns = columns;
            return columns;
        }

        _inputColumnCount = columns.Count;

        var nameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < columns.Count; i++)
            nameToIndex[columns[i].Name] = i;

        var outputColumns = new List<PipeColumnInfo>(columns);
        var processors = new ScriptColumnProcessor[_mappings.Count];
        var writtenIndices = new HashSet<int>();
        var engine = _jsEngineProvider.GetEngine();
        var compiledInitialFunctions = new JsValue[_mappings.Count];
        int procIdx = 0;

        foreach (var (colName, script) in _mappings)
        {
            if (!nameToIndex.TryGetValue(colName, out var colIndex))
            {
                colIndex = outputColumns.Count;
                nameToIndex[colName] = colIndex;
                outputColumns.Add(new PipeColumnInfo(colName, typeof(string), IsNullable: true));
            }
            else
            {
                // The script writes this column's values now, so whatever numeric width the source
                // declared no longer describes them: a DECIMAL(10,2) whose script returns a
                // third decimal digit would be refused by a builder held to the source's scale.
                outputColumns[colIndex] = outputColumns[colIndex] with { Precision = null, Scale = null };
            }

            writtenIndices.Add(colIndex);
            var (function, wrappedScript) = CompileBody(engine, script);
            compiledInitialFunctions[procIdx] = function;
            bool isPure = JsAstAnalyzer.IsPureForParallel(wrappedScript);
            processors[procIdx] = new ScriptColumnProcessor(procIdx, colIndex, wrappedScript, isPure);
            procIdx++;
        }

        foreach (var entry in _typeHints)
        {
            if (nameToIndex.TryGetValue(entry.Key, out var idx))
                outputColumns[idx] = outputColumns[idx] with { ClrType = entry.Value };
        }

        _columnNames = outputColumns.Select(c => c.Name).ToArray();
        _outputColumns = outputColumns;
        _processors = processors;
        _writtenColumnIndices = writtenIndices;
        SharesRowView = processors.All(p => JsRowRetention.CanShareView(p.WrappedScript, JsRowArgument.Row));

        // Seed the calling thread's engine cache with the already-compiled functions so the first
        // row pays no second Evaluate pass.
        _engineCache.AddOrUpdate(engine, CreateState(engine, compiledInitialFunctions));

        return outputColumns;
    }

    protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (!HasScript || _processors == null || _processors.Length == 0 || _outputColumns == null || _columnNames == null)
            return new ValueTask<RecordBatch?>(batch);

        int outCount = _outputColumns.Count;
        var outFields = new Field[outCount];
        var outArrays = new IArrowArray[outCount];

        try
        {
            for (int i = 0; i < _inputColumnCount; i++)
            {
                if (_writtenColumnIndices!.Contains(i))
                {
                    var colInfo = _outputColumns[i];
                    outFields[i] = ArrowTypeMapper.GetField(colInfo.Name, colInfo.ClrType, isNullable: true, colInfo.Precision, colInfo.Scale);
                }
                else
                {
                    outFields[i] = batch.Schema.GetFieldByIndex(i);
                    outArrays[i] = ArrowOwnership.RetainArray(batch.Column(i));
                }
            }

            for (int i = _inputColumnCount; i < outCount; i++)
            {
                var colInfo = _outputColumns[i];
                outFields[i] = ArrowTypeMapper.GetField(colInfo.Name, colInfo.ClrType, isNullable: true, colInfo.Precision, colInfo.Scale);
            }

            if (_jsEngineProvider is JsEngineProvider
                && batch.Length >= ParallelBatchThreshold
                && _processors.All(p => p.IsPure))
            {
                ExecuteJintProcessorsParallel(batch, _processors, outFields, outArrays, ct);
            }
            else
            {
                ExecuteJintProcessorsSequential(batch, _processors, outFields, outArrays);
            }

            return new ValueTask<RecordBatch?>(new RecordBatch(new Schema(outFields, batch.Schema.Metadata), outArrays, batch.Length));
        }
        catch
        {
            for (int i = 0; i < outArrays.Length; i++)
                outArrays[i]?.Dispose();
            throw;
        }
    }

    private void ExecuteJintProcessorsSequential(
        RecordBatch batch,
        ScriptColumnProcessor[] processors,
        Field[] outFields,
        IArrowArray[] outArrays)
    {
        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
        var readers = SharesRowView ? null : ArrowTypeMapper.CompileValueReaders(batch);
        if (readers is null)
            state.ArrowRowView.BindBatch(batch);

        int count = processors.Length;
        var builders = new IArrowArrayBuilder[count];
        var appenders = new Action<object?>[count];
        var skipInputCols = new IArrowArray?[count];

        for (int p = 0; p < count; p++)
        {
            var proc = processors[p];
            builders[p] = ArrowTypeMapper.CreateBuilder(outFields[proc.ColumnIndex].DataType);
            appenders[p] = ArrowTypeMapper.ResolveAppender(builders[p]);
            if (_skipNull && proc.ColumnIndex < _inputColumnCount)
            {
                var col = batch.Column(proc.ColumnIndex);
                if (col.NullCount > 0) skipInputCols[p] = col;
            }
        }

        for (int rowIdx = 0; rowIdx < batch.Length; rowIdx++)
        {
            var args = state.ArgsFor(readers, rowIdx);

            for (int p = 0; p < count; p++)
            {
                var proc = processors[p];
                if (skipInputCols[p]?.IsNull(rowIdx) == true)
                {
                    JsValueConversion.AppendNullFast(builders[p]);
                    continue;
                }

                try
                {
                    var result = engine.Call(state.Functions[proc.Index], JsValue.Undefined, args);
                    ValidateComputeResult(result, _columnNames![proc.ColumnIndex]);
                    JsValueConversion.AppendJsValueToBuilder(builders[p], result, appenders[p]);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Error evaluating compute script for column '{_columnNames![proc.ColumnIndex]}': {ex.Message}", ex);
                }
            }
        }

        for (int p = 0; p < count; p++)
            outArrays[processors[p].ColumnIndex] = ArrowTypeMapper.BuildArray(builders[p]);
    }

    private void ExecuteJintProcessorsParallel(
        RecordBatch batch,
        ScriptColumnProcessor[] processors,
        Field[] outFields,
        IArrowArray[] outArrays,
        CancellationToken ct)
    {
        int len = batch.Length;
        int procCount = processors.Length;
        var targets = new ParallelColumnBuffer[procCount];

        for (int p = 0; p < procCount; p++)
        {
            var proc = processors[p];
            var clrType = _outputColumns![proc.ColumnIndex].ClrType;
            IArrowArray? skipCol = _skipNull && proc.ColumnIndex < _inputColumnCount && batch.Column(proc.ColumnIndex).NullCount > 0
                ? batch.Column(proc.ColumnIndex)
                : null;
            targets[p] = new ParallelColumnBuffer(proc, clrType, skipCol, len);
        }

        var readers = SharesRowView ? null : ArrowTypeMapper.CompileValueReaders(batch);

        try
        {
            Parallel.ForEach(
                Partitioner.Create(0, len),
                new ParallelOptions { CancellationToken = ct },
                range =>
                {
                    var engine = _jsEngineProvider.GetEngine();
                    var state = GetOrCreateState(engine);
                    if (readers is null)
                        state.ArrowRowView.BindBatch(batch);

                    for (int rowIdx = range.Item1; rowIdx < range.Item2; rowIdx++)
                    {
                        var args = state.ArgsFor(readers, rowIdx);

                        for (int p = 0; p < procCount; p++)
                        {
                            var buf = targets[p];
                            var proc = buf.Processor;
                            if (buf.SkipCol?.IsNull(rowIdx) == true)
                            {
                                buf.SetNull(rowIdx);
                                continue;
                            }

                            try
                            {
                                var result = engine.Call(state.Functions[proc.Index], JsValue.Undefined, args);
                                ValidateComputeResult(result, _columnNames![proc.ColumnIndex]);
                                buf.SetValue(rowIdx, result);
                            }
                            catch (Exception ex)
                            {
                                throw new InvalidOperationException(
                                    $"Error evaluating compute script for column '{_columnNames![proc.ColumnIndex]}': {ex.Message}", ex);
                            }
                        }
                    }
                });
        }
        catch (AggregateException aex) when (aex.InnerExceptions.Count > 0)
        {
            throw aex.InnerExceptions[0];
        }

        for (int p = 0; p < procCount; p++)
            outArrays[targets[p].Processor.ColumnIndex] = targets[p].BuildArrowArray(outFields[targets[p].Processor.ColumnIndex]);
    }

    public override object?[]? Transform(IReadOnlyList<object?> row)
    {
        if (_processors == null || _processors.Length == 0 || _columnNames == null)
            return row as object?[] ?? row.ToArray();

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);

        var outputLength = _columnNames.Length;
        object?[] rowArray;

        if (outputLength > row.Count)
        {
            rowArray = new object?[outputLength];
            for (int i = 0; i < row.Count; i++) rowArray[i] = row[i];
        }
        else
        {
            rowArray = row as object?[] ?? row.ToArray();
        }

        // Every script reads the row as it came in, as the Arrow path does: the view reads rowArray, so the
        // results are written only once every script has run. A view of its own copies the row first.
        JsValue[] args;
        if (SharesRowView)
        {
            state.RowView.SetRow(rowArray);
            args = state.SingleArg;
        }
        else
        {
            args = [state.RowView.Own(row)];
        }
        var results = state.RowResults;

        foreach (var processor in _processors)
        {
            // A skipped cell is null, so writing null back below leaves it as it came.
            if (_skipNull && processor.ColumnIndex < _inputColumnCount && rowArray[processor.ColumnIndex] is null)
            {
                results[processor.Index] = null;
                continue;
            }

            try
            {
                var result = engine.Call(state.Functions[processor.Index], JsValue.Undefined, args);
                ValidateComputeResult(result, _columnNames[processor.ColumnIndex]);
                results[processor.Index] = JsValueConversion.JsValueToClr(result);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Error evaluating compute script for column '{_columnNames[processor.ColumnIndex]}': {ex.Message}", ex);
            }
        }

        foreach (var processor in _processors)
            rowArray[processor.ColumnIndex] = results[processor.Index];

        return rowArray;
    }

    private static void ValidateComputeResult(JsValue result, string columnName)
    {
        // null is a value a script may mean; undefined is the absence of one. Writing it as NULL
        // would drop the data without a trace, so it stops the run and names the column.
        if (result.IsUndefined())
        {
            throw new InvalidOperationException(
                "the script returned undefined. Return a value, or null for an empty one: a body with "
                + "statements needs an explicit 'return', and a function must be called to give its result.");
        }

        if (result.IsNumber())
        {
            double d = result.AsNumber();
            if (double.IsInfinity(d) || double.IsNaN(d))
                throw new InvalidOperationException($"Compute script for column '{columnName}' resulted in {d} (possible division by zero).");
        }
    }

    /// <summary>
    /// Compiles a column's script as <c>function(row)</c>. A body that parses as one expression is
    /// returned as such, so an expression containing the word "return" (an immediately invoked
    /// function) still yields its value; anything else is taken as the statements of the function.
    /// </summary>
    private static (JsValue Function, string WrappedScript) CompileBody(Engine engine, string script)
    {
        var body = script.Trim();
        var expression = body.TrimEnd(';').Trim();

        if (expression.Length > 0)
        {
            var asExpression = $"function(row) {{ return ({expression}\n); }}";
            try
            {
                return (engine.Evaluate($"({asExpression})"), asExpression);
            }
            catch (JavaScriptException)
            {
                // Defining a function runs nothing, so the only failure here is a syntax error: the
                // body is not one expression. Fall through to the statement form below.
            }
        }

        var asStatements = $"function(row) {{ {body} }}";
        return (engine.Evaluate($"({asStatements})"), asStatements);
    }

    private CompiledComputeState GetOrCreateState(Engine engine)
        => _engineCache.GetValue(engine, e =>
        {
            var functions = new JsValue[_processors!.Length];
            for (int i = 0; i < _processors.Length; i++)
                functions[i] = e.Evaluate($"({_processors[i].WrappedScript})");
            return CreateState(e, functions);
        });

    private CompiledComputeState CreateState(Engine engine, JsValue[] functions)
    {
        var refErr = JsValueConversion.GetReferenceErrorConstructor(engine);
        var rowView = new JsRowObjectInstance(engine, _columnNames!, _inputColumnCount, refErr);
        var arrowRowView = new ArrowRowObjectInstance(engine, _columnNames!, _inputColumnCount, refErr);
        return new CompiledComputeState(functions, rowView, arrowRowView);
    }

    private sealed class CompiledComputeState(JsValue[] functions, JsRowObjectInstance rowView, ArrowRowObjectInstance arrowRowView)
    {
        public JsValue[] Functions { get; } = functions;
        public JsRowObjectInstance RowView { get; } = rowView;
        public ArrowRowObjectInstance ArrowRowView { get; } = arrowRowView;
        public JsValue[] SingleArg { get; } = [rowView];
        public JsValue[] ArrowArg { get; } = [arrowRowView];
        public object?[] RowResults { get; } = new object?[functions.Length];

        /// <summary>
        /// The arguments for a batch's row: the cursor, bound to the batch, when <paramref name="readers"/> is
        /// null; otherwise a view of the row's own, read when it is made.
        /// </summary>
        public JsValue[] ArgsFor(Func<int, object?>[]? readers, int rowIdx)
        {
            if (readers is not null)
                return [RowView.Own(readers, rowIdx)];

            ArrowRowView.SetRowIndex(rowIdx);
            return ArrowArg;
        }
    }

    private sealed class ParallelColumnBuffer
    {
        public ScriptColumnProcessor Processor { get; }
        public IArrowArray? SkipCol { get; }
        private readonly double[]? _doubles;
        private readonly bool[]? _doubleNulls;
        private readonly string?[]? _strings;
        private readonly object?[]? _objects;
        private volatile bool _hasAnyNull;

        public ParallelColumnBuffer(ScriptColumnProcessor processor, Type clrType, IArrowArray? skipCol, int length)
        {
            Processor = processor;
            SkipCol = skipCol;

            if (clrType == typeof(double))
            {
                _doubles = new double[length];
                _doubleNulls = new bool[length];
            }
            else if (clrType == typeof(string))
            {
                _strings = new string?[length];
            }
            else
            {
                _objects = new object?[length];
            }
        }

        public void SetNull(int idx)
        {
            if (_doubleNulls != null)
            {
                _doubleNulls[idx] = true;
                _hasAnyNull = true;
            }
            else if (_strings != null) _strings[idx] = null;
            else if (_objects != null) _objects[idx] = null;
        }

        public void SetValue(int idx, JsValue result)
        {
            if (_doubles != null)
            {
                if (result.IsNull())
                {
                    _doubleNulls![idx] = true;
                    _hasAnyNull = true;
                    return;
                }
                _doubles[idx] = result.IsNumber()
                    ? result.AsNumber()
                    : Convert.ToDouble(JsValueConversion.JsValueToClr(result), CultureInfo.InvariantCulture);
                return;
            }

            if (_strings != null)
            {
                _strings[idx] = result.IsNull() ? null : JsValueConversion.JsValueToString(result);
                return;
            }

            _objects![idx] = JsValueConversion.JsValueToClr(result);
        }

        public IArrowArray BuildArrowArray(Field field)
        {
            if (_doubles != null)
            {
                if (!_hasAnyNull)
                {
                    var buf = new ArrowBuffer.Builder<double>(_doubles.Length);
                    buf.Append(_doubles);
                    return new DoubleArray(buf.Build(), ArrowBuffer.Empty, _doubles.Length, 0, 0);
                }

                var dBuilder = new DoubleArray.Builder();
                for (int i = 0; i < _doubles.Length; i++)
                {
                    if (_doubleNulls![i]) dBuilder.AppendNull();
                    else dBuilder.Append(_doubles[i]);
                }
                return dBuilder.Build();
            }

            if (_strings != null)
            {
                var sBuilder = new StringArray.Builder();
                for (int i = 0; i < _strings.Length; i++)
                {
                    if (_strings[i] is null) sBuilder.AppendNull();
                    else sBuilder.Append(_strings[i]);
                }
                return sBuilder.Build();
            }

            var builder = ArrowTypeMapper.CreateBuilder(field.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(builder);
            foreach (var v in _objects!)
                appender(v);
            return ArrowTypeMapper.BuildArray(builder);
        }
    }

    private sealed record ScriptColumnProcessor(
        int Index,
        int ColumnIndex,
        string WrappedScript,
        bool IsPure);
}
