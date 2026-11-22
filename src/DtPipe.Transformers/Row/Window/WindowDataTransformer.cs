using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Transformers.Services;
using Jint;
using Jint.Native;

namespace DtPipe.Transformers.Row.Window;

public class WindowDataTransformer : BaseColumnarTransformer, IMultiRowTransformer, IRequiresOptions<DtPipe.Transformers.Row.Window.WindowOptions>
{
    private readonly WindowOptions _options;
    private readonly IJsEngineProvider _jsEngineProvider;
    private readonly ConditionalWeakTable<Engine, CompiledWindowState> _engineCache = new();

    private readonly List<object?[]> _buffer = new();
    private object? _lastKey = null;

    private string[]? _columnNames;
    private IReadOnlyList<PipeColumnInfo>? _outputColumns;
    private Schema? _outputBatchSchema;
    private string? _wrappedScript;
    private int _keyColumnIndex = -1;

    public override bool CanProcessColumnar => true;

    public WindowDataTransformer(WindowOptions options, IJsEngineProvider jsEngineProvider)
    {
        _options = options;
        _jsEngineProvider = jsEngineProvider;
    }

    public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(
        IReadOnlyList<PipeColumnInfo> sourceColumns,
        CancellationToken cancellationToken = default)
    {
        await base.InitializeAsync(sourceColumns, cancellationToken);

        _columnNames = sourceColumns.Select(c => c.Name).ToArray();

        if (!string.IsNullOrWhiteSpace(_options.Script))
        {
            var engine = _jsEngineProvider.GetEngine();
            var script = _options.Script.Trim();

            if (!script.Contains("return ") && !script.EndsWith(";"))
                script = "return " + script + ";";

            var wrappedScript = $"function(rows) {{ {script} }}";
            _wrappedScript = wrappedScript;

            var fn = engine.Evaluate($"({wrappedScript})");
            _engineCache.AddOrUpdate(engine, CreateState(engine, fn));
        }

        if (!string.IsNullOrWhiteSpace(_options.Key))
        {
            int idx = -1;
            for (int i = 0; i < sourceColumns.Count; i++)
            {
                if (sourceColumns[i].Name.Equals(_options.Key, StringComparison.OrdinalIgnoreCase))
                {
                    idx = i;
                    break;
                }
            }
            if (idx < 0)
                throw new ArgumentException($"Window key column '{_options.Key}' not found.");
            _keyColumnIndex = idx;
        }

        // The script emits every value of every row it yields, so no column keeps the numeric
        // width its source declared — see ComputeDataTransformer for the same rule.
        _outputColumns = sourceColumns.Select(c => c with { Precision = null, Scale = null }).ToList();
        return _outputColumns;
    }

    public override object?[]? Transform(IReadOnlyList<object?> row)
    {
        // Window aggregation cannot emit per-row results in 1:1 mode; callers route through TransformMany or TransformBatchAsync.
        return null;
    }

    public IEnumerable<object?[]> TransformMany(IReadOnlyList<object?> row)
    {
        bool flush = false;

        if (_keyColumnIndex >= 0)
        {
            var currentKey = row[_keyColumnIndex];
            if (_buffer.Count > 0 && !Equals(currentKey, _lastKey))
                flush = true;
            _lastKey = currentKey;
        }

        if (flush)
        {
            foreach (var r in ProcessBufferRows()) yield return r;
        }

        // Materialize because ArrowRowView is ephemeral across rows.
        _buffer.Add(row as object?[] ?? row.ToArray());

        if (_options.Count.HasValue && _buffer.Count >= _options.Count.Value)
        {
            foreach (var r in ProcessBufferRows()) yield return r;
        }
    }

    public override IEnumerable<object?[]> Flush()
    {
        foreach (var r in ProcessBufferRows()) yield return r;
    }

    protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_wrappedScript == null || _columnNames == null || _outputColumns == null)
            return new ValueTask<RecordBatch?>(batch);

        EnsureOutputBatchSchema(batch.Schema);
        if (batch.Length == 0)
            return new ValueTask<RecordBatch?>(result: null);

        int len = batch.Length;
        var readers = ArrowTypeMapper.CompileValueReaders(batch);

        // Window-all (no count, no key): buffer everything until FlushBatchAsync.
        if (!_options.Count.HasValue && _keyColumnIndex < 0)
        {
            for (int i = 0; i < len; i++)
                _buffer.Add(MaterializeRow(readers, i));
            return new ValueTask<RecordBatch?>(result: null);
        }

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);

        IArrowArrayBuilder[]? builders = null;
        Action<object?>[]? appenders = null;
        int outRowCount = 0;

        void EnsureBuilders()
        {
            if (builders != null) return;
            int colCount = _outputBatchSchema!.FieldsList.Count;
            builders = new IArrowArrayBuilder[colCount];
            appenders = new Action<object?>[colCount];
            for (int c = 0; c < colCount; c++)
            {
                builders[c] = ArrowTypeMapper.CreateBuilder(_outputBatchSchema.GetFieldByIndex(c).DataType);
                appenders[c] = ArrowTypeMapper.ResolveAppender(builders[c]);
            }
        }

        if (_keyColumnIndex < 0)
        {
            int windowCount = _options.Count!.Value;
            int pos = 0;

            if (_buffer.Count > 0)
            {
                int needed = Math.Max(0, windowCount - _buffer.Count);
                int take = Math.Min(needed, len);
                for (int i = 0; i < take; i++)
                    _buffer.Add(MaterializeRow(readers, pos++));

                if (_buffer.Count >= windowCount)
                {
                    EnsureBuilders();
                    outRowCount += ExecuteWindowRowsIntoBuilders(engine, state, builders!, appenders!);
                    _buffer.Clear();
                }
            }

            if (pos + windowCount <= len)
            {
                EnsureBuilders();
                var arrowPool = state.EnsureArrowPool(windowCount);
                foreach (var slot in arrowPool) slot.BindBatch(batch);

                var windowArgs = windowCount == 0
                    ? state.GetEmptyArgs()
                    : state.GetArrowPoolArgs(windowCount);

                if (windowCount <= 0)
                {
                    while (pos < len)
                    {
                        _buffer.Add(MaterializeRow(readers, pos++));
                        outRowCount += ExecuteWindowRowsIntoBuilders(engine, state, builders!, appenders!);
                        _buffer.Clear();
                    }
                }
                else
                {
                    while (pos + windowCount <= len)
                    {
                        for (int k = 0; k < windowCount; k++)
                            arrowPool[k].SetRowIndex(pos + k);

                        var result = EvaluateWindow(engine, state.Function, windowArgs);
                        outRowCount += AppendResultArrayToBuilders(result, builders!, appenders!, state.ColJsKeys);
                        pos += windowCount;
                    }
                }
            }

            while (pos < len)
                _buffer.Add(MaterializeRow(readers, pos++));
        }
        else
        {
            int maxWindow = _options.Count ?? int.MaxValue;
            var keyReader = readers[_keyColumnIndex];
            bool arrowBound = false;
            int pos = 0;

            while (pos < len)
            {
                if (_buffer.Count > 0)
                {
                    var key = keyReader(pos);
                    if (!Equals(key, _lastKey))
                    {
                        EnsureBuilders();
                        outRowCount += ExecuteWindowRowsIntoBuilders(engine, state, builders!, appenders!);
                        _buffer.Clear();
                        continue;
                    }

                    _buffer.Add(MaterializeRow(readers, pos++));
                    if (_buffer.Count >= maxWindow)
                    {
                        EnsureBuilders();
                        outRowCount += ExecuteWindowRowsIntoBuilders(engine, state, builders!, appenders!);
                        _buffer.Clear();
                    }
                }
                else
                {
                    var runKey = keyReader(pos);
                    int runEnd = pos + 1;
                    int limit = pos + maxWindow;
                    if (limit < pos) limit = int.MaxValue; // overflow guard when maxWindow == int.MaxValue
                    int cap = Math.Min(len, limit);

                    while (runEnd < cap && Equals(keyReader(runEnd), runKey))
                        runEnd++;

                    bool closesInBatch = runEnd < len || (runEnd - pos) >= maxWindow;
                    if (closesInBatch)
                    {
                        EnsureBuilders();
                        int runLen = runEnd - pos;
                        var arrowPool = state.EnsureArrowPool(runLen);
                        if (!arrowBound)
                        {
                            foreach (var slot in arrowPool) slot.BindBatch(batch);
                            arrowBound = true;
                        }
                        else
                        {
                            for (int k = 0; k < runLen; k++) arrowPool[k].BindBatch(batch);
                        }

                        for (int k = 0; k < runLen; k++)
                            arrowPool[k].SetRowIndex(pos + k);

                        var windowArgs = state.GetArrowPoolArgs(runLen);
                        var result = EvaluateWindow(engine, state.Function, windowArgs);
                        outRowCount += AppendResultArrayToBuilders(result, builders!, appenders!, state.ColJsKeys);
                        _lastKey = runKey;
                        pos = runEnd;
                    }
                    else
                    {
                        for (int i = pos; i < runEnd; i++)
                            _buffer.Add(MaterializeRow(readers, i));
                        _lastKey = runKey;
                        pos = runEnd;
                    }
                }
            }
        }

        if (builders == null || outRowCount == 0)
            return new ValueTask<RecordBatch?>(result: null);

        return new ValueTask<RecordBatch?>(BuildBatchFromBuilders(builders, outRowCount));
    }

    public override async IAsyncEnumerable<RecordBatch> FlushBatchAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_buffer.Count == 0 || _wrappedScript == null || _columnNames == null || _outputColumns == null)
            yield break;

        _outputBatchSchema ??= ArrowSchemaFactory.Create(_outputColumns);

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);

        int colCount = _outputBatchSchema.FieldsList.Count;
        var builders = new IArrowArrayBuilder[colCount];
        var appenders = new Action<object?>[colCount];
        for (int c = 0; c < colCount; c++)
        {
            builders[c] = ArrowTypeMapper.CreateBuilder(_outputBatchSchema.GetFieldByIndex(c).DataType);
            appenders[c] = ArrowTypeMapper.ResolveAppender(builders[c]);
        }

        int outRowCount = ExecuteWindowRowsIntoBuilders(engine, state, builders, appenders);
        _buffer.Clear();

        if (outRowCount > 0)
            yield return BuildBatchFromBuilders(builders, outRowCount);

        await Task.CompletedTask;
    }

    private RecordBatch BuildBatchFromBuilders(IArrowArrayBuilder[] builders, int rowCount)
    {
        var outArrays = new IArrowArray[builders.Length];
        try
        {
            for (int c = 0; c < builders.Length; c++)
                outArrays[c] = ArrowTypeMapper.BuildArray(builders[c]);

            return new RecordBatch(_outputBatchSchema!, outArrays, rowCount);
        }
        catch
        {
            for (int i = 0; i < outArrays.Length; i++)
                outArrays[i]?.Dispose();
            throw;
        }
    }

    private void EnsureOutputBatchSchema(Schema inputSchema)
    {
        if (_outputBatchSchema != null) return;

        var fields = new Field[inputSchema.FieldsList.Count];
        for (int i = 0; i < fields.Length; i++)
        {
            var f = inputSchema.GetFieldByIndex(i);
            if (f.DataType is Decimal128Type or Decimal256Type or Decimal32Type or Decimal64Type)
            {
                var outCol = _outputColumns![i];
                fields[i] = ArrowTypeMapper.GetField(outCol.Name, outCol.ClrType, isNullable: true, precision: null, scale: null);
            }
            else
            {
                fields[i] = f.IsNullable ? f : new Field(f.Name, f.DataType, nullable: true, f.Metadata);
            }
        }
        _outputBatchSchema = new Schema(fields, inputSchema.Metadata);
    }

    private IEnumerable<object?[]> ProcessBufferRows()
    {
        if (_buffer.Count == 0 || _wrappedScript == null || _columnNames == null)
            yield break;

        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);

        var rowPool = state.EnsureRowPool(_buffer.Count);
        for (int i = 0; i < _buffer.Count; i++)
            rowPool[i].SetRow(_buffer[i]);

        var result = EvaluateWindow(engine, state.Function, state.GetRowPoolArgs(_buffer.Count));
        _buffer.Clear();

        if (result.IsArray())
        {
            var array = result.AsArray();
            var colJsKeys = state.ColJsKeys;

            foreach (var item in array)
            {
                if (item.IsObject())
                {
                    var obj = item.AsObject();
                    var newRow = new object?[colJsKeys.Length];
                    for (int c = 0; c < colJsKeys.Length; c++)
                        newRow[c] = JsValueConversion.JsValueToClr(obj.Get(colJsKeys[c]));
                    yield return newRow;
                }
            }
        }
    }

    private int ExecuteWindowRowsIntoBuilders(
        Engine engine,
        CompiledWindowState state,
        IArrowArrayBuilder[] builders,
        Action<object?>[] appenders)
    {
        var rowPool = state.EnsureRowPool(_buffer.Count);
        for (int i = 0; i < _buffer.Count; i++)
            rowPool[i].SetRow(_buffer[i]);

        var result = EvaluateWindow(engine, state.Function, state.GetRowPoolArgs(_buffer.Count));
        return AppendResultArrayToBuilders(result, builders, appenders, state.ColJsKeys);
    }

    private static int AppendResultArrayToBuilders(
        JsValue result,
        IArrowArrayBuilder[] builders,
        Action<object?>[] appenders,
        JsString[] colJsKeys)
    {
        if (!result.IsArray()) return 0;

        int emitted = 0;
        var array = result.AsArray();
        foreach (var item in array)
        {
            if (!item.IsObject()) continue;
            var obj = item.AsObject();
            for (int c = 0; c < colJsKeys.Length; c++)
                JsValueConversion.AppendJsValueToBuilder(builders[c], obj.Get(colJsKeys[c]), appenders[c]);
            emitted++;
        }
        return emitted;
    }

    private static JsValue EvaluateWindow(Engine engine, JsValue fn, JsValue[] args)
    {
        try
        {
            return engine.Call(fn, JsValue.Undefined, args);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Error evaluating window script: {ex.Message}", ex);
        }
    }

    private static object?[] MaterializeRow(Func<int, object?>[] readers, int rowIndex)
    {
        var row = new object?[readers.Length];
        for (int i = 0; i < row.Length; i++)
            row[i] = readers[i](rowIndex);
        return row;
    }

    private CompiledWindowState GetOrCreateState(Engine engine)
        => _engineCache.GetValue(engine, e => CreateState(e, e.Evaluate($"({_wrappedScript})")));

    private CompiledWindowState CreateState(Engine engine, JsValue fn)
    {
        var refErr = JsValueConversion.GetReferenceErrorConstructor(engine);
        var keys = _columnNames!.Select(JsString.Create).ToArray();
        return new CompiledWindowState(engine, fn, _columnNames!, keys, refErr);
    }

    private sealed class CompiledWindowState(
        Engine engine,
        JsValue function,
        string[] columnNames,
        JsString[] colJsKeys,
        Jint.Native.Error.ErrorConstructor refErrorCtor)
    {
        private readonly List<JsRowObjectInstance> _rowPool = new();
        private readonly List<ArrowRowObjectInstance> _arrowPool = new();
        private readonly Dictionary<int, JsValue[]> _rowArgsBySize = new();
        private readonly Dictionary<int, JsValue[]> _arrowArgsBySize = new();
        private JsValue[]? _emptyArgs;

        public JsValue Function { get; } = function;
        public JsString[] ColJsKeys { get; } = colJsKeys;

        public List<JsRowObjectInstance> EnsureRowPool(int size)
        {
            while (_rowPool.Count < size)
                _rowPool.Add(new JsRowObjectInstance(engine, columnNames, columnNames.Length, refErrorCtor));
            return _rowPool;
        }

        public List<ArrowRowObjectInstance> EnsureArrowPool(int size)
        {
            while (_arrowPool.Count < size)
                _arrowPool.Add(new ArrowRowObjectInstance(engine, columnNames, columnNames.Length, refErrorCtor));
            return _arrowPool;
        }

        public JsValue[] GetEmptyArgs()
            => _emptyArgs ??= [new JsArray(engine, 0u, 0u)];

        public JsValue[] GetRowPoolArgs(int count)
        {
            if (count == 0) return GetEmptyArgs();
            if (!_rowArgsBySize.TryGetValue(count, out var args))
            {
                var items = new JsValue[count];
                for (int i = 0; i < count; i++) items[i] = _rowPool[i];
                args = [new JsArray(engine, items)];
                _rowArgsBySize[count] = args;
            }
            return args;
        }

        public JsValue[] GetArrowPoolArgs(int count)
        {
            if (count == 0) return GetEmptyArgs();
            if (!_arrowArgsBySize.TryGetValue(count, out var args))
            {
                var items = new JsValue[count];
                for (int i = 0; i < count; i++) items[i] = _arrowPool[i];
                args = [new JsArray(engine, items)];
                _arrowArgsBySize[count] = args;
            }
            return args;
        }
    }
}
