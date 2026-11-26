using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
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
    private static bool IsNullCell(IArrowArray column, int row)
        => column is DictionaryArray ? ArrowTypeMap.GetValue(column, row) is null : column.IsNull(row);

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
            var (function, compiledBody) = JsScriptCompiler.Compile(engine, "row", script);
            compiledInitialFunctions[procIdx] = function;
            bool isPure = JsAstAnalyzer.IsPureForParallel(JsScriptCompiler.AsFunctionExpression("row", compiledBody));
            processors[procIdx] = new ScriptColumnProcessor(procIdx, colIndex, compiledBody, isPure);
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

            ExecuteJintProcessors(batch, _processors, outFields, outArrays, ct);

            return new ValueTask<RecordBatch?>(new RecordBatch(new Schema(outFields, batch.Schema.Metadata), outArrays, batch.Length));
        }
        catch
        {
            for (int i = 0; i < outArrays.Length; i++)
                outArrays[i]?.Dispose();
            throw;
        }
    }

    private void ExecuteJintProcessors(
        RecordBatch batch,
        ScriptColumnProcessor[] processors,
        Field[] outFields,
        IArrowArray[] outArrays,
        CancellationToken ct)
    {
        bool parallel = _jsEngineProvider.EnginePerThread
            && batch.Length >= ParallelBatchThreshold
            && processors.All(p => p.IsPure);

        if (!parallel)
        {
            var columns = ConvertRange(batch, processors, outFields, 0, batch.Length);
            for (int p = 0; p < processors.Length; p++)
                outArrays[processors[p].ColumnIndex] = columns[p];
            return;
        }

        // Each range is converted exactly as a whole batch is, by its own builders; the ranges are then
        // joined in row order, so the two paths hold no conversion of their own to drift apart.
        var ranges = Partitioner.Create(0, batch.Length).GetDynamicPartitions().ToArray();
        var chunks = new IArrowArray[ranges.Length][];
        try
        {
            try
            {
                Parallel.For(
                    0,
                    ranges.Length,
                    new ParallelOptions { CancellationToken = ct },
                    r => chunks[r] = ConvertRange(batch, processors, outFields, ranges[r].Item1, ranges[r].Item2));
            }
            catch (AggregateException aex) when (aex.InnerExceptions.Count > 0)
            {
                ExceptionDispatchInfo.Capture(aex.InnerExceptions[0]).Throw();
            }

            for (int p = 0; p < processors.Length; p++)
            {
                var column = new IArrowArray[ranges.Length];
                for (int r = 0; r < ranges.Length; r++)
                    column[r] = chunks[r][p];
                outArrays[processors[p].ColumnIndex] = ArrowArrayConcatenator.Concatenate(column)!;
            }
        }
        finally
        {
            foreach (var chunk in chunks)
            {
                if (chunk is null) continue;
                foreach (var array in chunk)
                    array.Dispose();
            }
        }
    }

    /// <summary>
    /// Runs every script over the rows of <paramref name="start"/> to <paramref name="end"/> and converts each
    /// result into the builder of its output column: the one place the Arrow path turns a script's value into
    /// a cell, whatever the size of the batch.
    /// </summary>
    private IArrowArray[] ConvertRange(
        RecordBatch batch,
        ScriptColumnProcessor[] processors,
        Field[] outFields,
        int start,
        int end)
    {
        var engine = _jsEngineProvider.GetEngine();
        var state = GetOrCreateState(engine);
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
                // A dictionary's entry may be null while its index is not: the array's own null count misses it.
                if (col.NullCount > 0 || col is DictionaryArray) skipInputCols[p] = col;
            }
        }

        for (int rowIdx = start; rowIdx < end; rowIdx++)
        {
            state.ArrowRowView.SetRowIndex(rowIdx);

            for (int p = 0; p < count; p++)
            {
                var proc = processors[p];
                if (skipInputCols[p] is { } skipCol && IsNullCell(skipCol, rowIdx))
                {
                    JsValueConversion.AppendNullFast(builders[p]);
                    continue;
                }

                try
                {
                    var result = engine.CallAndSettle(state.Functions[proc.Index], JsValue.Undefined, state.ArrowArg);
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

        var columns = new IArrowArray[count];
        for (int p = 0; p < count; p++)
            columns[p] = ArrowTypeMapper.BuildArray(builders[p]);
        return columns;
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
        // results are written only once every script has run.
        state.RowView.SetRow(rowArray);
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
                var result = engine.CallAndSettle(state.Functions[processor.Index], JsValue.Undefined, state.SingleArg);
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

    private CompiledComputeState GetOrCreateState(Engine engine)
        => _engineCache.GetValue(engine, e =>
        {
            var functions = new JsValue[_processors!.Length];
            for (int i = 0; i < _processors.Length; i++)
                functions[i] = JsScriptCompiler.CompilePrepared(e, "row", _processors[i].CompiledBody);
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
    }

    private sealed record ScriptColumnProcessor(
        int Index,
        int ColumnIndex,
        string CompiledBody,
        bool IsPure);
}
