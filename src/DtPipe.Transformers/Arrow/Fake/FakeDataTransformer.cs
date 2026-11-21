using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using Bogus;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Transforms data rows by replacing or creating columns using stateless UTF-8/primitive
/// generators backed by <see cref="Bogus.Database"/> lexicons with a zero-cache Bogus fallback.
/// Supports column references using {COLUMN_NAME} syntax for composing values.
/// </summary>
public sealed partial class FakeDataTransformer : BaseColumnarTransformer, IRequiresOptions<FakeOptions>
{
    private FakeColumnProcessor[]? _processors;
    private readonly FakerRegistry _fakerRegistry;
    private readonly FakeMappingParser _mappingParser;
    private readonly FakeOptions _options;
    private readonly string _locale;

    // Seed mode fields
    private readonly IReadOnlyList<string> _seedColumns;
    private readonly bool _seedRow;
    private readonly bool _skipNull;
    private readonly ulong _globalSeed64;
    private int[]? _seedColumnIndices;
    private long _rowCounter;

    // State initialized in InitializeAsync
    private int[] _nonTemplateColumns = [];
    private int[] _templateColumns = [];
    private bool[] _hasFakeForCol = [];
    private Dictionary<string, int>? _columnNameToIndex;
    private readonly List<string> _virtualColumns = [];
    private int _realColumnCount;
    private Schema? _outputSchema;
    private Schema? _lastInputSchema;
    private Schema? _cachedBatchSchema;

    // Regex for template substitution
    [GeneratedRegex(@"\{([^{}]+)\}", RegexOptions.Compiled)]
    private static partial Regex TemplatePattern();

    public override bool CanProcessColumnar => true;

    public FakeDataTransformer(FakeOptions options)
    {
        _options = options;
        _fakerRegistry = new FakerRegistry();
        _mappingParser = new FakeMappingParser(_fakerRegistry);
        _locale = options.Locale;
        _seedColumns = options.SeedColumn;
        _seedRow = options.SeedRow;
        _skipNull = options.SkipNull;

        bool deterministicMode = _seedRow || _seedColumns.Count > 0;
        _globalSeed64 = options.Seed.HasValue
            ? SplitMix64.Mix64(unchecked((ulong)options.Seed.Value) ^ 0x517CC1B727220A95UL)
            : (deterministicMode ? 0UL : unchecked((ulong)Random.Shared.NextInt64()));

        if (_seedRow && _seedColumns.Count > 0)
        {
            throw new ArgumentException("Options --fake-seed-row and --fake-seed-column cannot be used together.");
        }

        _mappingParser.ParseAll(options.Fake);
    }

    public bool HasFake => _mappingParser.HasMappings;
    public FakerRegistry Registry => _fakerRegistry;

    public override async ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        await base.InitializeAsync(columns, ct);
        if (!HasFake)
        {
            _processors = null;
            _nonTemplateColumns = [];
            _templateColumns = [];
            _hasFakeForCol = [];
            _lastInputSchema = null;
            _cachedBatchSchema = null;
            return columns;
        }

        // Load lexicon once per locale (also warms Bogus.Database before any parallel fallback).
        var lexicon = LocaleLexiconCache.Get(_locale);

        _columnNameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        _realColumnCount = columns.Count;
        for (var i = 0; i < columns.Count; i++)
        {
            _columnNameToIndex[columns[i].Name] = i;
        }

        _virtualColumns.Clear();
        foreach (var colName in _mappingParser.Mappings.Keys)
        {
            if (!_columnNameToIndex.ContainsKey(colName))
            {
                var virtualIndex = _realColumnCount + _virtualColumns.Count;
                _columnNameToIndex[colName] = virtualIndex;
                _virtualColumns.Add(colName);
            }
        }

        var totalColumns = _realColumnCount + _virtualColumns.Count;

        // Resolve seed column indices before building processors so salt mode is known.
        if (_seedColumns.Count > 0)
        {
            var indices = new int[_seedColumns.Count];
            for (var s = 0; s < _seedColumns.Count; s++)
            {
                var col = _seedColumns[s];
                if (!_columnNameToIndex.TryGetValue(col, out var idx))
                {
                    throw new InvalidOperationException($"Seed column '{col}' not found in result set.");
                }
                indices[s] = idx;
            }
            _seedColumnIndices = indices;
        }
        else
        {
            _seedColumnIndices = null;
        }

        bool deterministicMode = _seedRow || _seedColumnIndices is not null;

        var processors = new FakeColumnProcessor[totalColumns];
        var templateTargets = new List<int>();
        var nonTemplateColumns = new List<int>();
        var hasFakeForCol = new bool[totalColumns];

        for (var i = 0; i < columns.Count; i++)
        {
            var colName = columns[i].Name;
            if (!_mappingParser.Mappings.TryGetValue(colName, out var value))
                continue;

            RegisterColumnProcessor(i, colName, value, deterministicMode, lexicon, processors, nonTemplateColumns, templateTargets);
            hasFakeForCol[i] = true;
        }

        foreach (var virtualCol in _virtualColumns)
        {
            var virtualIndex = _columnNameToIndex[virtualCol];
            var value = _mappingParser.Mappings[virtualCol];

            RegisterColumnProcessor(virtualIndex, virtualCol, value, deterministicMode, lexicon, processors, nonTemplateColumns, templateTargets);
            hasFakeForCol[virtualIndex] = true;
        }

        _processors = processors;
        _nonTemplateColumns = nonTemplateColumns.ToArray();
        _templateColumns = TopologicalSortTemplates(templateTargets, processors);
        _hasFakeForCol = hasFakeForCol;
        _lastInputSchema = null;
        _cachedBatchSchema = null;

        // Prepare output columns schema
        var outputColumns = new List<PipeColumnInfo>(columns);
        foreach (var virtualColName in _virtualColumns)
        {
            var fakerPath = _mappingParser.Mappings[virtualColName];
            Type colType = typeof(string);

            if (!FakeMappingParser.IsTemplate(fakerPath))
            {
                var hashIndex = fakerPath.IndexOf('#');
                var basePath = hashIndex >= 0 ? fakerPath[..hashIndex] : fakerPath;
                colType = _fakerRegistry.GetReturnType(basePath) ?? typeof(string);
            }

            outputColumns.Add(new PipeColumnInfo(virtualColName, colType, true));
        }

        _outputSchema = ArrowSchemaFactory.Create(outputColumns);
        return outputColumns;
    }

    private void RegisterColumnProcessor(
        int colIdx,
        string colName,
        string value,
        bool deterministicMode,
        LocaleLexicon lexicon,
        FakeColumnProcessor[] processors,
        List<int> nonTemplateColumns,
        List<int> templateTargets)
    {
        if (FakeMappingParser.IsTemplate(value))
        {
            var (segments, refIndices) = CompileTemplate(value);
            processors[colIdx] = new FakeColumnProcessor(colIdx, segments, refIndices);
            templateTargets.Add(colIdx);
            return;
        }

        var pathSalt = StableHash.Compute64CaseInsensitive(value);
        // Deterministic seed modes derive salt solely from fakerPath (including any #variant) so adding,
        // reordering, or renaming target columns across tables preserves identical outputs.
        // Non-deterministic mode mixes column index so two columns with the same path differ.
        ulong colSalt = deterministicMode
            ? pathSalt
            : StableHash.Combine64(pathSalt, colIdx, 0x9E3779B97F4A7C15UL);

        var generator = BuildColumnGenerator(value, colName, lexicon);
        processors[colIdx] = new FakeColumnProcessor(colIdx, generator, colSalt);
        nonTemplateColumns.Add(colIdx);
    }

    private IFakeColumnGenerator BuildColumnGenerator(string fakerPath, string colName, LocaleLexicon lexicon)
    {
        var hashIndex = fakerPath.IndexOf('#');
        var basePath = hashIndex >= 0 ? fakerPath[..hashIndex] : fakerPath;

        var bogusDelegate = _fakerRegistry.GetGenerator(basePath);
        if (bogusDelegate is not null)
        {
            var native = NativeFakeGenerators.TryCreate(
                basePath,
                lexicon,
                static () => DateTime.UtcNow.Date);

            if (native is not null)
                return native;

            var returnType = _fakerRegistry.GetReturnType(basePath) ?? typeof(string);
            return new BogusFallbackGenerator(_locale, bogusDelegate, returnType);
        }

        var dotIndex = basePath.IndexOf('.');
        if (dotIndex > 0)
        {
            var datasetName = basePath[..dotIndex];
            if (_fakerRegistry.HasDataset(datasetName))
            {
                throw new ArgumentException($"Unknown fake provider: '{basePath}' for column '{colName}'. " +
                    $"Dataset '{datasetName}' exists but method '{basePath[(dotIndex + 1)..]}' was not found.");
            }
        }

        return NativeFakeGenerators.CreateLiteralConstant(fakerPath);
    }

    private (FakeTemplateSegment[] Segments, int[] RefIndices) CompileTemplate(string template)
    {
        var segments = new List<FakeTemplateSegment>();
        var refIndices = new HashSet<int>();
        int lastIndex = 0;

        foreach (Match match in TemplatePattern().Matches(template))
        {
            if (match.Index > lastIndex)
                segments.Add(FakeTemplateSegment.CreateLiteral(template[lastIndex..match.Index]));

            var refName = match.Groups[1].Value;
            if (_columnNameToIndex!.TryGetValue(refName, out var refIdx))
            {
                segments.Add(FakeTemplateSegment.CreateColumn(refIdx));
                refIndices.Add(refIdx);
            }
            else
            {
                segments.Add(FakeTemplateSegment.CreateLiteral(match.Value));
            }

            lastIndex = match.Index + match.Length;
        }

        if (lastIndex < template.Length)
            segments.Add(FakeTemplateSegment.CreateLiteral(template[lastIndex..]));

        return (segments.ToArray(), refIndices.ToArray());
    }

    private static int[] TopologicalSortTemplates(List<int> targets, FakeColumnProcessor[] processors)
    {
        if (targets.Count <= 1)
            return targets.ToArray();

        var targetSet = new HashSet<int>(targets);
        var visited = new HashSet<int>();
        var visiting = new HashSet<int>();
        var sorted = new List<int>(targets.Count);

        void Visit(int u)
        {
            if (visiting.Contains(u))
                throw new InvalidOperationException($"Cycle detected in fake template dependencies for column index {u}.");
            if (!visited.Add(u))
                return;

            visiting.Add(u);
            var refs = processors[u].ReferencedIndices;
            if (refs is not null)
            {
                foreach (var refIdx in refs)
                {
                    if (refIdx != u && targetSet.Contains(refIdx))
                        Visit(refIdx);
                }
            }
            visiting.Remove(u);
            sorted.Add(u);
        }

        foreach (var t in targets)
            Visit(t);

        return sorted.ToArray();
    }

    private Schema GetOrBuildBatchSchema(Schema inputSchema)
    {
        if (_cachedBatchSchema is not null && ReferenceEquals(_lastInputSchema, inputSchema))
            return _cachedBatchSchema;

        int totalColumns = _processors!.Length;
        var outputFields = new List<Field>(totalColumns);
        for (int i = 0; i < _realColumnCount; i++)
            outputFields.Add(_hasFakeForCol[i] ? _outputSchema!.GetFieldByIndex(i) : inputSchema.GetFieldByIndex(i));
        for (int i = _realColumnCount; i < totalColumns; i++)
            outputFields.Add(_outputSchema!.GetFieldByIndex(i));

        _cachedBatchSchema = new Schema(outputFields, null);
        _lastInputSchema = inputSchema;
        return _cachedBatchSchema;
    }

    protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_processors is null || _outputSchema is null)
            return new ValueTask<RecordBatch?>(batch);

        int rowCount = batch.Length;
        int totalColumns = _processors.Length;
        var batchSchema = GetOrBuildBatchSchema(batch.Schema);
        var resultArrays = new IArrowArray[totalColumns];

        // Non-fake real columns are aliased into the output and retained so the segment runner can
        // dispose the input batch without freeing buffers the output still references.
        for (int i = 0; i < _realColumnCount; i++)
        {
            if (!_hasFakeForCol[i])
                resultArrays[i] = ArrowOwnership.RetainArray(batch.Column(i));
        }

        long batchStartRow = _rowCounter;
        _rowCounter += rowCount;
        bool deterministicMode = _seedRow || _seedColumnIndices is not null;

        ulong[]? rentedKeys = null;
        bool[]? rentedHasKey = null;

        try
        {
            if (_seedColumnIndices is not null && rowCount > 0)
            {
                rentedKeys = ArrayPool<ulong>.Shared.Rent(rowCount);
                rentedHasKey = ArrayPool<bool>.Shared.Rent(rowCount);
                ArrowSeedKeyExtractor.FillRowKeys(batch, _seedColumnIndices, rentedKeys, rentedHasKey);
            }

            if (_nonTemplateColumns.Length >= 2 && rowCount >= 4096)
            {
                var parallelOptions = new ParallelOptions
                {
                    MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, _nonTemplateColumns.Length),
                    CancellationToken = ct
                };
                var keysSnapshot = rentedKeys;
                var hasKeySnapshot = rentedHasKey;

                Parallel.For(0, _nonTemplateColumns.Length, parallelOptions, k =>
                {
                    int colIdx = _nonTemplateColumns[k];
                    var proc = _processors[colIdx];
                    var skipSource = (_skipNull && colIdx < _realColumnCount) ? batch.Column(colIdx) : null;
                    ReadOnlySpan<ulong> rowKeysSpan = keysSnapshot is not null ? keysSnapshot.AsSpan(0, rowCount) : ReadOnlySpan<ulong>.Empty;
                    ReadOnlySpan<bool> hasKeySpan = hasKeySnapshot is not null ? hasKeySnapshot.AsSpan(0, rowCount) : ReadOnlySpan<bool>.Empty;

                    resultArrays[colIdx] = proc.Generator!.GenerateColumn(
                        batchSchema.GetFieldByIndex(colIdx),
                        rowCount,
                        rowKeysSpan,
                        hasKeySpan,
                        deterministicMode,
                        proc.ColSalt,
                        _globalSeed64,
                        batchStartRow,
                        skipSource);
                });
            }
            else
            {
                ReadOnlySpan<ulong> rowKeysSpan = rentedKeys is not null ? rentedKeys.AsSpan(0, rowCount) : ReadOnlySpan<ulong>.Empty;
                ReadOnlySpan<bool> hasKeySpan = rentedHasKey is not null ? rentedHasKey.AsSpan(0, rowCount) : ReadOnlySpan<bool>.Empty;

                for (int k = 0; k < _nonTemplateColumns.Length; k++)
                {
                    ct.ThrowIfCancellationRequested();
                    int colIdx = _nonTemplateColumns[k];
                    var proc = _processors[colIdx];
                    var skipSource = (_skipNull && colIdx < _realColumnCount) ? batch.Column(colIdx) : null;

                    resultArrays[colIdx] = proc.Generator!.GenerateColumn(
                        batchSchema.GetFieldByIndex(colIdx),
                        rowCount,
                        rowKeysSpan,
                        hasKeySpan,
                        deterministicMode,
                        proc.ColSalt,
                        _globalSeed64,
                        batchStartRow,
                        skipSource);
                }
            }

            if (_templateColumns.Length > 0)
            {
                ProcessTemplateColumns(batch, batchSchema, resultArrays, rowCount);
            }
        }
        finally
        {
            if (rentedKeys is not null)
                ArrayPool<ulong>.Shared.Return(rentedKeys);
            if (rentedHasKey is not null)
                ArrayPool<bool>.Shared.Return(rentedHasKey);
        }

        return new ValueTask<RecordBatch?>(new RecordBatch(batchSchema, resultArrays, rowCount));
    }

    private void ProcessTemplateColumns(RecordBatch batch, Schema batchSchema, IArrowArray[] resultArrays, int rowCount)
    {
        var sb = new StringBuilder(128);
        var inputReaders = new Dictionary<int, Func<int, object?>>();
        var outputReaders = new Dictionary<int, Func<int, object?>>();

        for (int t = 0; t < _templateColumns.Length; t++)
        {
            int colIdx = _templateColumns[t];
            var proc = _processors![colIdx];
            var segments = proc.TemplateSegments!;
            var skipSource = (_skipNull && colIdx < _realColumnCount) ? batch.Column(colIdx) : null;

            var targetField = batchSchema.GetFieldByIndex(colIdx);
            var isStringTarget = targetField.DataType is StringType;
            var strBuilder = isStringTarget ? new StringArray.Builder() : null;
            strBuilder?.Reserve(rowCount);
            var genericBuilder = isStringTarget ? null : ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var genericAppender = genericBuilder is not null ? ArrowTypeMapper.ResolveAppender(genericBuilder) : null;

            for (int row = 0; row < rowCount; row++)
            {
                if (skipSource is not null && skipSource.IsNull(row))
                {
                    if (strBuilder is not null) strBuilder.AppendNull();
                    else genericAppender!(null);
                    continue;
                }

                sb.Clear();
                for (int s = 0; s < segments.Length; s++)
                {
                    var seg = segments[s];
                    if (seg.IsLiteral)
                    {
                        sb.Append(seg.Literal);
                    }
                    else
                    {
                        var val = ReadTemplateCell(seg.ColumnIndex, colIdx, row, batch, batchSchema, resultArrays, inputReaders, outputReaders);
                        if (val is not null)
                            sb.Append(val);
                    }
                }

                var rendered = sb.ToString();
                if (strBuilder is not null) strBuilder.Append(rendered);
                else genericAppender!(rendered);
            }

            resultArrays[colIdx] = strBuilder is not null
                ? strBuilder.Build()
                : ArrowTypeMapper.BuildArray(genericBuilder!);
        }
    }

    private object? ReadTemplateCell(
        int refIdx,
        int targetColIdx,
        int rowIndex,
        RecordBatch batch,
        Schema batchSchema,
        IArrowArray[] resultArrays,
        Dictionary<int, Func<int, object?>> inputReaders,
        Dictionary<int, Func<int, object?>> outputReaders)
    {
        // Self-reference reads the incoming cell value rather than the unfinished target array.
        if (refIdx == targetColIdx || !_hasFakeForCol[refIdx])
        {
            if (refIdx >= _realColumnCount)
                return null;

            if (!inputReaders.TryGetValue(refIdx, out var reader))
            {
                reader = ArrowTypeMap.CompileValueReader(batch.Column(refIdx), batch.Schema.GetFieldByIndex(refIdx));
                inputReaders[refIdx] = reader;
            }
            return reader(rowIndex);
        }

        if (!outputReaders.TryGetValue(refIdx, out var outReader))
        {
            outReader = ArrowTypeMap.CompileValueReader(resultArrays[refIdx], batchSchema.GetFieldByIndex(refIdx));
            outputReaders[refIdx] = outReader;
        }
        return outReader(rowIndex);
    }
}
