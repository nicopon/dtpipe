using System.Threading.Channels;
using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;

namespace DtPipe.XStreamers.Native;

/// <summary>
/// Implements a hash join between two memory channels.
/// </summary>
/// <remarks>
/// <b>Limitation:</b> Only single-column join keys are supported via <c>--on MainCol=RefCol</c>.
/// Composite key joins are not supported in this version.
/// The reference stream (<c>--ref</c>) is fully materialized into a Dictionary in RAM (hash build phase).
/// It is recommended to use the smallest dataset as the reference.
/// A warning is emitted if the reference exceeds 50,000 rows.
/// </remarks>
public class NativeJoinXStreamer : IStreamReader
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _mainAlias;
    private readonly string _refAlias;
    private readonly string _mainKeyCol;
    private readonly string _refKeyCol;
    private readonly string[] _selectCols;
    private readonly string _joinType;
    private readonly ILogger _logger;

    private ChannelReader<IReadOnlyList<object?[]>>? _mainChannel;
    private ChannelReader<IReadOnlyList<object?[]>>? _refChannel;

    private IReadOnlyList<PipeColumnInfo>? _mainColumns;
    private IReadOnlyList<PipeColumnInfo>? _refColumns;
    private IReadOnlyList<PipeColumnInfo>? _outputColumns;

    private int _mainKeyIndex = -1;
    private int _refKeyIndex = -1;
    private readonly Dictionary<string, int> _refSelectIndices = new(StringComparer.OrdinalIgnoreCase);

    // Hash map to hold the fully materialized reference stream
    // Using string serialization of objects as naive hash key for now (TODO: optimize)
    private readonly Dictionary<string, object?[]> _refMap = new();

    public NativeJoinXStreamer(
        IMemoryChannelRegistry registry,
        string mainAlias,
        string refAlias,
        string onClause,
        string joinType,
        string selectCols,
        ILogger logger)
    {
        _registry = registry;
        _mainAlias = mainAlias;
        _refAlias = refAlias;
        _joinType = joinType;
        _selectCols = selectCols.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        _logger = logger;

        var parts = onClause.Split('=', StringSplitOptions.TrimEntries);
        if (parts.Length != 2 || parts[0].Contains(',') || parts[1].Contains(','))
        {
            throw new ArgumentException(
                $"Invalid '--on' clause: '{onClause}'. " +
                "Only single-column joins are supported (format: 'MainColumn=RefColumn'). " +
                "Composite key joins are not supported in this version.",
                nameof(onClause));
        }

        _mainKeyCol = parts[0];
        _refKeyCol = parts[1];
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _outputColumns;

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Wait for Main Channel schema
        _mainColumns = await _registry.WaitForChannelColumnsAsync(_mainAlias, ct);
        var mainEntry = _registry.GetChannel(_mainAlias);
        if (!mainEntry.HasValue) throw new InvalidOperationException($"Upstream memory channel '{_mainAlias}' not found.");
        _mainChannel = mainEntry.Value.Channel;

        // Wait for Reference Channel schema
        _refColumns = await _registry.WaitForChannelColumnsAsync(_refAlias, ct);
        var refEntry = _registry.GetChannel(_refAlias);
        if (!refEntry.HasValue) throw new InvalidOperationException($"Upstream memory channel '{_refAlias}' not found.");
        _refChannel = refEntry.Value.Channel;

        // Find Keys
        _mainKeyIndex = FindColumnIndex(_mainColumns, _mainKeyCol, _mainAlias);
        _refKeyIndex = FindColumnIndex(_refColumns, _refKeyCol, _refAlias);

        var outputCols = new List<PipeColumnInfo>(_mainColumns);

        // Bind Select Columns
        foreach (var colSpec in _selectCols)
        {
            int refIdx = FindColumnIndex(_refColumns, colSpec, _refAlias);
            _refSelectIndices[colSpec] = refIdx;

            var sourceCol = _refColumns[refIdx];
            // Forward the schema definition of the selected column exactly as it came from the reference provider!
            var newColInfo = new PipeColumnInfo(sourceCol.Name, sourceCol.ClrType, sourceCol.IsNullable, sourceCol.IsCaseSensitive, sourceCol.OriginalName);
            outputCols.Add(newColInfo);
        }

        _outputColumns = outputCols;

        // -------------------------------------------------------------
        // PHASE 1: Build the Reference Dictionary (Materialization)
        // -------------------------------------------------------------
        _logger.LogInformation("Building In-Memory Hash Join using '{RefAlias}' as reference table on key '{RefKeyCol}'", _refAlias, _refKeyCol);

        int refRowsCount = 0;
        await foreach (var rowBatch in _refChannel.ReadAllAsync(ct))
        {
            foreach (var row in rowBatch)
            {
                var keyVal = row[_refKeyIndex]?.ToString();
                if (!string.IsNullOrEmpty(keyVal))
                {
                    _refMap.TryAdd(keyVal, row); // Depending on semantics: Keep First or Key Conflict? TryAdd = Keep First
                }
                refRowsCount++;
            }
        }
        _logger.LogInformation("Hash Map built with {Count} indexed rows from '{RefAlias}'.", _refMap.Count, _refAlias);

        // Warning if the reference is large
        if (_refMap.Count > 50_000)
        {
            _logger.LogWarning(
                "The reference stream '{RefAlias}' contains {Count} rows fully loaded in RAM. " +
                "For best performance, '--ref' should point to the smaller side of the join. " +
                "Consider pre-filtering the reference dataset.",
                _refAlias, _refMap.Count);
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_mainChannel == null || _outputColumns == null) yield break;

        var buffer = new List<object?[]>(batchSize);
        long mainRowsMatched = 0;
        long mainRowsDiscarded = 0;

        _logger.LogInformation("Native Hash Join Phase 2: Probing streaming data from '{MainAlias}'...", _mainAlias);

        // -------------------------------------------------------------
        // PHASE 2: Probe the Main Stream
        // -------------------------------------------------------------
        await foreach (var rowBatch in _mainChannel.ReadAllAsync(ct))
        {
            foreach (var mainRow in rowBatch)
            {
                var mainKeyVal = mainRow[_mainKeyIndex]?.ToString();

                object?[]? matchedRefRow = null;

                if (!string.IsNullOrEmpty(mainKeyVal) && _refMap.TryGetValue(mainKeyVal, out var refRow))
                {
                    matchedRefRow = refRow;
                }

                if (matchedRefRow == null && _joinType.Equals("Inner", StringComparison.OrdinalIgnoreCase))
                {
                    // Inner Join miss, discard the row.
                    mainRowsDiscarded++;
                    continue;
                }

                // Expand array to merge output
                var outRow = new object?[_outputColumns.Count];
                Array.Copy(mainRow, outRow, mainRow.Length);

                // Append reference columns (if matched, else nulls for Left Join)
                int colOffset = mainRow.Length;
                foreach (var colSpec in _selectCols)
                {
                    int refIdx = _refSelectIndices[colSpec];
                    outRow[colOffset++] = matchedRefRow != null ? matchedRefRow[refIdx] : null;
                }

                buffer.Add(outRow);
                mainRowsMatched++;

                if (buffer.Count >= batchSize)
                {
                    yield return new ReadOnlyMemory<object?[]>(buffer.ToArray());
                    buffer.Clear();
                }
            }
        }

        if (buffer.Count > 0)
        {
            yield return new ReadOnlyMemory<object?[]>(buffer.ToArray());
        }

        _logger.LogInformation("Native Hash Join complete. Total keys matched: {Matched}, Discarded (Inner misses): {Discarded}", mainRowsMatched, mainRowsDiscarded);
    }

    public ValueTask DisposeAsync()
    {
        _refMap.Clear();
        return ValueTask.CompletedTask;
    }

    private static int FindColumnIndex(IReadOnlyList<PipeColumnInfo> columns, string colName, string streamAlias)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (columns[i].Name.Equals(colName, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new ArgumentException($"Column '{colName}' not found in schema of memory channel '{streamAlias}'. Available columns: {string.Join(", ", columns.Select(c => c.Name))}");
    }
}
