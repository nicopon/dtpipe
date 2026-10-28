using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.DryRun;

/// <summary>
/// Records the first N rows leaving each pipeline stage, as the real execution produces them.
/// </summary>
public sealed class SampleTapRecorder : ISampleTap
{
    /// <summary>
    /// Per-stage ceiling on what is kept in memory. It is a safety limit, not the meaning of
    /// --dry-run N: N bounds the SOURCE, through the reader's limit, and every stage then shows
    /// everything those N rows produced. Capping downstream stages at N instead would truncate
    /// a 1:N expansion back to N rows — reporting ten where the run writes thirty, which is the
    /// very class of lie this path exists to remove.
    /// </summary>
    public const int MaxQuota = 1000;

    private sealed class Stage
    {
        public required int Index;
        public required string Name;
        public required IReadOnlyList<PipeColumnInfo> Schema;
        public required bool IsColumnar;
        public readonly List<object?[]> Rows = new();
        public long TotalSeen;
    }

    private readonly Dictionary<int, Stage> _stages = new();
    private readonly int _quota;
    private readonly DynamicTypeObserver _typeObserver = new();
    private bool _wantsMore = true;

    /// <param name="quota">Per-stage ceiling. Defaults to <see cref="MaxQuota"/>; the source is
    /// bounded by the pipeline's limit, not here.</param>
    public SampleTapRecorder(int quota = MaxQuota)
        => _quota = Math.Clamp(quota, 1, MaxQuota);

    public bool WantsMore => _wantsMore;

    public void OnStageSchema(int stageIndex, string stageName, IReadOnlyList<PipeColumnInfo> schema, bool isColumnar)
    {
        if (_stages.TryGetValue(stageIndex, out var existing))
        {
            // A stage can be declared twice — the flush pass revisits the same transformers.
            existing.Schema = schema;
            return;
        }

        _stages[stageIndex] = new Stage
        {
            Index = stageIndex,
            Name = stageName,
            Schema = schema,
            IsColumnar = isColumnar
        };
        _wantsMore = true;
    }

    public void OnRow(int stageIndex, IReadOnlyList<object?> row)
    {
        if (!_stages.TryGetValue(stageIndex, out var stage)) return;

        stage.TotalSeen++;
        if (stage.Rows.Count >= _quota) { RecomputeWantsMore(); return; }

        var values = row as object?[] ?? row.ToArray();
        stage.Rows.Add(values);
        _typeObserver.ObserveRow(stage.Schema, values);
        RecomputeWantsMore();
    }

    /// <summary>
    /// Extracts what is still missing and lets the batch go: the segment runner owns it
    /// (.claude/rules/engine-invariants.md › "RecordBatch ownership"), so nothing here disposes it or keeps it.
    ///
    /// Values come out through <see cref="ArrowTypeMapper.GetValueForField"/> — the
    /// metadata-aware reader — and never through the storage-only GetValue. Reading a
    /// FixedSizeBinary(16) carrying arrow.uuid as a byte[] is exactly the type loss the old
    /// row-mode fallback caused, and reintroducing it here would put the divergence back
    /// inside the unified path.
    /// </summary>
    public void OnBatch(int stageIndex, RecordBatch batch)
    {
        if (!_stages.TryGetValue(stageIndex, out var stage)) return;

        stage.TotalSeen += batch.Length;

        var missing = _quota - stage.Rows.Count;
        if (missing <= 0) { RecomputeWantsMore(); return; }

        var take = Math.Min(missing, batch.Length);
        var fields = batch.Schema.FieldsList;
        for (var i = 0; i < take; i++)
        {
            var values = new object?[fields.Count];
            for (var c = 0; c < fields.Count; c++)
                values[c] = ArrowTypeMapper.GetValueForField(batch.Column(c), fields[c], i);

            stage.Rows.Add(values);
            _typeObserver.ObserveRow(stage.Schema, values);
        }

        RecomputeWantsMore();
    }

    /// <summary>Assembles what was observed. Call once the run has finished.</summary>
    public SampleRun Build(long rowsRead, long rowsWritten)
    {
        var stages = _stages.Values
            .OrderBy(s => s.Index)
            .Select(s => new StageCapture(s.Index, s.Name, s.Schema, s.IsColumnar, s.Rows, s.TotalSeen))
            .ToList();

        return new SampleRun(stages, rowsRead, rowsWritten);
    }

    /// <summary>Type hints observed at runtime, for the performance advisory.</summary>
    public IReadOnlyDictionary<string, string> TypeHints => _typeObserver.GenerateHints();

    private void RecomputeWantsMore()
    {
        foreach (var s in _stages.Values)
        {
            if (s.Rows.Count < _quota) { _wantsMore = true; return; }
        }
        _wantsMore = false;
    }
}
