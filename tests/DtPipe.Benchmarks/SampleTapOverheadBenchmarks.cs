using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Benchmarks;

/// <summary>
/// The price of the observation seam that unifies the dry-run with the real path.
/// The tap is offered every row and every batch, so "a null tap costs
/// nothing" is a claim that has to be a number.
///
/// <c>*_Ordinary</c> is a run with no tap at all — what every non-sampling run does, and the
/// number that must not move. <c>*_SampleSated</c> and <c>*_SampleRecording</c> are sample
/// mode: a tap is present and the stream carries an extra iterator layer. Their cost is
/// bounded by --dry-run N and paid only when sampling was asked for, so it buys the
/// unification; the ordinary column is the one that would forbid it.
/// </summary>
[MemoryDiagnoser]
public class SampleTapOverheadBenchmarks
{
	private const int Rows = 100_000;
	private const int BatchSize = 4_096;

	private static readonly IExportProgress Progress = new DtPipe.Feedback.NullExportProgress();

	private static readonly PipelineExecutor Executor = new(
		Enumerable.Empty<IRowToColumnarBridgeFactory>(),
		Enumerable.Empty<IColumnarToRowBridgeFactory>(),
		NullLogger<PipelineExecutor>.Instance);

	private List<IDataTransformer> _rowChain = null!;
	private List<IDataTransformer> _columnarChain = null!;
	private IReadOnlyList<PipeColumnInfo> _columns = null!;

	/// <summary>A tap that is present and never satisfied — the worst case for the offer.</summary>
	private sealed class HungryTap : ISampleTap
	{
		public bool WantsMore => true;
		public long Rows;
		public void OnStageSchema(int stageIndex, string stageName, IReadOnlyList<PipeColumnInfo> schema, bool isColumnar) { }
		public void OnRow(int stageIndex, IReadOnlyList<object?> row) => Rows++;
		public void OnBatch(int stageIndex, RecordBatch batch) => Rows += batch.Length;
	}

	/// <summary>A tap that has all it wants — the case an almost-finished sample run is in.</summary>
	private sealed class SatedTap : ISampleTap
	{
		public bool WantsMore => false;
		public void OnStageSchema(int stageIndex, string stageName, IReadOnlyList<PipeColumnInfo> schema, bool isColumnar) { }
		public void OnRow(int stageIndex, IReadOnlyList<object?> row) => throw new InvalidOperationException("offered while sated");
		public void OnBatch(int stageIndex, RecordBatch batch) => throw new InvalidOperationException("offered while sated");
	}

	[GlobalSetup]
	public void Setup()
	{
		_columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		_rowChain = [new Increment(), new Increment(), new Increment()];
		_columnarChain = [new ColumnarIncrement(), new ColumnarIncrement(), new ColumnarIncrement()];
	}

	// ── row chain ────────────────────────────────────────────────────────────

	[Benchmark(Baseline = true)] public Task<long> RowChain_Ordinary()   => RunRowAsync(null);
	[Benchmark]                  public Task<long> RowChain_SampleSated() => RunRowAsync(new SatedTap());
	[Benchmark]                  public Task<long> RowChain_SampleRecording() => RunRowAsync(new HungryTap());

	// ── columnar chain ───────────────────────────────────────────────────────

	[Benchmark] public Task<long> ColumnarChain_Ordinary()     => RunColumnarAsync(null);
	[Benchmark] public Task<long> ColumnarChain_SampleSated()   => RunColumnarAsync(new SatedTap());
	[Benchmark] public Task<long> ColumnarChain_SampleRecording() => RunColumnarAsync(new HungryTap());

	private async Task<long> RunRowAsync(ISampleTap? tap)
	{
		var writer = new NullRowWriter();
		var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(_rowChain);
		foreach (var s in segments) { s.InputSchema = _columns; s.OutputSchema = _columns; }

		using var cts = new CancellationTokenSource();
		await Executor.ExecuteSegmentedPipelineAsync(
			new IntReader(Rows), writer, segments, _columns,
			new PipelineOptions { BatchSize = BatchSize },
			Progress, cts, cts.Token, tap);
		return writer.Rows;
	}

	private async Task<long> RunColumnarAsync(ISampleTap? tap)
	{
		var writer = new NullColumnarWriter();
		var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(_columnarChain);
		foreach (var s in segments) { s.InputSchema = _columns; s.OutputSchema = _columns; }

		using var cts = new CancellationTokenSource();
		await Executor.ExecuteSegmentedPipelineAsync(
			new IntReader(Rows), writer, segments, _columns,
			new PipelineOptions { BatchSize = BatchSize },
			Progress, cts, cts.Token, tap);
		return writer.Rows;
	}

	// ── fixtures ─────────────────────────────────────────────────────────────

	private sealed class Increment : IDataTransformer
	{
		public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
			=> ValueTask.FromResult(columns);
		public object?[]? Transform(IReadOnlyList<object?> row) => [(int)row[0]! + 1];
	}

	private sealed class ColumnarIncrement : BaseColumnarTransformer
	{
		public override bool CanProcessColumnar => true;
		protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
		{
			var src = (Int32Array)batch.Column(0);
			var b = new Int32Array.Builder();
			for (var i = 0; i < batch.Length; i++) b.Append(src.GetValue(i)!.Value + 1);
			return ValueTask.FromResult<RecordBatch?>(new RecordBatch(batch.Schema, new IArrowArray[] { b.Build() }, batch.Length));
		}
	}

	private sealed class IntReader : IColumnarStreamReader
	{
		private readonly int _count;
		private readonly Schema _schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build();
		public IntReader(int count) => _count = count;
		public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		public Schema? Schema => _schema;
		public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

		public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
		{
			for (var i = 0; i < _count; i += BatchSize)
			{
				var n = Math.Min(BatchSize, _count - i);
				var b = new Int32Array.Builder();
				for (var j = 0; j < n; j++) b.Append(i + j);
				yield return new RecordBatch(_schema, new IArrowArray[] { b.Build() }, n);
			}
			await Task.CompletedTask;
		}

		public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
		{
			for (var i = 0; i < _count; i += batchSize)
			{
				var n = Math.Min(batchSize, _count - i);
				var rows = new object?[n][];
				for (var j = 0; j < n; j++) rows[j] = [i + j];
				yield return rows.AsMemory();
			}
			await Task.CompletedTask;
		}

		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class NullRowWriter : IRowDataWriter
	{
		public long Rows;
		public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) { Rows += rows.Count; return ValueTask.CompletedTask; }
		public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class NullColumnarWriter : IColumnarDataWriter
	{
		public long Rows;
		public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default) { Rows += batch.Length; batch.Dispose(); return ValueTask.CompletedTask; }
		public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}
}
