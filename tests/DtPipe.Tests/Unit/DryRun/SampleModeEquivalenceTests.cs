using System.Runtime.CompilerServices;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using DtPipe.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// The architectural acceptance criterion, as an executable property:
///
///     what a sample run REPORTS is what a real run WRITES.
///
/// It was written red, against the second engine that used to answer for the dry-run, and
/// turned green by deleting that engine rather than by touching the assertion. Routing around
/// the old analyser instead of removing it would have left it red — which is what makes "the
/// dry-run goes through the real execution path" a verified fact and not a promise.
///
/// The transformers here are minimal doubles rather than Expand/Window themselves: the
/// property under test is the ENGINE's row semantics (1:N, N:1, end-of-stream flush), not
/// the JS expression engine those two carry. Their own suites cover the expressions; the
/// doubles reproduce the interface contracts exactly, including the two Transform(row)
/// implementations that make the current divergence what it is.
/// </summary>
public class SampleModeEquivalenceTests
{
	private const int SampleSize = 10;

	// ─────────────────────────────────────────────────────────────────────────
	// The property
	// ─────────────────────────────────────────────────────────────────────────

	public static TheoryData<string> Pipelines() => new()
	{
		"passthrough",   // 1:1 — green today and after; anti-regression
		"expand",        // 1:N — ExpandDataTransformer.cs:77-81 returns only the first of N
		"window",        // N:1 — WindowDataTransformer.cs:67-76 returns null, and Flush() is never called
	};

	[Theory]
	[MemberData(nameof(Pipelines))]
	public async Task SampleTrace_FinalStage_Equals_RealRun_Output(string pipeline)
	{
		var written = await RunRealPipelineAsync(Build(pipeline), SampleSize);
		var reported = await RunSampleModeAsync(Build(pipeline), SampleSize);

		Assert.Equal(Render(written), Render(reported));
	}

	/// <summary>
	/// End-of-stream rows are part of a pipeline's output, so they are part of what a sample
	/// must report. Split out from the theory because it names one contract — IDataTransformer.Flush.
	/// </summary>
	[Fact]
	public async Task Flushed_Rows_Are_Reported_By_Sample_Mode()
	{
		var reported = await RunSampleModeAsync(Build("window"), SampleSize);
		Assert.NotEmpty(reported);
	}

	// ─────────────────────────────────────────────────────────────────────────
	// Harness — the two sides of the property
	// ─────────────────────────────────────────────────────────────────────────

	private static readonly PipelineExecutor Executor = new(
		[new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
		[new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
		NullLogger<PipelineExecutor>.Instance);

	/// <summary>The real path: what the writer actually receives, bounded to N input rows.</summary>
	private static async Task<List<object?[]>> RunRealPipelineAsync(List<IDataTransformer> pipeline, int limit)
	{
		await using var reader = new SequenceReader(limit * 4);
		await reader.OpenAsync(CancellationToken.None);

		var schema = reader.Columns!;
		foreach (var t in pipeline) schema = await t.InitializeAsync(schema, CancellationToken.None);

		var writer = new CapturingWriter();
		var progress = Mock.Of<IExportProgress>();
		var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(pipeline);
		foreach (var s in segments)
		{
			s.InputSchema = reader.Columns!;
			s.OutputSchema = schema;
		}

		var options = new PipelineOptions { BatchSize = 4, Limit = limit };
		using var cts = new CancellationTokenSource();
		await Executor.ExecuteSegmentedPipelineAsync(
			reader, writer, segments, schema, options, progress, cts, cts.Token);

		return writer.Rows;
	}

	/// <summary>
	/// The sample path — the unified one: the real engine, the real sink, the capture the tap
	/// made on the way past. The caller's assertion did not change when this body did, which is
	/// what the property is for.
	/// </summary>
	private static async Task<List<object?[]>> RunSampleModeAsync(List<IDataTransformer> pipeline, int sampleCount)
	{
		await using var reader = new SequenceReader(sampleCount * 4);
		var report = await DtPipe.Tests.Helpers.SampleRunHarness.AnalyzeAsync(reader, pipeline, sampleCount);
		return report.Run.FinalRows().ToList();
	}

	private static string Render(List<object?[]> rows)
		=> string.Join("\n", rows.Select(r => string.Join("|", r.Select(v => v?.ToString() ?? "<null>"))));

	private static List<IDataTransformer> Build(string pipeline) => pipeline switch
	{
		"passthrough" => new List<IDataTransformer> { new PassThroughTransformer() },
		"expand"      => new List<IDataTransformer> { new ExpandingTransformer() },
		"window"      => new List<IDataTransformer> { new WindowingTransformer() },
		_             => throw new ArgumentOutOfRangeException(nameof(pipeline), pipeline, "Unknown pipeline"),
	};

	// ─────────────────────────────────────────────────────────────────────────
	// Doubles
	// ─────────────────────────────────────────────────────────────────────────

	private sealed class SequenceReader : IStreamReader
	{
		private readonly int _count;
		public SequenceReader(int count) => _count = count;
		public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

		public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
			int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
		{
			for (var i = 0; i < _count; i += batchSize)
			{
				var n = Math.Min(batchSize, _count - i);
				var rows = new object?[n][];
				for (var j = 0; j < n; j++) rows[j] = new object?[] { i + j };
				yield return rows.AsMemory();
				await Task.Yield();
			}
		}

		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class CapturingWriter : IRowDataWriter
	{
		public List<object?[]> Rows { get; } = new();
		public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
		{
			Rows.AddRange(rows);
			return ValueTask.CompletedTask;
		}
		public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class PassThroughTransformer : IDataTransformer
	{
		public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
			=> ValueTask.FromResult(columns);
		public object?[]? Transform(IReadOnlyList<object?> row) => row as object?[] ?? row.ToArray();
	}

	/// <summary>1:N. Transform(row) mirrors ExpandDataTransformer.cs:77-81 — the first of N.</summary>
	private sealed class ExpandingTransformer : IMultiRowTransformer
	{
		private const int Factor = 3;
		public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
			=> ValueTask.FromResult(columns);

		public IEnumerable<object?[]> TransformMany(IReadOnlyList<object?> row)
		{
			for (var i = 0; i < Factor; i++) yield return new object?[] { $"{row[0]}.{i}" };
		}

		public object?[]? Transform(IReadOnlyList<object?> row) => TransformMany(row).FirstOrDefault();
	}

	/// <summary>N:1 with an end-of-stream flush. Transform(row) mirrors WindowDataTransformer.cs:67-76 — null.</summary>
	private sealed class WindowingTransformer : IMultiRowTransformer
	{
		private const int Window = 4;
		private readonly List<object?> _buffer = new();

		public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
			=> ValueTask.FromResult(columns);

		public IEnumerable<object?[]> TransformMany(IReadOnlyList<object?> row)
		{
			_buffer.Add(row[0]);
			if (_buffer.Count < Window) yield break;
			yield return Aggregate();
		}

		public IEnumerable<object?[]> Flush()
		{
			if (_buffer.Count > 0) yield return Aggregate();
		}

		public object?[]? Transform(IReadOnlyList<object?> row) => null;

		private object?[] Aggregate()
		{
			var sum = _buffer.Sum(v => Convert.ToInt32(v));
			_buffer.Clear();
			return new object?[] { sum };
		}
	}
}
