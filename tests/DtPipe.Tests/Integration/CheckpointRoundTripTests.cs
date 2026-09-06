using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Sessions;
using DtPipe.Services;
using DtPipe.Tests.Helpers;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Integration;

/// <summary>
/// A checkpoint has to be able to stand in for the source it was taken from. These drive the
/// real executor: what a run writes with a checkpoint must equal what it writes without one,
/// and resuming from the checkpoint must replay the same rows.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class CheckpointRoundTripTests : IDisposable
{
	private readonly string _tmp;
	private readonly string? _savedState;
	private readonly SessionStore _session;
	private readonly CheckpointStore _store;

	public CheckpointRoundTripTests()
	{
		_tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_rt_{Guid.NewGuid():N}");
		Directory.CreateDirectory(_tmp);
		_savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));

		_session = new SessionStore(new SessionIdentity("rt", Path.Combine(_tmp, ".dtpipe"), SessionOrigin.Explicit));
		_store = new CheckpointStore(_session);
	}

	public void Dispose()
	{
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
		if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
	}

	private static readonly PipelineExecutor Executor = new(
		[new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(
			NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
		[new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
		NullLogger<PipelineExecutor>.Instance);

	[Fact]
	public async Task A_Tee_Does_Not_Change_What_The_Writer_Receives()
	{
		var without = await RunAsync(checkpointKey: null);
		var with = await RunAsync(checkpointKey: "k1");

		with.Should().Equal(without, "materialising is observation, not transformation");
	}

	[Fact]
	public async Task Resuming_From_A_Checkpoint_Replays_The_Same_Rows()
	{
		var original = await RunAsync(checkpointKey: "k1");

		var resumed = new List<int>();
		var reader = new CheckpointStreamReader(_store, "k1");
		await reader.OpenAsync();
		await foreach (var batch in reader.ReadRecordBatchesAsync())
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				for (var i = 0; i < batch.Length; i++) resumed.Add(ids.GetValue(i)!.Value);
			}
		}

		resumed.Should().Equal(original);
	}

	[Fact]
	public async Task A_Checkpoint_Holds_What_The_Writer_Would_Have_Received()
	{
		// Not the source rows: the transformer runs before the materialisation point, so
		// resuming skips work already done rather than redoing it.
		await RunAsync(checkpointKey: "k1", transform: true);

		var rows = new List<int>();
		await foreach (var batch in _store.ReadAsync("k1"))
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				for (var i = 0; i < batch.Length; i++) rows.Add(ids.GetValue(i)!.Value);
			}
		}

		rows.Should().Equal(Enumerable.Range(0, 8).Select(i => i + 1000));
	}

	[Fact]
	public async Task Teeing_Leaks_No_Native_Memory()
	{
		var pool = new TrackingMemoryPool();

		await RunAsync(checkpointKey: "k1", pool: pool);

		pool.TotalAllocations.Should().BeGreaterThan(0);
		pool.ActiveAllocations.Should().Be(0,
			"the tee retains a reference per extra consumer and each disposes its own — a refcount bump, never a deep copy");
	}

	[Fact]
	public async Task An_Interrupted_Stream_Publishes_No_Checkpoint()
	{
		var act = async () => await RunAsync(checkpointKey: "k1", failAfter: 1);

		await act.Should().ThrowAsync<InvalidOperationException>();
		_store.Contains("k1").Should().BeFalse(
			"a truncated checkpoint that looked complete would be replayed as if it were the whole source");
	}

	/// <summary>
	/// Deterministic sampling already existed but was neither materialisable nor
	/// addressable. Materialising it is the promise; that a seeded sample
	/// comes back as the SAME rows is what makes the promise worth anything, and what lets an
	/// agent iterate against a fixed sample instead of a moving one.
	/// </summary>
	[Fact]
	public async Task A_Seeded_Sample_Materialises_And_Replays_The_Same_Rows()
	{
		var written = await RunAsync(checkpointKey: "k1", samplingRate: 0.5, samplingSeed: 42);

		written.Should().NotBeEmpty("a rate of 0.5 over 8 rows must keep some");
		written.Count.Should().BeLessThan(8, "and drop some — otherwise the test proves nothing about sampling");

		var replayed = new List<int>();
		await foreach (var batch in _store.ReadAsync("k1"))
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				for (var i = 0; i < batch.Length; i++) replayed.Add(ids.GetValue(i)!.Value);
			}
		}

		replayed.Should().Equal(written);
	}

	[Fact]
	public async Task The_Same_Seed_Selects_The_Same_Rows_Twice()
	{
		var first = await RunAsync(checkpointKey: null, samplingRate: 0.5, samplingSeed: 42);
		var second = await RunAsync(checkpointKey: null, samplingRate: 0.5, samplingSeed: 42);

		second.Should().Equal(first);
	}

	[Fact]
	public async Task A_Different_Seed_Selects_Different_Rows()
	{
		var a = await RunAsync(checkpointKey: null, samplingRate: 0.5, samplingSeed: 42);
		var b = await RunAsync(checkpointKey: null, samplingRate: 0.5, samplingSeed: 1234);

		b.Should().NotEqual(a);
	}

	/// <summary>
	/// The sampling parameters are part of what produces the rows, so they are part of the key.
	/// Without this, two samples of the same query would overwrite each other and an agent would
	/// silently read someone else's draw.
	/// </summary>
	[Fact]
	public void The_Sampling_Seed_Is_Part_Of_The_Checkpoint_Identity()
	{
		string Key(int? seed) => DtPipe.Sessions.CheckpointKey.Compute(
			"csv:in.csv", "SELECT *", null, 0.5, seed, 0, 1024, 0, 0);

		Key(42).Should().NotBe(Key(1234));
		Key(42).Should().Be(Key(42));
	}

	/// <summary>
	/// A pipeline with no Arrow anywhere — row reader, row writer, no columnar transformer — is
	/// the commonest shape there is (CSV to CSV), and refusing to materialise it was not
	/// defensible. The engine appends an empty columnar segment so the chain reaches Arrow at
	/// the writer boundary, tees there and bridges back.
	/// </summary>
	[Fact]
	public async Task A_Row_Mode_Pipeline_Can_Still_Be_Materialised()
	{
		var written = await RunAsync(checkpointKey: "k1", rowWriter: true);

		written.Should().Equal(Enumerable.Range(0, 8));

		var replayed = new List<int>();
		await foreach (var batch in _store.ReadAsync("k1"))
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				for (var i = 0; i < batch.Length; i++) replayed.Add(ids.GetValue(i)!.Value);
			}
		}

		replayed.Should().Equal(written, "what was materialised is what the writer received");
	}

	[Fact]
	public async Task The_Bridge_Is_Added_Only_When_Materialising()
	{
		var withCheckpoint = await RunAsync(checkpointKey: "k1", rowWriter: true);
		var without = await RunAsync(checkpointKey: null, rowWriter: true);

		without.Should().Equal(withCheckpoint,
			"the round-trip must not change the rows, and without --checkpoint it does not happen at all");
	}

	[Fact]
	public void Resuming_From_A_Missing_Checkpoint_Names_What_Is_Available()
	{
		var factory = new CheckpointReaderFactory("nope", "rt");

		var act = () => factory.Create(new DtPipe.Core.Options.OptionsRegistry());

		act.Should().Throw<InvalidOperationException>().WithMessage("*no checkpoints*");
	}

	[Fact]
	public void A_Checkpoint_Key_Is_Never_Claimed_By_Connection_String_Matching()
		=> new CheckpointReaderFactory("abc", null).CanHandle("abc").Should().BeFalse(
			"a hex key must never enter the component-prefix grammar");

	// ─────────────────────────────────────────────────────────────────────────

	private async Task<List<int>> RunAsync(
		string? checkpointKey, bool transform = false, TrackingMemoryPool? pool = null, int failAfter = -1,
		double samplingRate = 1.0, int? samplingSeed = null, bool rowWriter = false)
	{
		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		IStreamReader reader = rowWriter ? new RowOnlyReader(8) : new BatchReader(8, pool, failAfter);
		await reader.OpenAsync();

		var pipeline = transform ? new List<IDataTransformer> { new Offset(1000) } : [];
		var schema = columns;
		foreach (var t in pipeline) schema = (List<PipeColumnInfo>)await t.InitializeAsync(schema);

		var segments = DtPipe.Core.Pipelines.PipelineSegmenter.GetSegments(pipeline);
		foreach (var s in segments) { s.InputSchema = columns; s.OutputSchema = schema; }

		var columnarWriter = new CollectingColumnarWriter();
		var rowSink = new CollectingRowWriter();
		IDataWriter writer = rowWriter ? rowSink : columnarWriter;

		Func<IAsyncEnumerable<RecordBatch>, IAsyncEnumerable<RecordBatch>>? materialise =
			checkpointKey is null ? null : src => CheckpointTee.TeeAsync(src, _store, checkpointKey);

		using var cts = new CancellationTokenSource();
		await Executor.ExecuteSegmentedPipelineAsync(
			reader, writer, segments, schema,
			new PipelineOptions { BatchSize = 3, SamplingRate = samplingRate, SamplingSeed = samplingSeed },
			Mock.Of<IExportProgress>(), cts, cts.Token, null, materialise);

		return rowWriter ? rowSink.Ids : columnarWriter.Ids;
	}

	/// <summary>A source with no Arrow side at all — the CSV-to-CSV shape.</summary>
	private sealed class RowOnlyReader : IStreamReader
	{
		private readonly int _n;
		public RowOnlyReader(int n) => _n = n;
		public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;
		public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
		{
			var rows = Enumerable.Range(0, _n).Select(i => new object?[] { i }).ToArray();
			yield return rows.AsMemory();
			await Task.CompletedTask;
		}
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class CollectingRowWriter : IRowDataWriter
	{
		public List<int> Ids { get; } = new();
		public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> c, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
		{
			foreach (var r in rows) Ids.Add(Convert.ToInt32(r[0]));
			return ValueTask.CompletedTask;
		}
		public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class Offset : BaseColumnarTransformer
	{
		private readonly int _by;
		public Offset(int by) => _by = by;
		public override bool CanProcessColumnar => true;
		protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
		{
			var src = (Int32Array)batch.Column(0);
			var b = new Int32Array.Builder();
			for (var i = 0; i < batch.Length; i++) b.Append(src.GetValue(i)!.Value + _by);
			return ValueTask.FromResult<RecordBatch?>(new RecordBatch(batch.Schema, new IArrowArray[] { b.Build() }, batch.Length));
		}
	}

	private sealed class BatchReader : IColumnarStreamReader
	{
		private readonly int _n;
		private readonly TrackingMemoryPool? _pool;
		private readonly int _failAfter;
		private readonly Schema _schema = new Schema.Builder().Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false)).Build();

		public BatchReader(int n, TrackingMemoryPool? pool, int failAfter) { _n = n; _pool = pool; _failAfter = failAfter; }

		public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
		public Schema? Schema => _schema;
		public Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

		public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
		{
			var emitted = 0;
			for (var i = 0; i < _n; i += 3)
			{
				if (_failAfter >= 0 && emitted >= _failAfter) throw new InvalidOperationException("source failed");
				var count = Math.Min(3, _n - i);
				var b = new Int32Array.Builder();
				for (var j = 0; j < count; j++) b.Append(i + j);
				yield return new RecordBatch(_schema, [_pool is null ? b.Build() : b.Build(_pool)], count);
				emitted++;
				await Task.Yield();
			}
		}

		public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
		{
			await foreach (var batch in ReadRecordBatchesAsync(ct))
				using (batch)
					foreach (var m in DtPipe.Core.Infrastructure.Arrow.ArrowRowConverter.FlattenBatch(batch, batchSize))
						yield return m;
		}

		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}

	private sealed class CollectingColumnarWriter : IColumnarDataWriter
	{
		public List<int> Ids { get; } = new();
		public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> c, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				for (var i = 0; i < batch.Length; i++) Ids.Add(ids.GetValue(i)!.Value);
			}
			return ValueTask.CompletedTask;
		}
		public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
		public ValueTask DisposeAsync() => ValueTask.CompletedTask;
	}
}
