using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Services;
using DtPipe.Transformers.Arrow.Project;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

[Collection(SessionStateCollection.Name)]
public class CheckpointStoreTests : IDisposable
{
	private readonly string _tmp;
	private readonly string? _savedState;
	private readonly SessionStore _session;
	private readonly CheckpointStore _store;

	public CheckpointStoreTests()
	{
		_tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_ck_{Guid.NewGuid():N}");
		Directory.CreateDirectory(_tmp);
		_savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));

		_session = new SessionStore(new SessionIdentity("ck-test", Path.Combine(_tmp, ".dtpipe"), SessionOrigin.Explicit));
		_store = new CheckpointStore(_session);
	}

	public void Dispose()
	{
		Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
		if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
	}

	private static readonly Schema TestSchema = new Schema.Builder()
		.Field(f => f.Name("Id").DataType(Int32Type.Default).Nullable(false))
		.Field(f => f.Name("Name").DataType(StringType.Default).Nullable(true))
		.Build();

	private static async IAsyncEnumerable<RecordBatch> Batches(int count, int rowsEach,
		Apache.Arrow.Memory.MemoryAllocator? pool = null, [EnumeratorCancellation] CancellationToken ct = default)
	{
		for (var b = 0; b < count; b++)
		{
			var ids = new Int32Array.Builder();
			var names = new StringArray.Builder();
			for (var i = 0; i < rowsEach; i++)
			{
				ids.Append(b * rowsEach + i);
				names.Append($"row-{b}-{i}");
			}
			yield return new RecordBatch(TestSchema,
				[pool is null ? ids.Build() : ids.Build(pool), pool is null ? names.Build() : names.Build(pool)],
				rowsEach);
			await Task.Yield();
		}
	}

	private static async Task<List<(int Id, string? Name)>> Drain(IAsyncEnumerable<RecordBatch> batches)
	{
		var rows = new List<(int, string?)>();
		await foreach (var batch in batches)
		{
			using (batch)
			{
				var ids = (Int32Array)batch.Column(0);
				var names = (StringArray)batch.Column(1);
				for (var i = 0; i < batch.Length; i++) rows.Add((ids.GetValue(i)!.Value, names.GetString(i)));
			}
		}
		return rows;
	}

	[Fact]
	public async Task Batches_Round_Trip_Through_The_Store()
	{
		var written = await _store.WriteAsync("key1", Batches(3, 4));
		written.Should().Be(12);

		var rows = await Drain(_store.ReadAsync("key1"));

		rows.Should().HaveCount(12);
		rows[0].Should().Be((0, "row-0-0"));
		rows[^1].Should().Be((11, "row-2-3"));
	}

	[Fact]
	public async Task What_Lands_On_Disk_Is_Not_Readable_Arrow()
	{
		await _store.WriteAsync("key1", Batches(1, 4));

		var bytes = await File.ReadAllBytesAsync(_store.PathFor("key1"));

		System.Text.Encoding.ASCII.GetString(bytes).Should().NotContain("row-0-0",
			"a copied project directory must carry inert bytes, not values");
		System.Text.Encoding.ASCII.GetString(bytes[..8]).Should().Be("DTPCKPT1");
	}

	[Fact]
	public async Task Destroying_The_Key_Makes_The_Checkpoint_Unreadable()
	{
		await _store.WriteAsync("key1", Batches(1, 4));
		SessionKeyStore.DeleteKey(_session.Identity.Name);

		var act = async () => await Drain(_store.ReadAsync("key1"));

		await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*key*",
			"crypto-shredding is the property that makes a purge reliable rather than best-effort");
		File.Exists(_store.PathFor("key1")).Should().BeTrue("and it holds even though the bytes are still there");
	}

	[Fact]
	public async Task A_Foreign_Key_Yields_Nothing()
	{
		await _store.WriteAsync("key1", Batches(1, 4));
		await File.WriteAllBytesAsync(SessionKeyStore.KeyPath(_session.Identity.Name), RandomNumberGenerator.GetBytes(32));

		var act = async () => await Drain(_store.ReadAsync("key1"));

		await act.Should().ThrowAsync<CryptographicException>();
	}

	/// <summary>
	/// A batch the store hands back survives the contract every other batch obeys: a transformer
	/// retains a column, the segment runner disposes the input, and the retained column still reads.
	/// </summary>
	/// <remarks>
	/// What the store reads back is Arrow IPC, whose batches carry no shared handle on their column
	/// buffers, so <c>ArrowOwnership.RetainArray</c> bumps nothing and the dispose of the input frees
	/// the body under an output that aliases it. The symptom was a <c>NullReferenceException</c>
	/// thrown from inside <c>StringArray.GetString</c> rather than a leak, so there is no
	/// <c>TrackingMemoryPool</c> signal here and the assertion is on the values.
	/// </remarks>
	[Fact]
	public async Task A_Resumed_Batch_Survives_An_Aliasing_Transformer()
	{
		await _store.WriteAsync("key1", Batches(2, 8));

		// Project with a rename returns a new batch aliasing every input column.
		var project = new ProjectDataTransformer(new ProjectOptions { Rename = ["Id:Ident"] });
		await project.InitializeAsync([new PipeColumnInfo("Id", typeof(int), false),
		                               new PipeColumnInfo("Name", typeof(string), true)]);

		var executor = new PipelineExecutor(
			Enumerable.Empty<IRowToColumnarBridgeFactory>(),
			Enumerable.Empty<IColumnarToRowBridgeFactory>(),
			Microsoft.Extensions.Logging.Abstractions.NullLogger<PipelineExecutor>.Instance);

		var seen = 0;
		await foreach (var batch in executor.ApplyColumnarSegmentAsync(
			_store.ReadAsync("key1"), [project], Moq.Mock.Of<IExportProgress>(), default))
		{
			using (batch)
			{
				batch.Schema.FieldsList[0].Name.Should().Be("Ident");
				var ids = (Int32Array)batch.Column(0);
				var names = (StringArray)batch.Column(1);
				for (var i = 0; i < batch.Length; i++, seen++)
				{
					ids.GetValue(i).Should().Be(seen);
					names.GetString(i).Should().Be($"row-{seen / 8}-{seen % 8}");
				}
			}
		}

		seen.Should().Be(16, "every row crossed the segment intact");
	}

	[Fact]
	public async Task Writing_Disposes_Every_Batch_It_Consumes()
	{
		var pool = new TrackingMemoryPool();

		await _store.WriteAsync("key1", Batches(4, 16, pool));

		pool.TotalAllocations.Should().BeGreaterThan(0);
		pool.ActiveAllocations.Should().Be(0,
			"the store is a terminal consumer; Arrow buffers are off-heap and the GC never reports them");
	}

	[Fact]
	public async Task An_Interrupted_Write_Leaves_No_Checkpoint_Behind()
	{
		async IAsyncEnumerable<RecordBatch> Failing([EnumeratorCancellation] CancellationToken ct = default)
		{
			await foreach (var b in Batches(1, 4, ct: ct)) yield return b;
			throw new InvalidOperationException("source failed");
		}

		var act = async () => await _store.WriteAsync("key1", Failing());

		await act.Should().ThrowAsync<InvalidOperationException>();
		_store.Contains("key1").Should().BeFalse(
			"a half-written file that reads as a complete checkpoint is worse than no checkpoint");
	}

	[Fact]
	public async Task Listing_Reports_Only_Complete_Checkpoints()
	{
		await _store.WriteAsync("aaa", Batches(1, 2));
		await _store.WriteAsync("bbb", Batches(1, 2));
		Directory.CreateDirectory(_session.CheckpointPath("ccc"));

		_store.List().Should().Equal("aaa", "bbb");
	}

	[Fact]
	public async Task Reading_An_Unknown_Checkpoint_Says_So()
	{
		var act = async () => await Drain(_store.ReadAsync("nope"));

		await act.Should().ThrowAsync<FileNotFoundException>();
	}

	[Fact]
	public async Task The_Schema_Survives_The_Round_Trip()
	{
		await _store.WriteAsync("key1", Batches(1, 2));

		var schema = await _store.ReadSchemaAsync("key1");

		schema!.FieldsList.Select(f => f.Name).Should().Equal("Id", "Name");
		schema.GetFieldByName("Name").IsNullable.Should().BeTrue();
	}
}
