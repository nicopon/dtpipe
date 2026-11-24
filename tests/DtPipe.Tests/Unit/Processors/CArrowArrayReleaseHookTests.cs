using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Processors.DuckDB;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// The release hook the DuckDB table function chains onto every column it exports, played without DuckDB: a
/// batch exported by <see cref="CArrowArrayExporter"/>, released the way a consumer releases it. What this does
/// not prove: that DuckDB calls the release once per array it took, and only once, and when it does.
/// <c>DuckDBSqlProcessorTests</c> runs the hook under DuckDB without counting.
/// </summary>
public unsafe class CArrowArrayReleaseHookTests
{
    private static readonly FieldInfo ReleaseField =
        typeof(CArrowArray).GetField("release", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;

    /// <summary>
    /// The hook reaches Apache.Arrow's internal <c>release</c> field through the address the C Data Interface
    /// gives it: immediately before <c>private_data</c>. A version of Apache.Arrow that moves the field fails here,
    /// not in the hook.
    /// </summary>
    [Fact]
    public void The_Release_Callback_Sits_Just_Before_Private_Data_As_The_C_Data_Interface_Lays_It_Out()
    {
        ReleaseField.Should().NotBeNull("the hook rewrites the field Apache.Arrow names release");
        ReleaseField.FieldType.IsUnmanagedFunctionPointer.Should().BeTrue();
        typeof(CArrowArray).IsLayoutSequential.Should().BeTrue("the managed struct is the C struct only with its fields in order");

        var releaseOffset = (int)Marshal.OffsetOf<CArrowArray>("release");
        var privateDataOffset = (int)Marshal.OffsetOf<CArrowArray>(nameof(CArrowArray.private_data));
        var size = Marshal.SizeOf<CArrowArray>();

        privateDataOffset.Should().Be(releaseOffset + IntPtr.Size, "struct ArrowArray puts release immediately before private_data");
        size.Should().Be(5 * sizeof(long) + 5 * IntPtr.Size, "struct ArrowArray is five int64 then five pointers");
        (privateDataOffset + IntPtr.Size).Should().Be(size, "private_data is its last field");
        sizeof(CArrowArray).Should().Be(size);

        CArrowArray array = default;
        var slotOffset = (int)((byte*)CArrowArrayReleaseHook.ReleaseSlot(&array) - (byte*)&array);
        slotOffset.Should().Be(releaseOffset, "the slot the hook reads and writes is that field");
    }

    [Fact]
    public void The_Slot_Holds_The_Release_The_Exporter_Writes_And_Clears()
    {
        using var batch = ColumnBatch(new TrackingMemoryPool(), 4);
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(batch, &array);

        var written = ReadRelease(&array);
        written.Should().NotBe(IntPtr.Zero);
        (*CArrowArrayReleaseHook.ReleaseSlot(&array)).Should().Be(written);

        CArrowArrayReleaseHook.ReleaseExportedIfUnclaimed(&array);

        ReadRelease(&array).Should().Be(IntPtr.Zero, "a release marks its array released");
    }

    /// <summary>
    /// The consumer's release runs the exporter's release, which marks the array released and drops the export's
    /// hold on the buffers, and disposes the owner, which drops the batch's: nothing is left allocated. Run twice,
    /// Apache.Arrow's release throws, and inside the hook's unmanaged callback that ends the process: a run that
    /// gets here ran it once.
    /// </summary>
    [Fact]
    public void A_Consumer_Release_Runs_The_Exporters_Release_Once_And_Disposes_The_Owner_Once()
    {
        var pool = new TrackingMemoryPool();
        var owner = new CountingOwner(ColumnBatch(pool, 8));
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(owner.Batch, &array);

        CArrowArrayReleaseHook.Chain(&array, owner);

        owner.Disposals.Should().Be(0, "the consumer holds the array");
        pool.ActiveAllocations.Should().BeGreaterThan(0);

        ReleaseAsAConsumerDoes(&array);

        owner.Disposals.Should().Be(1);
        ReadRelease(&array).Should().Be(IntPtr.Zero, "the exporter's release marks the array released");
        pool.ActiveAllocations.Should().Be(0, "neither the export nor the batch holds a buffer");

        CArrowArrayReleaseHook.ReleaseExportedIfUnclaimed(&array);

        owner.Disposals.Should().Be(1, "a released array releases nothing again");
    }

    /// <summary>
    /// What <c>duckdb_data_chunk_from_arrow</c> leaves when it fails before taking the array: the error path
    /// releases it, the owner with it, and a second pass finds nothing to release.
    /// </summary>
    [Fact]
    public void An_Export_Nobody_Took_Is_Released_On_The_Error_Path_And_Only_Once()
    {
        var pool = new TrackingMemoryPool();
        var owner = new CountingOwner(ColumnBatch(pool, 8));
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(owner.Batch, &array);
        CArrowArrayReleaseHook.Chain(&array, owner);

        CArrowArrayReleaseHook.ReleaseExportedIfUnclaimed(&array);

        owner.Disposals.Should().Be(1);
        ReadRelease(&array).Should().Be(IntPtr.Zero);
        pool.ActiveAllocations.Should().Be(0);

        CArrowArrayReleaseHook.ReleaseExportedIfUnclaimed(&array);

        owner.Disposals.Should().Be(1);
    }

    /// <summary>
    /// A consumer takes an array by copying the struct and clearing the release of the original: the error path
    /// leaves it to that consumer, whose release of its copy finds the owner by <c>private_data</c>.
    /// </summary>
    [Fact]
    public void An_Export_A_Consumer_Took_Is_Left_To_The_Consumer_On_The_Error_Path()
    {
        var pool = new TrackingMemoryPool();
        var owner = new CountingOwner(ColumnBatch(pool, 8));
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(owner.Batch, &array);
        CArrowArrayReleaseHook.Chain(&array, owner);

        var taken = array;
        *CArrowArrayReleaseHook.ReleaseSlot(&array) = IntPtr.Zero;

        CArrowArrayReleaseHook.ReleaseExportedIfUnclaimed(&array);

        owner.Disposals.Should().Be(0, "the consumer holds the array");
        pool.ActiveAllocations.Should().BeGreaterThan(0);

        ReleaseAsAConsumerDoes(&taken);

        owner.Disposals.Should().Be(1);
        pool.ActiveAllocations.Should().Be(0);
    }

    /// <summary>
    /// The hook chains to the one release it captured, so an array that carries another is refused: released by
    /// its own release, once, its owner disposed, once. The refusal leaves the captured release as it was.
    /// </summary>
    [Fact]
    public void An_Array_Carrying_Another_Release_Is_Released_Disposed_And_Refused()
    {
        // The hook captures its release from the first array it sees in the process; chaining one here makes
        // that the exporter's, whatever ran before.
        ChainAndRelease();

        var pool = new TrackingMemoryPool();
        var owner = new CountingOwner(ColumnBatch(pool, 8));
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(owner.Batch, &array);
        _foreignForwardsTo = ReadRelease(&array);
        _foreignCalls = 0;
        *CArrowArrayReleaseHook.ReleaseSlot(&array) = (IntPtr)(delegate* unmanaged[Cdecl]<CArrowArray*, void>)&ForeignRelease;

        InvalidOperationException? refusal = null;
        try
        {
            CArrowArrayReleaseHook.Chain(&array, owner);
        }
        catch (InvalidOperationException ex)
        {
            refusal = ex;
        }

        refusal.Should().NotBeNull();
        refusal!.Message.Should().Contain("release callback mismatch");
        _foreignCalls.Should().Be(1);
        owner.Disposals.Should().Be(1);
        pool.ActiveAllocations.Should().Be(0);

        ChainAndRelease();
    }

    private static void ChainAndRelease()
    {
        var pool = new TrackingMemoryPool();
        var owner = new CountingOwner(ColumnBatch(pool, 4));
        CArrowArray array = default;
        CArrowArrayExporter.ExportRecordBatch(owner.Batch, &array);
        CArrowArrayReleaseHook.Chain(&array, owner);
        ReleaseAsAConsumerDoes(&array);

        owner.Disposals.Should().Be(1);
        pool.ActiveAllocations.Should().Be(0);
    }

    // One column, held as ExecCallback holds it: a retained view, so the batch keeps its own reference on the
    // buffers beside the export's.
    private static RecordBatch ColumnBatch(TrackingMemoryPool pool, int rows)
    {
        var builder = new Int64Array.Builder();
        for (var i = 0; i < rows; i++) builder.Append(i);
        using var column = builder.Build(pool);
        var schema = new Schema([new Field("v", Int64Type.Default, nullable: true)], null);
        return new RecordBatch(schema, [ArrowOwnership.RetainArray(column)], rows);
    }

    // Read through the field, not through the hook's slot: the consumer side does not depend on the code it checks.
    private static IntPtr ReadRelease(CArrowArray* array) => (IntPtr)ReleaseField.GetValue(*array)!;

    private static void ReleaseAsAConsumerDoes(CArrowArray* array)
        => ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)ReadRelease(array))(array);

    private static IntPtr _foreignForwardsTo;
    private static int _foreignCalls;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ForeignRelease(CArrowArray* array)
    {
        Interlocked.Increment(ref _foreignCalls);
        ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)_foreignForwardsTo)(array);
    }

    private sealed class CountingOwner(RecordBatch batch) : IDisposable
    {
        private int _disposals;

        public RecordBatch Batch { get; } = batch;

        public int Disposals => Volatile.Read(ref _disposals);

        public void Dispose()
        {
            Interlocked.Increment(ref _disposals);
            Batch.Dispose();
        }
    }
}
