using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// Hands out <see cref="RecordBatch"/>es that reached managed code through the Arrow C Data
/// interface — the shape every DuckDB read produces — and counts the release callbacks they fire.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="TrackingMemoryPool"/> counts what crosses a <c>MemoryAllocator</c>, and the memory
/// behind an imported batch never does: it is freed by the C release callback. A pool-based
/// assertion over this path stays green whether or not the consumer disposes, so it must not be
/// used to claim anything about it. The counter here sits on the callback, which is the event
/// "the memory was actually handed back".
/// </para>
/// <para>
/// A batch handed out by <see cref="Import"/> carries the same ownership contract as one from
/// <c>DuckDbArrowResultReader</c>: whoever receives it disposes it exactly once, and a retained
/// view (<c>ArrowOwnership.RetainAll</c>) postpones the callback until the last holder is gone.
/// </para>
/// </remarks>
public sealed unsafe class CDataReleaseProbe : IDisposable
{
    // The importer MOVES the exported struct into its own allocation, so the CArrowArray* the
    // callback receives is not the one Import allocated. private_data survives the move intact,
    // which makes it the only usable key back to the probe that handed the batch out.
    private static readonly ConcurrentDictionary<IntPtr, CDataReleaseProbe> Owners = new();

    // Every export from CArrowArrayExporter carries the same release function; capturing it once
    // is enough to chain to it after counting, and Import refuses a second, different one.
    private static IntPtr _exporterRelease;

    private long _imported;
    private long _released;

    /// <summary>Batches handed out by <see cref="Import"/>.</summary>
    public long Imported => Interlocked.Read(ref _imported);

    /// <summary>Batches whose C Data release callback has fired.</summary>
    public long Released => Interlocked.Read(ref _released);

    /// <summary>Batches handed out and not yet released.</summary>
    public long Outstanding => Imported - Released;

    /// <summary>
    /// Round-trips <paramref name="source"/> through the C Data interface and returns the imported
    /// batch. The caller owns <paramref name="source"/> and may dispose it immediately: the export
    /// keeps the buffers alive until the release callback runs.
    /// </summary>
    public RecordBatch Import(RecordBatch source)
    {
        var exported = CArrowArray.Create();
        CArrowArrayExporter.ExportRecordBatch(source, exported);

        var slot = ReleaseSlot(exported);
        var exporterRelease = *slot;
        if (exporterRelease == IntPtr.Zero)
            throw new InvalidOperationException(
                "No release callback where the C Data layout puts one — the struct's fields have moved.");

        var known = Interlocked.CompareExchange(ref _exporterRelease, exporterRelease, IntPtr.Zero);
        if (known != IntPtr.Zero && known != exporterRelease)
            throw new InvalidOperationException(
                "Two different exporter release callbacks — chaining to one of them would be wrong.");

        Owners[(IntPtr)exported->private_data] = this;
        *slot = (IntPtr)(delegate* unmanaged<CArrowArray*, void>)&CountingRelease;

        Interlocked.Increment(ref _imported);
        try
        {
            return CArrowArrayImporter.ImportRecordBatch(exported, source.Schema);
        }
        finally
        {
            // The import moved the contents out; this frees the now-empty struct, not the data.
            CArrowArray.Free(exported);
        }
    }

    /// <summary>
    /// Imports every batch of <paramref name="source"/>, disposing each managed original once it
    /// has been exported — so nothing but the imported batch keeps the data alive, exactly as
    /// when DuckDB owns it.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ImportAllAsync(
        IEnumerable<RecordBatch> source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var batch in source)
        {
            ct.ThrowIfCancellationRequested();
            RecordBatch imported;
            using (batch) imported = Import(batch);
            yield return imported;
            await Task.Yield();
        }
    }

    // Apache.Arrow keeps the release field internal, and the C Data layout fixes its position:
    // the callback pointer sits immediately before private_data in every ArrowArray.
    private static IntPtr* ReleaseSlot(CArrowArray* array) => (IntPtr*)&array->private_data - 1;

    [UnmanagedCallersOnly]
    private static void CountingRelease(CArrowArray* array)
    {
        if (Owners.TryRemove((IntPtr)array->private_data, out var probe))
            Interlocked.Increment(ref probe._released);

        var inner = (delegate* unmanaged<CArrowArray*, void>)_exporterRelease;
        if (inner != null) inner(array);
    }

    /// <summary>Drops the probe's rows from the shared key table; counts are read before this.</summary>
    public void Dispose()
    {
        foreach (var entry in Owners)
            if (ReferenceEquals(entry.Value, this))
                Owners.TryRemove(entry.Key, out _);
    }
}
