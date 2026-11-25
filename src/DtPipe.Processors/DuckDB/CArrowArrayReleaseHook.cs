using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.C;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Processors.DuckDB;

/// <summary>
/// Keeps what owns an exported <see cref="CArrowArray"/> until its consumer releases the array: the
/// release <see cref="CArrowArrayExporter"/> wrote is replaced by one that runs it, then disposes the
/// owner. Each happens once, on the consumer's release or on <see cref="ReleaseExportedIfUnclaimed"/>.
/// </summary>
internal static unsafe class CArrowArrayReleaseHook
{
    // Keyed by private_data: a consumer that takes the array moves the struct and releases its own
    // copy, and private_data survives the move.
    private static readonly ConcurrentDictionary<IntPtr, IDisposable> RetainedExports = new();

    /// <summary>
    /// Where a fault of a release is told. The release is an unmanaged callback: an exception leaving it ends
    /// the process, so each is caught and logged here. The processor opened last sets it.
    /// </summary>
    internal static ILogger Logger { get; set; } = NullLogger.Instance;

    // The release CArrowArrayExporter writes into every array it exports, captured from the first one.
    private static IntPtr _exporterRelease;

    /// <summary>
    /// Makes the release of <paramref name="array"/>, just written by <see cref="CArrowArrayExporter"/>,
    /// run the exporter's release and then dispose <paramref name="owner"/>. An array that carries a
    /// release other than the one captured is released, its owner disposed, and refused.
    /// </summary>
    public static void Chain(CArrowArray* array, IDisposable owner)
    {
        var slot = ReleaseSlot(array);
        var innerRelease = *slot;
        var existing = Interlocked.CompareExchange(ref _exporterRelease, innerRelease, IntPtr.Zero);
        if (existing != IntPtr.Zero && existing != innerRelease)
        {
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)innerRelease)(array);
            owner.Dispose();
            throw new InvalidOperationException("Unexpected Arrow exporter release callback mismatch.");
        }

        RetainedExports[(IntPtr)array->private_data] = owner;
        *slot = (IntPtr)(delegate* unmanaged[Cdecl]<CArrowArray*, void>)&ReleaseRetained;
    }

    /// <summary>
    /// Releases <paramref name="array"/> unless it is released already or a consumer took it: either
    /// leaves its release null.
    /// </summary>
    public static void ReleaseExportedIfUnclaimed(CArrowArray* array)
    {
        var releasePtr = *ReleaseSlot(array);
        if (releasePtr != IntPtr.Zero)
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)releasePtr)(array);
    }

    // Apache.Arrow keeps CArrowArray.release internal; the Arrow C Data ABI places it immediately before
    // private_data. CArrowArrayReleaseHookTests reads the struct's layout and fails if that moves.
    internal static IntPtr* ReleaseSlot(CArrowArray* array) => (IntPtr*)&array->private_data - 1;

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void ReleaseRetained(CArrowArray* array)
    {
        IDisposable? owner = null;
        try
        {
            RetainedExports.TryRemove((IntPtr)array->private_data, out owner);

            var inner = (delegate* unmanaged[Cdecl]<CArrowArray*, void>)_exporterRelease;
            if (inner != null) inner(array);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Releasing an exported Arrow array failed");
        }

        try
        {
            owner?.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Disposing the owner of an exported Arrow array failed");
        }
    }
}
