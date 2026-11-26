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
    private readonly record struct RetainedExport(IDisposable Owner, ILogger Logger);

    // Keyed by private_data: a consumer that takes the array moves the struct and releases its own
    // copy, and private_data survives the move.
    private static readonly ConcurrentDictionary<IntPtr, RetainedExport> RetainedExports = new();

    // The release CArrowArrayExporter writes into every array it exports, captured from the first one.
    private static IntPtr _exporterRelease;

    /// <summary>
    /// Makes the release of <paramref name="array"/>, just written by <see cref="CArrowArrayExporter"/>,
    /// run the exporter's release and then dispose <paramref name="owner"/>. An array that carries a
    /// release other than the one captured is released, its owner disposed, and refused.
    /// </summary>
    public static void Chain(CArrowArray* array, IDisposable owner, ILogger? logger = null)
    {
        if (array == null)
        {
            owner.Dispose();
            throw new ArgumentNullException(nameof(array));
        }

        var slot = ReleaseSlot(array);
        var innerRelease = *slot;
        var selfRelease = (IntPtr)(delegate* unmanaged[Cdecl]<CArrowArray*, void>)&ReleaseRetained;
        if (innerRelease == IntPtr.Zero)
        {
            owner.Dispose();
            throw new InvalidOperationException("Cannot chain release on an already-released Arrow array.");
        }
        if (innerRelease == selfRelease)
        {
            owner.Dispose();
            throw new InvalidOperationException("Arrow array release hook is already chained.");
        }
        if (array->private_data == null)
        {
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)innerRelease)(array);
            owner.Dispose();
            throw new InvalidOperationException("Exported Arrow array has null private_data.");
        }

        var existing = Interlocked.CompareExchange(ref _exporterRelease, innerRelease, IntPtr.Zero);
        if (existing != IntPtr.Zero && existing != innerRelease)
        {
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)innerRelease)(array);
            owner.Dispose();
            throw new InvalidOperationException("Unexpected Arrow exporter release callback mismatch.");
        }

        if (!RetainedExports.TryAdd((IntPtr)array->private_data, new RetainedExport(owner, logger ?? NullLogger.Instance)))
        {
            ((delegate* unmanaged[Cdecl]<CArrowArray*, void>)innerRelease)(array);
            owner.Dispose();
            throw new InvalidOperationException("Duplicate private_data while retaining exported Arrow array.");
        }
        *slot = selfRelease;
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
        if (array == null) return;

        RetainedExport retained = default;
        bool hasRetained = false;
        try
        {
            hasRetained = RetainedExports.TryRemove((IntPtr)array->private_data, out retained);

            var inner = (delegate* unmanaged[Cdecl]<CArrowArray*, void>)Volatile.Read(ref _exporterRelease);
            if (inner != null) inner(array);
        }
        catch (Exception ex)
        {
            *ReleaseSlot(array) = IntPtr.Zero;
            SafeLogError(hasRetained ? retained.Logger : NullLogger.Instance, ex, "Releasing an exported Arrow array failed");
        }

        try
        {
            if (hasRetained) retained.Owner.Dispose();
        }
        catch (Exception ex)
        {
            SafeLogError(retained.Logger, ex, "Disposing the owner of an exported Arrow array failed");
        }
    }

    private static void SafeLogError(ILogger logger, Exception ex, string message)
    {
        try
        {
            logger.LogError(ex, "{Message}", message);
        }
        catch
        {
            // Never let a logging fault unwind across an unmanaged callback.
        }
    }
}
