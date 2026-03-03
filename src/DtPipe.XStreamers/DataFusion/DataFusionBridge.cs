using System.Runtime.InteropServices;
using Apache.Arrow.C;

namespace DtPipe.XStreamers.DataFusion;

internal static partial class DataFusionBridge
{
    private const string LibName = "dtpipe_xstreamers_datafusion";

    [LibraryImport(LibName, EntryPoint = "dtfb_runtime_new")]
    internal static partial nint RuntimeNew();

    [LibraryImport(LibName, EntryPoint = "dtfb_runtime_destroy")]
    internal static partial void RuntimeDestroy(nint rt);

    [LibraryImport(LibName, EntryPoint = "dtfb_context_new")]
    internal static partial nint ContextNew(nint runtime);

    [LibraryImport(LibName, EntryPoint = "dtfb_context_destroy")]
    internal static partial void ContextDestroy(nint ctx);

    [LibraryImport(LibName, EntryPoint = "dtfb_register_parquet")]
    internal static partial int RegisterParquet(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [LibraryImport(LibName, EntryPoint = "dtfb_register_csv")]
    internal static partial int RegisterCsv(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    [LibraryImport(LibName, EntryPoint = "dtfb_register_stream")]
    internal static unsafe partial int RegisterStream(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, CArrowArrayStream* stream);

    [LibraryImport(LibName, EntryPoint = "dtfb_register_batches")]
    internal static unsafe partial int RegisterBatches(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string name, CArrowSchema* schema, CArrowArray** batches, nuint numBatches);

    [LibraryImport(LibName, EntryPoint = "dtfb_get_schema")]
    internal static unsafe partial int GetSchema(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string sql, CArrowSchema* schema);

    [LibraryImport(LibName, EntryPoint = "dtfb_execute_to_fd")]
    internal static partial int ExecuteToFd(nint ctx, [MarshalAs(UnmanagedType.LPUTF8Str)] string sql, nint handle);
}
