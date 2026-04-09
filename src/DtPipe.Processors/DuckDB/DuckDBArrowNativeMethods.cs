using System.Runtime.InteropServices;
using DuckDB.NET.Native;

namespace DtPipe.Processors.DuckDB;

internal static partial class DuckDBArrowNativeMethods
{
    private const string DuckDbLibrary = "duckdb";

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBArrow
    {
        public nint InternalPtr;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBArrowStream
    {
        public nint InternalPtr;
    }

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query_arrow", StringMarshalling = StringMarshalling.Utf8)]
    public static partial DuckDBState DuckDBQueryArrow(DuckDBNativeConnection connection, string query, out DuckDBArrow outResult);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_arrow")]
    public static partial void DuckDBDestroyArrow(ref nint result);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query_arrow_schema")]
    public static unsafe partial DuckDBState DuckDBQueryArrowSchema(DuckDBArrow result, nint* outSchema);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query_arrow_array")]
    public static unsafe partial DuckDBState DuckDBQueryArrowArray(DuckDBArrow result, nint* outArray);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_arrow_scan", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial DuckDBState DuckDBArrowScan(DuckDBNativeConnection connection, string tableName, Apache.Arrow.C.CArrowArrayStream* stream);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_arrow_row_count")]
    public static partial long DuckDBArrowRowCount(DuckDBArrow result);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_arrow_column_count")]
    public static partial long DuckDBArrowColumnCount(DuckDBArrow result);
}
