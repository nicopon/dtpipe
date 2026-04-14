using System.Runtime.InteropServices;
using Apache.Arrow.C;
using DuckDB.NET.Native;

namespace DtPipe.Processors.DuckDB;

/// <summary>
/// P/Invoke bindings for DuckDB's Arrow C Data Interface (input and output paths).
/// All functions are in the "duckdb" native library bundled with DuckDB.NET.Data.Full.
/// </summary>
internal static partial class DuckDBArrowNativeMethods
{
    private const string DuckDbLibrary = "duckdb";

    // ── INPUT: register an Arrow stream as a DuckDB table (zero-copy) ────────────────

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_arrow_scan", StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial DuckDBState DuckDBArrowScan(
        DuckDBNativeConnection connection, string tableName, CArrowArrayStream* stream);

    // ── PREPARE ───────────────────────────────────────────────────────────────────────

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepare", StringMarshalling = StringMarshalling.Utf8)]
    internal static partial DuckDBState DuckDBPrepare(
        DuckDBNativeConnection conn, string query, out DuckDBPreparedStatement stmt);

    // ── SCHEMA FROM PREPARED STATEMENT (before any execution) ────────────────────────

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepared_statement_column_count")]
    internal static partial ulong DuckDBPreparedStatementColumnCount(DuckDBPreparedStatement stmt);

    // Returned pointer must be freed with DuckDBFree.
    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepared_statement_column_name")]
    internal static partial IntPtr DuckDBPreparedStatementColumnName(
        DuckDBPreparedStatement stmt, ulong col);

    // Returned DuckDBLogicalType (SafeHandle) must be disposed.
    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepared_statement_column_logical_type")]
    internal static partial DuckDBLogicalType DuckDBPreparedStatementColumnLogicalType(
        DuckDBPreparedStatement stmt, ulong col);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_free")]
    internal static partial void DuckDBFree(IntPtr ptr);

    // ── ARROW OPTIONS (carries arrow_lossless_conversion flag) ────────────────────────

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_connection_get_arrow_options")]
    internal static partial void DuckDBConnectionGetArrowOptions(
        DuckDBNativeConnection conn, out IntPtr opts);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_arrow_options")]
    internal static partial void DuckDBDestroyArrowOptions(ref IntPtr opts);

    // ── ARROW SCHEMA + CHUNK CONVERSION (non-deprecated) ─────────────────────────────

    // Converts a DuckDB schema (type + name arrays) to an Arrow CArrowSchema.
    // Returns duckdb_error_data (IntPtr): null = success, non-null = error.
    // Caller must destroy error with DuckDBDestroyErrorData; schema with out_schema->release().
    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_to_arrow_schema")]
    internal static unsafe partial IntPtr DuckDBToArrowSchema(
        IntPtr opts,
        IntPtr* types,           // array of duckdb_logical_type opaque pointers
        byte** names,            // array of UTF-8 C string pointers
        ulong count,
        CArrowSchema* outSchema);

    // Converts a DuckDB DataChunk to an Arrow CArrowArray (CDI).
    // Returns duckdb_error_data; null = success. Caller releases error + Arrow array.
    // duckdb_data_chunk_to_arrow copies buffer data — chunk can be freed after this call.
    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_to_arrow")]
    internal static unsafe partial IntPtr DuckDBDataChunkToArrow(
        IntPtr opts,
        DuckDBDataChunk chunk,
        CArrowArray* outArray);

    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_error_data")]
    internal static partial void DuckDBDestroyErrorData(ref IntPtr errorData);

    // Borrowed pointer — do not free separately; freed by DuckDBDestroyErrorData.
    [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_error_message")]
    internal static partial IntPtr DuckDBErrorMessage(IntPtr errorData);

    // ── STREAMING EXECUTE (deprecated — no non-deprecated lazy alternative exists) ────
    // DllImport used instead of LibraryImport: the source generator (SYSLIB1051) cannot
    // marshal DuckDBResult (struct with private fields in another assembly).

    // SOLE PATH TO LAZY STREAMING IN DUCKDB C API v1.5.
    // Deprecated and scheduled for removal — but duckdb_pending_prepared (non-deprecated)
    // forces full materialisation (allow_streaming=false → PhysicalMaterializedCollector).
    // There is no non-deprecated equivalent in v1.5. When DuckDB introduces one, migrate here.
    // The sentinel test DuckDB_StreamingAPI_IsAvailable in DuckDBSqlProcessorTests.cs will
    // fail with EntryPointNotFoundException if a DuckDB upgrade removes this function,
    // forcing a conscious decision. Do NOT add a silent fallback to materialized execution.
    [DllImport(DuckDbLibrary, EntryPoint = "duckdb_execute_prepared_streaming")]
    internal static extern DuckDBState DuckDBExecutePreparedStreaming(
        DuckDBPreparedStatement stmt, out DuckDBResult result);

    [DllImport(DuckDbLibrary, EntryPoint = "duckdb_result_is_streaming")]
    [return: MarshalAs(UnmanagedType.I1)]
    internal static extern bool DuckDBResultIsStreaming(DuckDBResult result);

    // ── LAZY ITERATION (non-deprecated) ──────────────────────────────────────────────

    // duckdb_result passed by value: internal_data heap pointer is shared across copies,
    // so successive calls on value-copies correctly advance the same streaming cursor.
    [DllImport(DuckDbLibrary, EntryPoint = "duckdb_fetch_chunk")]
    internal static extern DuckDBDataChunk DuckDBFetchChunk(DuckDBResult result);

    [DllImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_result")]
    internal static extern void DuckDBDestroyResult(ref DuckDBResult result);

    // Borrowed — valid until DuckDBDestroyResult is called.
    [DllImport(DuckDbLibrary, EntryPoint = "duckdb_result_error")]
    internal static extern IntPtr DuckDBResultError(ref DuckDBResult result);
}
