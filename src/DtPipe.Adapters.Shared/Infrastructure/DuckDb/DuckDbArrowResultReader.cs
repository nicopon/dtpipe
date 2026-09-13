using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.Shared.Infrastructure.DuckDb;

/// <summary>
/// Reads a DuckDB query result through the Arrow C Data interface: the schema comes off the
/// prepared statement before execution, and chunks are fetched lazily and converted to
/// <see cref="RecordBatch"/> without ever passing through a row.
/// </summary>
/// <remarks>
/// <para>
/// Shared by the <c>duck:</c> reader and the <c>--sql</c> processor, which differ only in how
/// their connection was populated — the processor registers Arrow streams as scannable tables
/// first, the reader opens a database file. Everything after the connection is this class.
/// </para>
/// <para>
/// The batches it yields own memory DuckDB allocated, released by the C Data release callback
/// rather than by a <c>MemoryAllocator</c>. The ordinary ownership rule applies — the consumer
/// disposes what it receives — but the allocator-based test net cannot observe this path.
/// </para>
/// </remarks>
public sealed class DuckDbArrowResultReader : IDisposable
{
    private readonly DuckDBConnection _connection;
    private readonly string _query;
    private readonly ILogger _logger;

    private DuckDBPreparedStatement? _statement;
    private IntPtr _arrowOptions = IntPtr.Zero;

    /// <summary>The result schema, available after <see cref="Prepare"/> and before any row is read.</summary>
    public Schema? Schema { get; private set; }

    public DuckDbArrowResultReader(DuckDBConnection connection, string query, ILogger? logger = null)
    {
        _connection = connection;
        _query = query;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Prepares the statement and reads its schema, which it returns. Must be called before
    /// <see cref="ReadRecordBatchesAsync"/>.
    /// </summary>
    public Schema Prepare()
    {
        if (DuckDbArrowNative.DuckDBPrepare(_connection.NativeConnection, _query, out var stmt) != DuckDBState.Success)
        {
            var errPtr = DuckDbArrowNative.DuckDBPrepareError(stmt);
            var msg = errPtr != IntPtr.Zero ? Marshal.PtrToStringUTF8(errPtr) : "unknown error";
            stmt.Dispose();
            throw new InvalidOperationException($"DuckDB prepare failed for query: {_query}. Error: {msg}");
        }
        _statement = stmt;

        // Arrow options carry the arrow_lossless_conversion flag, and serve both the schema
        // conversion and every chunk conversion afterwards.
        DuckDbArrowNative.DuckDBConnectionGetArrowOptions(_connection.NativeConnection, out _arrowOptions);
        Schema = InspectSchema();
        return Schema;
    }

    /// <summary>
    /// Sets the session flags the Arrow path depends on. Separate from <see cref="Prepare"/>
    /// because a caller may have its own session setup to interleave.
    /// </summary>
    public static async Task ApplyArrowSessionSettingsAsync(DuckDBConnection connection, CancellationToken ct = default)
    {
        using var cmd = connection.CreateCommand();
        // Export Arrow extension types faithfully (UUID -> FixedSizeBinary(16) + arrow.uuid).
        cmd.CommandText = "SET arrow_lossless_conversion = true";
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private unsafe Schema InspectSchema()
    {
        var count = DuckDbArrowNative.DuckDBPreparedStatementColumnCount(_statement!);
        if (count == 0)
            throw new InvalidOperationException($"Prepared statement returned 0 columns for query: {_query}");

        var logicalTypes = new DuckDBLogicalType[count];
        var namePointers = new IntPtr[count];

        try
        {
            for (ulong i = 0; i < count; i++)
            {
                logicalTypes[i] = DuckDbArrowNative.DuckDBPreparedStatementColumnLogicalType(_statement!, i);
                namePointers[i] = DuckDbArrowNative.DuckDBPreparedStatementColumnName(_statement!, i);
            }

            // DangerousGetHandle is safe here: the handles outlive the fixed block.
            var rawTypes = System.Array.ConvertAll(logicalTypes, t => t.DangerousGetHandle());

            CArrowSchema ffiSchema = default;
            fixed (IntPtr* pTypes = rawTypes)
            fixed (IntPtr* pNames = namePointers)
            {
                var errData = DuckDbArrowNative.DuckDBToArrowSchema(
                    _arrowOptions, pTypes, (byte**)pNames, count, &ffiSchema);

                if (errData != IntPtr.Zero)
                {
                    var msg = Marshal.PtrToStringUTF8(DuckDbArrowNative.DuckDBErrorMessage(errData)) ?? "unknown error";
                    DuckDbArrowNative.DuckDBDestroyErrorData(ref errData);
                    throw new InvalidOperationException($"duckdb_to_arrow_schema failed: {msg}");
                }
            }

            return CArrowSchemaImporter.ImportSchema(&ffiSchema);
        }
        finally
        {
            foreach (var namePtr in namePointers)
                if (namePtr != IntPtr.Zero) DuckDbArrowNative.DuckDBFree(namePtr);

            foreach (var lt in logicalTypes)
                lt?.Dispose();
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_statement is null)
            throw new InvalidOperationException("Call Prepare first.");

        await Task.Yield();

        var result = ExecuteStreaming();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Extracted as a non-async unsafe method: an async iterator cannot take the
                // address of a local (&ffiArray).
                var batch = FetchAndConvertChunk(result);
                if (batch is null) yield break;
                yield return batch;
            }
        }
        finally
        {
            DuckDbArrowNative.DuckDBDestroyResult(ref result);
        }
    }

    private DuckDBResult ExecuteStreaming()
    {
        if (DuckDbArrowNative.DuckDBExecutePreparedStreaming(_statement!, out var result) != DuckDBState.Success)
        {
            var errPtr = DuckDbArrowNative.DuckDBResultError(ref result);
            var msg = errPtr != IntPtr.Zero ? Marshal.PtrToStringUTF8(errPtr) : "unknown error";
            DuckDbArrowNative.DuckDBDestroyResult(ref result);
            throw new InvalidOperationException($"DuckDB streaming execute failed: {msg}");
        }

        if (!DuckDbArrowNative.DuckDBResultIsStreaming(result))
            _logger.LogDebug(
                "DuckDbArrowResultReader: optimizer chose materialized execution (non-streaming). " +
                "Result is correct but fully buffered in DuckDB memory before the first batch.");

        return result;
    }

    // duckdb_data_chunk_to_arrow copies buffer data, so the chunk is disposed immediately.
    private unsafe RecordBatch? FetchAndConvertChunk(DuckDBResult result)
    {
        using var chunk = DuckDbArrowNative.DuckDBFetchChunk(result);
        if (chunk.IsInvalid) return null;

        CArrowArray ffiArray = default;
        var errData = DuckDbArrowNative.DuckDBDataChunkToArrow(_arrowOptions, chunk, &ffiArray);
        if (errData != IntPtr.Zero)
        {
            var msg = Marshal.PtrToStringUTF8(DuckDbArrowNative.DuckDBErrorMessage(errData)) ?? "unknown error";
            DuckDbArrowNative.DuckDBDestroyErrorData(ref errData);
            throw new InvalidOperationException($"duckdb_data_chunk_to_arrow failed: {msg}");
        }

        return CArrowArrayImporter.ImportRecordBatch(&ffiArray, Schema!);
    }

    public void Dispose()
    {
        if (_arrowOptions != IntPtr.Zero) DuckDbArrowNative.DuckDBDestroyArrowOptions(ref _arrowOptions);
        _statement?.Dispose();
        _statement = null;
    }
}
