using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Expressions;
using DtPipe.Core.Models;
using DtPipe.Core.Security;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.ObjectStorage;

/// <summary>
/// Reads an object-storage location by delegating to the DuckDB reader: the location becomes a
/// read_parquet/read_csv/read_json call on an in-memory database, so batches arrive through the
/// same Arrow path as every other DuckDB source.
///
/// The wrapper exists for one reason beyond delegation: the credential statement contains
/// literal secrets, and a DuckDB error during connection setup can quote the statement back.
/// Every failure is re-thrown with those values masked.
/// </summary>
public sealed class ObjectStorageStreamReader : IColumnarStreamReader, IBatchSizeConfigurable
{
    private readonly ObjectStorageBinding _binding;
    private readonly DuckDataSourceReader _inner;

    public ObjectStorageStreamReader(
        ObjectStorageBinding binding,
        ILogger? logger = null,
        IStringContentResolver? resolver = null,
        IMcpSecurityContext? mcpSecurityContext = null)
    {
        _binding = binding;
        var options = new DuckDbReaderOptions { InitSql = binding.InitSql };
        // The MCP context is forwarded so a tool-driven session keeps DuckDB's
        // enable_external_access=false guard: object storage must not become a way around it.
        _inner = new DuckDataSourceReader(
            DuckDbConnectionHelper.InMemoryConnectionString,
            binding.SelectQuery,
            options,
            logger,
            resolver: resolver,
            mcpSecurityContext: mcpSecurityContext);
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _inner.Columns;

    public Schema? Schema => _inner.Schema;

    public int BatchSize
    {
        get => _inner.BatchSize;
        set => _inner.BatchSize = value;
    }

    public long MaxBatchBytes
    {
        get => _inner.MaxBatchBytes;
        set => _inner.MaxBatchBytes = value;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        try
        {
            await _inner.OpenAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Wrap(ex);
        }
    }

    private InvalidOperationException Wrap(Exception ex) => new(
        $"Failed to read '{_binding.Uri.DuckDbUri}': {_binding.Secret.Redact(ex.Message)}", ex);

    public IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default)
        => _inner.ReadRecordBatchesAsync(ct);

    public IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct = default)
        => _inner.ReadBatchesAsync(batchSize, ct);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
