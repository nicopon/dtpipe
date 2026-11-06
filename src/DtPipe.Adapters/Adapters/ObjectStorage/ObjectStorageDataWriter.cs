using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Expressions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.ObjectStorage;

/// <summary>
/// Writes an object-storage location by staging rows in an in-memory DuckDB table and issuing a
/// single COPY ... TO '&lt;uri&gt;' at completion. DuckDB owns the upload, so multipart and
/// partitioned output come for free and nothing is buffered on local disk.
///
/// An object write is an overwrite of one key — there is no append, truncate or upsert against a
/// blob — so no write strategy is exposed. The COPY is deliberately deferred to CompleteAsync:
/// a pipeline that faults mid-stream leaves the destination key untouched rather than replacing
/// it with a partial object.
/// </summary>
public sealed class ObjectStorageDataWriter : IColumnarDataWriter, IColumnTypeGate
{
    public IReadOnlyList<ColumnTypeRefusal> RefuseColumns(Apache.Arrow.Schema schema) => _inner.RefuseColumns(schema);

    private const string StagingTable = "dtpipe_object_out";

    private readonly ObjectStorageBinding _binding;
    private readonly DuckDbDataWriter _inner;

    public ObjectStorageDataWriter(
        ObjectStorageBinding binding,
        ILogger<DuckDbDataWriter> logger,
        ITypeMapper typeMapper,
        IStringContentResolver? resolver = null)
    {
        _binding = binding;
        var options = new DuckDbWriterOptions
        {
            Table = StagingTable,
            Strategy = DuckDbWriteStrategy.Recreate,
            InitSql = binding.InitSql,
        };
        _inner = new DuckDbDataWriter(DuckDbConnectionHelper.InMemoryConnectionString, options, logger, typeMapper, resolver);
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        try
        {
            await _inner.InitializeAsync(columns, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Wrap(ex);
        }
    }

    public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
        => _inner.WriteRecordBatchAsync(batch, ct);

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        try
        {
            await _inner.CompleteAsync(ct);
            await _inner.ExecuteCommandAsync(_binding.BuildCopyStatement(StagingTable), ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw Wrap(ex);
        }
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
        => _inner.ExecuteCommandAsync(command, ct);

    public ValueTask DisposeAsync()
        // The staging database is in-memory: dropping the connection without a COPY simply
        // discards the rows, which is what a faulted pipeline should leave behind.
        => _inner.DisposeAsync();

    private InvalidOperationException Wrap(Exception ex) => new(
        $"Failed to write '{_binding.Uri.DuckDbUri}': {_binding.Secret.Redact(ex.Message)}", ex);
}
