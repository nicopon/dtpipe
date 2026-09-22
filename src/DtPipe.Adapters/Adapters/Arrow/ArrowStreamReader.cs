using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.Arrow;

public class ArrowAdapterStreamReader : IColumnarStreamReader
{
	private readonly string _path;
	private readonly ArrowReaderOptions _options;
	private readonly ILogger _logger;

	private Stream? _inputStream;
	private ArrowStreamTerminationProbe? _probe;
	private Apache.Arrow.Ipc.ArrowStreamReader? _arrowReader;
	private ArrowFileReader? _arrowFileReader;
    private bool _isIpcFile;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema => _isIpcFile ? _arrowFileReader?.Schema : _arrowReader?.Schema;

	private string Source => string.IsNullOrEmpty(_path) || _path == "-" ? "STDIN" : _path;

	public ArrowAdapterStreamReader(string path, ArrowReaderOptions options, ILogger? logger = null)
	{
		_path = path;
		_options = options;
		_logger = logger ?? NullLogger.Instance;
	}

	public Task OpenAsync(CancellationToken ct = default)
	{
		if (string.IsNullOrEmpty(_path) || _path == "-")
		{
			if (!Console.IsInputRedirected)
			{
				throw new InvalidOperationException("Structure input (STDIN) is not redirected.");
			}
			_inputStream = Console.OpenStandardInput();
            _isIpcFile = false;
		}
		else
		{
			if (!File.Exists(_path))
				throw new FileNotFoundException($"Arrow file not found: {_path}", _path);

			_inputStream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);

            _isIpcFile = _path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
                         _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
		}

        if (_isIpcFile)
        {
            _arrowFileReader = new ArrowFileReader(_inputStream);
            var schema = _arrowFileReader.Schema;
            Columns = MapSchema(schema);
        }
        else
        {
            _probe = new ArrowStreamTerminationProbe(_inputStream);
            _arrowReader = new Apache.Arrow.Ipc.ArrowStreamReader(_probe);
            var schema = _arrowReader.Schema;
            Columns = MapSchema(schema);
        }

        return Task.CompletedTask;
	}

    /// <summary>
    /// The one loop that pulls batches out of whichever reader was opened, and the one place that
    /// decides what the end of the data means.
    /// </summary>
    /// <remarks>
    /// Both public read methods go through this. Four copies of <c>if (batch == null) break;</c>
    /// lived here instead, and none of them asked whether the stream had ended or merely stopped —
    /// which is how a killed producer became a successful run. A second loop added beside this one
    /// starts that over.
    /// </remarks>
    private async IAsyncEnumerable<RecordBatch> ReadAllAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        if (Columns is null) throw new InvalidOperationException("Call OpenAsync first.");

        if (_isIpcFile && _arrowFileReader != null)
        {
            // The IPC file format carries its batch count in the footer, so a file that stops
            // short is a file whose footer disagrees with its body — say so rather than break.
            int count = await _arrowFileReader.RecordBatchCountAsync();
            for (int i = 0; i < count; i++)
            {
                var batch = await _arrowFileReader.ReadRecordBatchAsync(i, ct)
                    ?? throw new EndOfStreamException(
                        $"Arrow file '{Source}' is truncated: its footer declares {count} record " +
                        $"batches and the body runs out at {i}.");
                yield return TakeOwnership(batch);
            }
        }
        else if (_arrowReader != null)
        {
            while (true)
            {
                var batch = await _arrowReader.ReadNextRecordBatchAsync(ct);
                if (batch is null)
                {
                    EnsureStreamEnded();
                    yield break;
                }
                yield return TakeOwnership(batch);
            }
        }
    }

    /// <summary>
    /// Re-homes an IPC batch onto buffers that carry their own reference count, and releases the
    /// message body it was read into.
    /// </summary>
    /// <remarks>
    /// <b>An IPC batch does not obey the ownership contract the rest of the pipeline is written
    /// against</b> (CLAUDE.md › "RecordBatch ownership"). Its column buffers hold no shared handle;
    /// the whole message body is a single allocation owned by the <see cref="RecordBatch"/>. So
    /// <c>ArrowOwnership.RetainArray</c> — which the six column-aliasing transformers call to keep
    /// an input column alive past the segment runner's dispose — bumps nothing, and disposing the
    /// input frees the body under the output. Reading it back then dereferences freed native
    /// memory: <c>--mask</c>, <c>--fake</c>, <c>--null</c> and <c>--format</c> each crashed on an
    /// <c>arrow:</c> source with a <c>NullReferenceException</c> raised from inside Arrow, while
    /// the same transformers over a <c>parquet:</c> source were correct.
    ///
    /// <para>
    /// The machinery for sharing a buffer is <c>internal</c> to Apache.Arrow, so the handle cannot
    /// be attached from here: the copy is what buys the contract. It is one allocator copy per
    /// batch — measured at roughly three times the cost of reading the batch, ~3 ns per row — paid
    /// on this reader only, and it is what lets every consumer downstream obey one rule instead of
    /// asking where its batch came from.
    /// </para>
    /// </remarks>
    private static RecordBatch TakeOwnership(RecordBatch batch)
    {
        using (batch)
        {
            var allocator = MemoryAllocator.Default.Value;
            int columnCount = batch.Schema.FieldsList.Count;
            var columns = new IArrowArray[columnCount];
            for (int i = 0; i < columnCount; i++)
                columns[i] = global::Apache.Arrow.ArrowArrayFactory.BuildArray(batch.Column(i).Data.Clone(allocator));

            return new RecordBatch(batch.Schema, columns, batch.Length);
        }
    }

    /// <summary>
    /// Refuses a stream that stopped on a message boundary without its end-of-stream marker.
    /// </summary>
    private void EnsureStreamEnded()
    {
        if (_probe is not { SourceRanDry: true }) return;

        throw new EndOfStreamException(
            $"Arrow stream '{Source}' is truncated: it stops without the end-of-stream marker, so " +
            "the producer died or was killed before it finished writing. The rows read so far are " +
            "a partial result.");
    }

    public IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default)
        => ReadAllAsync(ct);

    private List<PipeColumnInfo> MapSchema(Schema schema)
    {
        return schema.FieldsList.Select(f => new PipeColumnInfo(
            f.Name,
            ArrowTypeMapper.GetClrTypeFromField(f),
            f.IsNullable
        )).ToList();
    }


	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
        // Row-mode consumer: FlattenBatch fully materialises each row into object?[], so the batch
        // can be disposed here once its rows have been yielded.
        await foreach (var batch in ReadAllAsync(ct))
        {
            using (batch)
            {
                foreach (var memory in ArrowRowConverter.FlattenBatch(batch, batchSize))
                {
                    yield return memory;
                }
            }
        }
	}


	public async ValueTask DisposeAsync()
	{
		if (_inputStream != null)
		{
			await _inputStream.DisposeAsync();
			_inputStream = null;
		}
	}
}
