using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using System.IO.Pipes;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

/// <summary>
/// Writes data to Arrow IPC format.
/// This writer is now purely columnar and relies on the engine to provide RecordBatches.
/// </summary>
public sealed class ArrowAdapterDataWriter : IColumnarDataWriter, IRequiresOptions<ArrowWriterOptions>, ISchemaInspector
{
    private readonly string _path;
    private readonly ArrowWriterOptions _options;

    private Stream? _outputStream;
    private ArrowStreamWriter? _arrowStreamWriter;
    private ArrowFileWriter? _arrowFileWriter;
    private bool _isIpcFile;
    private Schema? _schema;

    public ArrowAdapterDataWriter(string path) : this(path, new ArrowWriterOptions())
    {
    }

    public ArrowAdapterDataWriter(string path, ArrowWriterOptions options)
    {
        _path = path;
        _options = options;
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        using (batch)
        {
            if (_isIpcFile)
                await _arrowFileWriter!.WriteRecordBatchAsync(batch, ct);
            else
                await _arrowStreamWriter!.WriteRecordBatchAsync(batch, ct);
        }
    }

    public bool RequiresTargetInspection => false;

	// Opening the target for inspection can block indefinitely: a FIFO with no writer yet blocks
	// its reader's open() until one arrives, and this process is itself about to become that
	// writer (InitializeAsync, right after this call returns) - a run bound to a named pipe via
	// --bind-output deadlocked here under a real reader on the other end. Bounded so a preview
	// reports "unknown" instead, which InspectTargetAsync is already free to return.
	private static readonly TimeSpan InspectionOpenTimeout = TimeSpan.FromSeconds(2);

	public async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (_path == "-")
		{
			return new TargetSchemaInfo([], false, null, null, null);
		}

        if (string.IsNullOrEmpty(_path))
        {
             throw new InvalidOperationException("Output path is required. Use '-' for standard output.");
        }

		// A pipe location is never a filesystem path: File.Exists would read it as one relative to
		// the current directory and (harmlessly, but wrongly) report it as absent. Its state mirrors
		// a FIFO with no writer yet - unknown until connected, which InitializeAsync is about to do.
		if (ArrowPipeLocation.TryParse(_path, out _))
		{
			return new TargetSchemaInfo([], true, null, null, null);
		}

		if (!File.Exists(_path))
		{
			return new TargetSchemaInfo([], false, null, null, null);
		}

		var openTask = Task.Run(() => File.OpenRead(_path), ct);
		FileStream fs;
		try
		{
			fs = await openTask.WaitAsync(InspectionOpenTimeout, ct);
		}
		catch (TimeoutException)
		{
			// The open is still blocked on the far side of a pipe with no writer - the same
			// position InitializeAsync is about to fill. Close it in the background rather than
			// abandon an open read end that could later steal bytes from the real consumer.
			_ = openTask.ContinueWith(t =>
			{
				if (t.IsCompletedSuccessfully) t.Result.Dispose();
			}, TaskScheduler.Default);
			return new TargetSchemaInfo([], true, null, null, null);
		}

		try
		{
			using (fs)
			{
				// Try reading as file first (IPC file format)
				if (_path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) || _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase))
				{
					try
					{
						using var reader = new ArrowFileReader(fs);
						var schema = reader.Schema;
						var columns = MapArrowSchema(schema);
						return new TargetSchemaInfo(columns, true, null, fs.Length, null);
					}
					catch { /* Fallback to stream */ }
				}

				// Try as stream (IPC stream format)
				fs.Position = 0;
				using var streamReader = new ArrowStreamReader(fs);
				var streamSchema = streamReader.Schema;
				var streamColumns = MapArrowSchema(streamSchema);
				return new TargetSchemaInfo(streamColumns, true, null, fs.Length, null);
			}
		}
		catch
		{
			return new TargetSchemaInfo([], true, null, new FileInfo(_path).Length, null);
		}
	}

	private IReadOnlyList<TargetColumnInfo> MapArrowSchema(Schema schema)
	{
		var columns = new List<TargetColumnInfo>();
		foreach (var field in schema.FieldsList)
		{
			var clrType = ArrowTypeMapper.GetClrTypeFromField(field);
			columns.Add(new TargetColumnInfo(
				field.Name,
				field.DataType.Name,
				clrType,
				field.IsNullable,
				false, false, null, null, null));
		}
		return columns;
	}

	// See ArrowAdapterStreamReader.PipeConnectTimeout: the pipeline node creates the pipe before
	// launching the child that writes this side of it, so the bound only guards against a
	// misconfigured one.
	private static readonly TimeSpan PipeConnectTimeout = TimeSpan.FromSeconds(10);

	public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		if (_path == "-")
		{
			_outputStream = Console.OpenStandardOutput();
            _isIpcFile = false;
		}
		else if (ArrowPipeLocation.TryParse(_path, out var pipeName))
		{
			var client = new NamedPipeClientStream(".", pipeName, PipeDirection.Out, PipeOptions.Asynchronous);
			try
			{
				await client.ConnectAsync((int)PipeConnectTimeout.TotalMilliseconds, ct);
			}
			catch (TimeoutException ex)
			{
				client.Dispose();
				throw new TimeoutException(
					$"Arrow writer could not connect to named pipe '{pipeName}' within {PipeConnectTimeout.TotalSeconds:F0}s. " +
					"Nothing was listening as a server on it.", ex);
			}
			_outputStream = client;
			_isIpcFile = false;
		}
		else
		{
			_outputStream = new FileStream(_path, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.Asynchronous);
            _isIpcFile = _path.EndsWith(".arrow", StringComparison.OrdinalIgnoreCase) ||
                         _path.EndsWith(".arrowfile", StringComparison.OrdinalIgnoreCase);
		}

		_schema = BuildSchema(columns);

        if (_isIpcFile)
            _arrowFileWriter = new ArrowFileWriter(_outputStream, _schema, leaveOpen: true);
        else
    		_arrowStreamWriter = new ArrowStreamWriter(_outputStream, _schema, leaveOpen: true);
	}

	private static Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
		=> ArrowSchemaFactory.Create(columns);

	public async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		if (_arrowStreamWriter != null)
		{
			await _arrowStreamWriter.WriteEndAsync(ct);
		}
        if (_arrowFileWriter != null)
        {
            await _arrowFileWriter.WriteEndAsync(ct);
        }
	}

	public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
	{
		throw new NotSupportedException("Executing raw commands is not supported for Arrow targets.");
	}

	public async ValueTask DisposeAsync()
	{
		if (_arrowStreamWriter != null)
		{
			_arrowStreamWriter.Dispose();
			_arrowStreamWriter = null;
		}
        if (_arrowFileWriter != null)
        {
            _arrowFileWriter.Dispose();
            _arrowFileWriter = null;
        }
		if (_outputStream != null)
		{
			await _outputStream.DisposeAsync();
			_outputStream = null;
		}
	}
}
