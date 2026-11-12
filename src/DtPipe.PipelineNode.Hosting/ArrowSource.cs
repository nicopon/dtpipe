using System.IO.Pipes;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>What a source is asked for when a reader connects: the source and the connection. The request of a run will join it.</summary>
public sealed class ArrowSourceContext
{
    internal ArrowSourceContext(string sourceId, int connection)
    {
        SourceId = sourceId;
        Connection = connection;
    }

    /// <summary>The id the source was registered under.</summary>
    public string SourceId { get; }

    /// <summary>The number of the connection since the host started, from 1.</summary>
    public int Connection { get; }
}

/// <summary>Offers rows produced in .NET to the dtpipe children of a node.</summary>
public static partial class ArrowSourceExtensions
{
    /// <summary>The longest socket path a Unix domain socket takes (<c>sun_path</c> of macOS, with its terminator).</summary>
    internal const int MaxSocketPathLength = 103;

    /// <summary>
    /// Offers <paramref name="id"/> as a source brick whose rows <paramref name="read"/> produces, in the fixed
    /// <paramref name="schema"/>. Every connection of a dtpipe child to the source's socket runs <paramref name="read"/> once; the
    /// server disposes each batch it yields once sent, and stops enumerating when the child leaves. A producer that throws
    /// ends the stream without its end marker, so the run fails instead of loading part of the rows.
    /// </summary>
    /// <param name="node">The node to add the source to.</param>
    /// <param name="id">The brick's id: letters, digits, '-' and '_'. It names the variable that carries the socket's path.</param>
    /// <param name="title">The brick's title, as the coordinator shows it.</param>
    /// <param name="schema">The schema of every batch the source yields. It is written first and never depends on the data.</param>
    /// <param name="read">Produces the batches of one connection.</param>
    /// <param name="description">The brick's description.</param>
    /// <param name="socketDirectory">Where the socket lives; the temporary directory when left out.</param>
    public static DtPipeNodeBuilder AddArrowSource(this DtPipeNodeBuilder node, string id, string title, Schema schema,
        Func<ArrowSourceContext, CancellationToken, IAsyncEnumerable<RecordBatch>> read, string? description = null,
        string? socketDirectory = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(read);
        if (!SourceId().IsMatch(id))
            throw new ArgumentException($"Source id '{id}': letters, digits, '-' and '_' only, starting with a letter.", nameof(id));

        var socket = SocketPath(id, socketDirectory);
        if (!OperatingSystem.IsWindows() && socket.Length > MaxSocketPathLength)
            throw new ArgumentException(
                $"Source '{id}': the socket path is {socket.Length} characters, a Unix socket takes {MaxSocketPathLength}. Pass a shorter socketDirectory.",
                nameof(socketDirectory));

        // The socket path is a variable of the node, never part of the brick: the brick's text is its version, and a path
        // that differs by process would change it. The coordinator learns the variable's name and nothing of its value.
        var variable = "DTPIPE_SRC_" + id.ToUpperInvariant().Replace('-', '_');
        var branch = new JsonObject { ["input"] = "arrow:pipe://${{" + variable + "}}" };
        node.WithVariable(variable, socket, $"Local Arrow socket of the source '{id}'");
        node.AddBrick(id, BrickKind.Source, title, branch.ToJsonString(), description);
        node.Services.AddSingleton<IHostedService>(sp =>
            new ArrowSocketServer(id, socket, schema, read, sp.GetRequiredService<ILoggerFactory>().CreateLogger($"DtPipe.ArrowSource.{id}")));
        return node;
    }

    /// <summary>
    /// Where <see cref="AddArrowSource"/> puts the socket of the source <paramref name="id"/> in this process. Reading it is for a
    /// host that wants to point a dtpipe at its own socket; the brick never carries it.
    /// </summary>
    public static string SocketPath(string id, string? socketDirectory = null) =>
        // One folder per process, made private by the server: every source of the process has its socket in it.
        Path.Combine(Path.GetFullPath(socketDirectory ?? Path.GetTempPath()), $"dtps-{Environment.ProcessId}", $"{id}.sock");

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_-]*$")]
    private static partial Regex SourceId();
}

/// <summary>
/// Serves a source as an Arrow IPC stream on a Unix domain socket: a connection is a request to read, and nothing else tells the
/// service when dtpipe wants rows, so each connection runs the producer once.
/// </summary>
/// <remarks>
/// The socket listens from the constructor on. A host builds every hosted service before it starts any, so the node that
/// announces the brick reading this socket never runs before it exists, whatever the registration order. The socket lives in a
/// folder of the current user's alone: the rows are not for any other user of the machine.
/// </remarks>
internal sealed class ArrowSocketServer : BackgroundService
{
    private readonly string _sourceId;
    private readonly Schema _schema;
    private readonly byte[] _schemaBytes;
    private readonly Func<ArrowSourceContext, CancellationToken, IAsyncEnumerable<RecordBatch>> _read;
    private readonly ILogger _logger;
    private readonly HashSet<Task> _inFlight = [];
    private readonly string? _folder;
    private NamedPipeServerStream? _next;

    public ArrowSocketServer(string sourceId, string path, Schema schema,
        Func<ArrowSourceContext, CancellationToken, IAsyncEnumerable<RecordBatch>> read, ILogger logger)
    {
        (_sourceId, Path, _schema, _read, _logger) = (sourceId, path, schema, read, logger);
        _schemaBytes = Serialize(schema);
        if (!OperatingSystem.IsWindows())
        {
            // The socket's folder is the current user's alone, set before the socket exists: a socket's own mode follows the umask,
            // and a chmod after the bind would leave a moment in which another user could connect.
            _folder = System.IO.Path.GetDirectoryName(path)!;
            const UnixFileMode Private = UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute;
            System.IO.Directory.CreateDirectory(_folder, Private);
            File.SetUnixFileMode(_folder, Private);
            // A socket file left by a process that died makes bind fail.
            if (File.Exists(path)) File.Delete(path);
        }
        _next = NewInstance();
    }

    public string Path { get; }

    private NamedPipeServerStream NewInstance() =>
        new(Path, PipeDirection.Out, NamedPipeServerStream.MaxAllowedServerInstances, PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.CurrentUserOnly);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connection = 0;
        _logger.LogInformation("Serving the source '{Source}' on {Path}", _sourceId, Path);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var pipe = _next!;
                _next = null;
                try
                {
                    await pipe.WaitForConnectionAsync(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    await pipe.DisposeAsync();
                    break;
                }
                catch (IOException ex)
                {
                    // A client that left before the accept completed: this instance is spent, the next one serves.
                    _logger.LogWarning(ex, "A connection to the source '{Source}' failed before it was served", _sourceId);
                    await pipe.DisposeAsync();
                    _next = NewInstance();
                    continue;
                }

                // The next instance listens before this one is handed over: on a Unix socket the file lives as long as an
                // instance of the server does.
                _next = NewInstance();
                var task = Task.Run(() => ServeAsync(pipe, ++connection, stoppingToken), CancellationToken.None);
                lock (_inFlight) _inFlight.Add(task);
                _ = task.ContinueWith(t => { lock (_inFlight) _inFlight.Remove(t); }, TaskScheduler.Default);
            }
        }
        catch (Exception ex)
        {
            // A source that cannot listen is the source's problem; it never stops the service that hosts it.
            _logger.LogError(ex, "The source '{Source}' stopped serving", _sourceId);
        }
        finally
        {
            _next?.Dispose();
            _next = null;
        }

        Task[] running;
        lock (_inFlight) running = [.. _inFlight];
        await Task.WhenAll(running);
    }

    public override void Dispose()
    {
        _next?.Dispose();
        base.Dispose();
        // Removed only when the last source of the process has left it.
        try { if (_folder is not null) System.IO.Directory.Delete(_folder); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }

    private async Task ServeAsync(NamedPipeServerStream pipe, int connection, CancellationToken ct)
    {
        var batches = 0;
        var rows = 0L;
        string ending;
        await using (pipe)
        {
            try
            {
                using var writer = new ArrowStreamWriter(pipe, _schema, leaveOpen: true);
                // The schema goes out before the producer runs, so the reader's open never waits on the producer.
                await Send(() => writer.WriteStartAsync(ct));
                await Send(() => pipe.FlushAsync(ct));
                var verified = _schema;
                await foreach (var batch in _read(new ArrowSourceContext(_sourceId, connection), ct).WithCancellation(ct))
                {
                    using (batch)
                    {
                        if (!ReferenceEquals(batch.Schema, verified))
                        {
                            if (!Serialize(batch.Schema).AsSpan().SequenceEqual(_schemaBytes))
                                throw new InvalidOperationException($"A batch of the source '{_sourceId}' does not have the schema the source declared.");
                            verified = batch.Schema;
                        }
                        batches++;
                        await Send(() => writer.WriteRecordBatchAsync(batch, ct));
                        await Send(() => pipe.FlushAsync(ct));
                        rows += batch.Length;
                    }
                }
                // The end marker means the producer finished: a stream that closes without it fails the run.
                await Send(() => writer.WriteEndAsync(ct));
                await Send(() => pipe.FlushAsync(ct));
                ending = "end of stream";
            }
            catch (ReaderLeftException ex)
            {
                // The reader left (a limit, a sample, a run that failed downstream): leaving the loop disposes the producer's
                // enumerator, which stops it calling whatever it reads.
                ending = $"reader left ({ex.InnerException!.Message})";
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                ending = "service stopping";
            }
            catch (Exception ex)
            {
                ending = "producer failed, no end of stream";
                _logger.LogError(ex, "The source '{Source}' failed on connection {Connection}; the stream ends without its end marker", _sourceId, connection);
            }
        }
        _logger.LogInformation("Source '{Source}' connection {Connection}: {Batches} batch(es), {Rows} row(s), {Ending}",
            _sourceId, connection, batches, rows, ending);
    }

    /// <summary>A write that fails with an I/O error means the reader is gone; any other error is the producer's.</summary>
    private static async Task Send(Func<Task> write)
    {
        try
        {
            await write();
        }
        catch (IOException ex)
        {
            throw new ReaderLeftException(ex);
        }
    }

    private static byte[] Serialize(Schema schema)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true)) writer.WriteStart();
        return stream.ToArray();
    }

    private sealed class ReaderLeftException(IOException inner) : Exception(inner.Message, inner);
}
