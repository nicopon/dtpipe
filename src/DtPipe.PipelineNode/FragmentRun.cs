using System.Diagnostics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Services;

namespace DtPipe.PipelineNode;

/// <summary>
/// One run of a fragment: its dtpipe child, the named pipes of its excess edges, the relay of each edge
/// between the child's streams and the transfer handle of that edge, and the first fault that ended it.
/// It knows neither the TransportR client the handles come from nor the coordinator: it is given each
/// handle once resolved, and says how it ended.
/// </summary>
/// <remarks>
/// Cancellation is by stream rupture, never by signal - Windows has no console to deliver one to a
/// child: a fault closes the child's own stdio, which is exactly what its own <c>arrow:</c>
/// reader/writer already turns into a truncated-stream exit 1, then the child is killed after a grace
/// period if closing its streams alone did not end it.
/// </remarks>
internal sealed class FragmentRun : IAsyncDisposable
{
    private const int GracePeriodMs = 3000;

    /// <summary>How long the relays get to fail on their own once the child has died, before the fragment reports without them.</summary>
    private const int RelayDrainAfterChildFailureMs = 1000;

    private readonly PipelineNodeOptions _options;
    private readonly ILogger _logger;
    private Process _child = null!;

    /// <summary>
    /// One server pipe per excess edge (the second-and-later edge of a direction), keyed by
    /// <see cref="EdgeBinding.Alias"/> — empty for a fragment with at most one inbound and one
    /// outbound edge, which still rides stdin/stdout exactly as before J2.
    /// </summary>
    private Dictionary<string, NamedPipeServerStream> _pipes = new(StringComparer.Ordinal);

    private readonly List<Task> _relayTasks = new();
    private readonly Dictionary<string, long> _rowCounts = new();
    private readonly object _rowCountsLock = new();
    private int _faulted;

    /// <summary>Completed when the child exits with a non-zero code, whatever its relays are doing.</summary>
    private readonly TaskCompletionSource _childFailed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _ruptured;
    private string? _firstFault;
    private FaultOrigin? _faultOrigin;

    private int _wiredCount;
    /// <summary>Wire pushes received, counted on arrival: the coordinator's own view of "fully wired".</summary>
    private int _wireRequests;

    public FragmentRun(PipelineNodeOptions options, ILogger logger)
    {
        _options = options;
        _logger = logger;
    }

    public int ChildProcessId => _child.Id;
    public bool ChildHasExited => _child.HasExited;
    public IReadOnlyDictionary<string, long> RowCounts { get { lock (_rowCountsLock) return new Dictionary<string, long>(_rowCounts); } }

    /// <summary>The counts as the relays write them, for the report that sends them as they are.</summary>
    internal Dictionary<string, long> LiveRowCounts => _rowCounts;

    public string? FirstFault => _firstFault;
    public FaultOrigin? Origin => _faultOrigin;

    /// <summary>True once <see cref="Launch"/> has spawned the child - before that, <see cref="ChildProcessId"/> throws.</summary>
    public bool IsLaunched => _child is not null;

    /// <summary>The Wire requests received so far.</summary>
    public int WireRequests => Volatile.Read(ref _wireRequests);

    /// <summary>Spawns the child and watches it.</summary>
    public void Launch()
    {
        (_child, _pipes) = LaunchChild(_options, _logger);
        WatchChild();
    }

    /// <summary>Counts a Wire request on arrival, before its handle is awaited.</summary>
    public void NoteWireRequested() => Interlocked.Increment(ref _wireRequests);

    /// <summary>Starts relaying the edge <paramref name="alias"/> through <paramref name="handle"/>; true when it was the last edge declared.</summary>
    public bool Wire(string alias, object handle)
    {
        var edge = _options.Edges.First(e => e.Alias == alias);
        if (edge.Direction == EdgeDirection.Outbound)
            lock (_relayTasks) _relayTasks.Add(Task.Run(() => RelayOutboundAsync(alias, (Send<byte[]>)handle)));
        else
            lock (_relayTasks) _relayTasks.Add(Task.Run(() => RelayInboundAsync(alias, (Receive<byte[]>)handle)));
        return Interlocked.Increment(ref _wiredCount) == _options.Edges.Count;
    }

    /// <summary>Short and random, never a readable composed name (alias + run id): the pipe name
    /// joins <c>$TMPDIR/CoreFxPipe_</c> on Unix, and a long <c>TMPDIR</c> (macOS routinely runs
    /// ~60 characters) already leaves little of the 104-character <c>sun_path</c> budget spare.</summary>
    private static string NewPipeName() => Guid.NewGuid().ToString("N")[..10];

    private static (Process Child, Dictionary<string, NamedPipeServerStream> Pipes) LaunchChild(
        PipelineNodeOptions options, ILogger logger)
    {
        var inbound = options.Edges.Where(e => e.Direction == EdgeDirection.Inbound).ToList();
        var outbound = options.Edges.Where(e => e.Direction == EdgeDirection.Outbound).ToList();

        // The first edge of each direction keeps riding stdin/stdout - the path already proven;
        // only an excess edge (a join's second source, a fan-out's second sink) needs a pipe of
        // its own, created here as server before the child that connects to it as client starts.
        var pipes = new Dictionary<string, NamedPipeServerStream>(StringComparer.Ordinal);
        var bindInputPairs = new List<string>();
        var bindOutputPairs = new List<string>();

        foreach (var edge in inbound.Skip(1))
        {
            var pipeName = NewPipeName();
            pipes[edge.Alias] = new NamedPipeServerStream(pipeName, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            // AliasBindingApplier prepends its own 'arrow:' to this location - adding it here too
            // would double it into 'arrow:arrow:pipe://...' and fail closed with "file not found".
            bindInputPairs.Add($"{edge.Alias}=pipe://{pipeName}");
        }
        foreach (var edge in outbound.Skip(1))
        {
            var pipeName = NewPipeName();
            pipes[edge.Alias] = new NamedPipeServerStream(pipeName, PipeDirection.In, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            bindOutputPairs.Add($"{edge.Alias}=pipe://{pipeName}");
        }

        var psi = new ProcessStartInfo
        {
            FileName = options.DtPipeExecutable,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var (name, value) in options.ChildEnvironment ?? new Dictionary<string, string>())
            psi.Environment[name] = value;
        psi.ArgumentList.Add("--job");
        psi.ArgumentList.Add(options.FragmentJobPath);
        if (bindInputPairs.Count > 0) { psi.ArgumentList.Add("--bind-input"); psi.ArgumentList.Add(string.Join(",", bindInputPairs)); }
        if (bindOutputPairs.Count > 0) { psi.ArgumentList.Add("--bind-output"); psi.ArgumentList.Add(string.Join(",", bindOutputPairs)); }

        // The fragment's own primary 'arrow:-' already means the child's own stdin/stdout, piped
        // to this node rather than inherited (AliasBindingApplier); no binding flag is needed for
        // it. An excess edge's 'arrow:-' is overridden above instead.
        var process = new Process { StartInfo = psi, EnableRaisingEvents = true };
        process.Start();
        _ = DrainStderrAsync(process, logger);

        // An edge-less direction still has to be a live pipe (never the node's own stdio), but
        // nothing ever wires it: close it, or drain it, so the child cannot block writing to or
        // reading from a pipe nobody is ever going to service.
        if (inbound.Count == 0)
            process.StandardInput.Close();
        if (outbound.Count == 0)
            _ = DrainAsync(process.StandardOutput.BaseStream, logger, "stdout");

        return (process, pipes);
    }

    /// <summary>
    /// Races the excess edge's own <see cref="NamedPipeServerStream.WaitForConnectionAsync()"/>
    /// against the child's own exit: without this, a child that dies before ever connecting to its
    /// bound pipe (an invalid job, a binding error) would block this relay task, and so this whole
    /// node, forever - nothing else was ever going to connect to a server nobody dials into after
    /// its only prospective client is dead.
    /// </summary>
    private async Task<Stream> ConnectPipeAsync(NamedPipeServerStream pipe, string alias)
    {
        var childExited = _child.WaitForExitAsync();
        var connected = pipe.WaitForConnectionAsync();
        if (await Task.WhenAny(connected, childExited) == childExited)
            throw new IOException($"child dtpipe exited before connecting to the named pipe for edge '{alias}'.");
        await connected;
        return pipe;
    }

    private static async Task DrainStderrAsync(Process process, ILogger logger)
    {
        string? firstLine = null;
        string? line;
        try
        {
            while ((line = await process.StandardError.ReadLineAsync()) is not null)
            {
                firstLine ??= line;
                logger.LogDebug("child stderr: {Line}", line);
            }
        }
        catch (ObjectDisposedException) { /* process/stream torn down concurrently — nothing left to drain */ }
        if (firstLine is not null)
            logger.LogInformation("child's first stderr line: {Line}", firstLine);
    }

    private static async Task DrainAsync(Stream stream, ILogger logger, string label)
    {
        var buffer = new byte[4096];
        try
        {
            while (await stream.ReadAsync(buffer) > 0) { }
        }
        catch (Exception ex) when (ex is ObjectDisposedException or IOException)
        {
            logger.LogDebug("draining child's {Label} ended: {Message}", label, ex.Message);
        }
    }

    private async Task RelayOutboundAsync(string alias, Send<byte[]> send)
    {
        var counter = new ArrowIpcRowCounter();
        var buffer = new byte[_options.ReadChunkBytes];
        var sending = false;
        try
        {
            var stream = _pipes.TryGetValue(alias, out var pipe)
                ? await ConnectPipeAsync(pipe, alias)
                : _child.StandardOutput.BaseStream;

            while (true)
            {
                int read = await stream.ReadAsync(buffer);
                if (read == 0) break;
                var chunk = buffer.AsSpan(0, read).ToArray();
                counter.Feed(chunk);
                sending = true;
                await send.SendAsync(chunk);
                sending = false;
            }

            await _child.WaitForExitAsync();
            if (_child.ExitCode == 0)
            {
                sending = true;
                await send.CompleteAsync();
            }
            else
                Fault(FaultOrigin.Local, $"outbound edge '{alias}': child dtpipe exited {_child.ExitCode}");
        }
        catch (ObjectDisposedException) when (sending)
        {
            // The handle itself was disposed under the call: the transfer was torn down, the same
            // as TransferFailedException - not a local failure worth naming as the run's cause.
            Fault(FaultOrigin.Remote, $"outbound edge '{alias}': the transfer was torn down");
        }
        catch (Exception ex) when (ex is TransferFailedException or OperationCanceledException)
        {
            // TransferFailedException is the documented contract for SendAsync/CompleteAsync. An
            // OperationCanceledException here is never this code's own doing - no cancellation
            // token reaches SendAsync - so it can only be the transfer failing underneath it, the
            // same as TransferFailedException.
            Fault(FaultOrigin.Remote, $"outbound edge '{alias}': {ex.Message}");
        }
        catch (Exception ex)
        {
            // Anything else - reading the child's own stdout failed locally.
            Fault(FaultOrigin.Local, $"outbound edge '{alias}': {ex.Message}");
        }
        finally
        {
            lock (_rowCountsLock) _rowCounts[alias] = counter.RowCount;
            if (_pipes.TryGetValue(alias, out var ownPipe)) { try { await ownPipe.DisposeAsync(); } catch { /* best-effort */ } }
            try { await send.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    private async Task RelayInboundAsync(string alias, Receive<byte[]> receive)
    {
        var counter = new ArrowIpcRowCounter();
        try
        {
            var stream = _pipes.TryGetValue(alias, out var pipe)
                ? await ConnectPipeAsync(pipe, alias)
                : _child.StandardInput.BaseStream;

            await foreach (var chunk in receive.ReceiveAsync())
            {
                counter.Feed(chunk);
                await stream.WriteAsync(chunk);
            }
            await stream.FlushAsync();
            await receive.WaitForCompletionAsync();
        }
        catch (Exception ex) when (ex is TransferFailedException or OperationCanceledException)
        {
            // Thrown by ReceiveAsync's own enumeration or WaitForCompletionAsync: the child was
            // still fine, the transfer itself failed. OperationCanceledException here is never this
            // code's own doing either, for the same reason as the outbound side.
            Fault(FaultOrigin.Remote, $"inbound edge '{alias}': {ex.Message}");
        }
        catch (Exception ex)
        {
            // Anything else - writing to the child's own stdin failed locally (a dead child closes
            // its end of the pipe first).
            Fault(FaultOrigin.Local, $"inbound edge '{alias}': {ex.Message}");
        }
        finally
        {
            lock (_rowCountsLock) _rowCounts[alias] = counter.RowCount;
            // Never synthesises dtpipe's own end-of-stream marker: on success it was already part of
            // the relayed bytes, on failure the child's arrow: reader turns this truncation into its
            // own EndOfStreamException — the propagation this node adds is exactly one closed stream,
            // the primary edge's real stdin or an excess edge's own named pipe, never the other one.
            if (_pipes.TryGetValue(alias, out var ownPipe)) { try { await ownPipe.DisposeAsync(); } catch { /* already closed */ } }
            else { try { _child.StandardInput.Close(); } catch { /* already closed */ } }
            try { await receive.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    /// <summary>
    /// Records only the first fault: both origin and message come from that single winning call, so
    /// a second relay task's own fault (this fragment can have one inbound and one outbound edge)
    /// never mixes its origin with the first one's message. Every fault ruptures the child at once.
    /// </summary>
    internal void Fault(FaultOrigin origin, string reason)
    {
        if (Interlocked.CompareExchange(ref _firstFault, reason, null) is null)
        {
            _faultOrigin = origin;
            _logger.LogError("Node faulted ({Origin}): {Reason}", origin, reason);
        }
        Interlocked.Exchange(ref _faulted, 1);
        Rupture();
    }

    /// <summary>
    /// Closes the child's stdin and every named pipe, then kills the child if it has not exited
    /// within the grace period - once, at the first fault. It cannot wait for the relays to finish:
    /// an outbound relay blocked reading a child that has nothing to write (an aggregate over a slow
    /// source) finishes only when that child's stdout closes, which is what the kill brings about.
    /// Closes every pipe regardless of whether its own relay task already did (a harmless second
    /// dispose): an excess edge never wired has no relay task, and its pipe would otherwise sit
    /// listening for a client that is never coming.
    /// </summary>
    private void Rupture()
    {
        var child = _child;
        if (child is null || Interlocked.Exchange(ref _ruptured, 1) != 0) return;

        try { child.StandardInput.Close(); } catch { }
        foreach (var pipe in _pipes.Values) { try { pipe.Dispose(); } catch { } }

        _ = Task.Run(async () =>
        {
            try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromMilliseconds(GracePeriodMs)); }
            catch (TimeoutException) { try { child.Kill(entireProcessTree: true); } catch { } }
            catch (Exception) { /* disposed with the node: nothing left to kill */ }
        });
    }

    /// <summary>
    /// Watches the child independently of the relays: a child that exits non-zero is this fragment's
    /// own failure from that instant. A relay only notices it on its next read or write, which a
    /// fragment waiting on a silent peer never makes, so without this the fragment would never report.
    /// </summary>
    private void WatchChild()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await WaitForChildExitAsync(CancellationToken.None);
                var code = _child.ExitCode;
                if (code == 0) return;
                Fault(FaultOrigin.Local, $"child dtpipe exited {code}");
                _childFailed.TrySetResult();
            }
            catch (Exception) { /* disposed with the node: nothing left to watch */ }
        });
    }

    /// <summary>
    /// Waits for every wired edge to finish relaying - a fault has already ruptured the child - then
    /// for the child itself. Returns the child's own exit code, or 1 if the fragment faulted. A relay
    /// still waiting on its peer once the child has failed gets a short delay to fail on its own, then
    /// is left behind: the coordinator's teardown ends its transfer.
    /// </summary>
    public async Task<int> RunToCompletionAsync(CancellationToken ct = default)
    {
        Task[] tasks;
        lock (_relayTasks) tasks = _relayTasks.ToArray();
        var relays = Task.WhenAll(tasks);
        if (await Task.WhenAny(relays, _childFailed.Task) != relays)
            await Task.WhenAny(relays, Task.Delay(RelayDrainAfterChildFailureMs, ct));

        if (_faulted != 0)
        {
            Rupture();
            try { _child.StandardOutput.Close(); } catch { }
        }

        await WaitForChildExitAsync(ct);
        return _faulted != 0 ? Math.Max(_child.ExitCode, 1) : _child.ExitCode;
    }

    /// <summary>
    /// <c>Process.WaitForExitAsync</c> alone can stay pending after the process has exited, which would
    /// hold the fragment's <c>Exited</c> report - and the run's verdict - back for good. Polling
    /// <c>HasExited</c> beside it bounds that.
    /// </summary>
    private async Task WaitForChildExitAsync(CancellationToken ct)
    {
        var exited = _child.WaitForExitAsync(ct);
        while (!exited.IsCompleted && !_child.HasExited)
            await Task.WhenAny(exited, Task.Delay(ExitPollMs, ct));
        if (exited.IsFaulted || exited.IsCanceled) await exited;
    }

    private const int ExitPollMs = 200;

    public ValueTask DisposeAsync()
    {
        // A launch that could not start its child leaves nothing to tear down.
        if (_child is null) return ValueTask.CompletedTask;

        if (!_child.HasExited)
        {
            try { _child.Kill(entireProcessTree: true); } catch { }
        }
        _child.Dispose();
        foreach (var pipe in _pipes.Values) { try { pipe.Dispose(); } catch { } }
        return ValueTask.CompletedTask;
    }
}
