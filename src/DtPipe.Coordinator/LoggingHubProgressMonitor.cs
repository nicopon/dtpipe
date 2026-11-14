using Microsoft.Extensions.Logging;
using TransportR.Hub;

namespace DtPipe.Coordinator;

/// <summary>
/// Logs what the hub announces: the bytes it forwards (at trace level, the coordinator has no metrics
/// story yet), and, at information level, each write stream it takes on and each write-transport probe
/// it admits. The hub logs the same two events itself, below the level the server's filter lets through.
/// </summary>
public class LoggingHubProgressMonitor : IHubProgressMonitor
{
    private readonly ILogger<LoggingHubProgressMonitor> _logger;

    public LoggingHubProgressMonitor(ILogger<LoggingHubProgressMonitor> logger)
    {
        _logger = logger;
    }

    public void OnBytesTransferred(long bytes) =>
        _logger.LogTrace("{Bytes} bytes transferred", bytes);

    public void OnWriteStreamOpened(WriteStreamOpened opened) =>
        _logger.LogInformation(
            "Transfer {TransferId}: client {ClientId} writes its {Stream} stream over {Transport}",
            opened.TransferId, opened.ClientId, opened.Stream, opened.Transport);

    public void OnWriteTransportProbed(WriteTransportProbed probed) =>
        _logger.LogInformation(
            "Client {ClientId} probed its write path: {Transport} gets through",
            probed.ClientId, probed.Transport);
}
