using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>
/// Logs the bytes the hub forwards; the coordinator has no metrics story yet.
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
}
