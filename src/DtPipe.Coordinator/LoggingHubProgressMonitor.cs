using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>
/// Satisfies <c>SignalRDataServer</c>'s requirement for an <see cref="IHubProgressMonitor"/> -
/// without one registered, the hub accepts a transfer and then returns 500 on every stream request
/// instead of forwarding bytes. Logs; the coordinator has no metrics story yet.
/// </summary>
public class LoggingHubProgressMonitor : IHubProgressMonitor
{
    private readonly ILogger<LoggingHubProgressMonitor> _logger;

    public LoggingHubProgressMonitor(ILogger<LoggingHubProgressMonitor> logger)
    {
        _logger = logger;
    }

    public void InitializeTransfer(string transferId) =>
        _logger.LogDebug("Transfer {TransferId} initialized", transferId);

    public void OnBytesTransferred(long bytes) =>
        _logger.LogTrace("{Bytes} bytes transferred", bytes);

    public void CompleteTransfer(string transferId) =>
        _logger.LogDebug("Transfer {TransferId} completed", transferId);
}
