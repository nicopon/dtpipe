using DtPipe.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DtPipe.Services;

/// <summary>
/// Handles metrics saving and memory logging.
/// </summary>
internal sealed class MetricsService
{
    private readonly IExportObserver _observer;
    private readonly ILogger<MetricsService> _logger;

    public MetricsService(IExportObserver observer, ILogger<MetricsService> logger)
    {
        _observer = observer;
        _logger = logger;
    }

    public async Task SaveMetricsAsync(IExportProgress progress, string? metricsPath, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(metricsPath)) return;

        var metrics = progress.GetMetrics();
        try
        {
            var json = System.Text.Json.JsonSerializer.Serialize(metrics,
                new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            await File.WriteAllTextAsync(metricsPath, json, ct);
            _observer.LogMessage($"   [grey]Metrics saved to: {metricsPath}[/]");
        }
        catch (Exception ex)
        {
            _observer.LogWarning($"Failed to save metrics: {ex.Message}");
        }
    }

    public void LogMemoryUsage()
    {
        var managedMemory = GC.GetTotalMemory(false) / 1024 / 1024;
        var totalMemory = System.Diagnostics.Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024;

        _logger.LogDebug("Memory Stats: Managed={Managed}MB, WorkingSet={Total}MB", managedMemory, totalMemory);
    }
}
