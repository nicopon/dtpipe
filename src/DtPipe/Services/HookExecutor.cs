using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;

namespace DtPipe.Services;

/// <summary>
/// Executes pre/post/error/finally hooks on a writer.
/// </summary>
public sealed class HookExecutor
{
    private readonly IExportObserver _observer;
    private readonly ILogger<HookExecutor> _logger;

    public HookExecutor(IExportObserver observer, ILogger<HookExecutor> logger)
    {
        _observer = observer;
        _logger = logger;
    }

    public async Task ExecuteAsync(IDataWriter writer, string hookName, string? command, CancellationToken ct, TimeSpan? timeout = null)
    {
        if (string.IsNullOrWhiteSpace(command)) return;

        _observer.OnHookExecuting(hookName, command);

        try
        {
            if (timeout.HasValue)
            {
                using var hookCts = new CancellationTokenSource(timeout.Value);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, hookCts.Token);
                await writer.ExecuteCommandAsync(command, linkedCts.Token);
            }
            else
            {
                await writer.ExecuteCommandAsync(command, ct);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hook '{HookName}' failed: {Command}", hookName, command);
            throw;
        }
    }
}
