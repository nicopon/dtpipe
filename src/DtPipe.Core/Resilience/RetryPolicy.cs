using Microsoft.Extensions.Logging;

namespace DtPipe.Core.Resilience;

/// <summary>
/// Simple retry policy with exponential backoff for transient errors.
/// </summary>
public class RetryPolicy
{
    private readonly int _maxRetries;
    private readonly TimeSpan _initialDelay;
    private readonly ILogger _logger;
    private readonly Func<Exception, bool> _isTransient;

    public RetryPolicy(int maxRetries, TimeSpan initialDelay, ILogger logger, Func<Exception, bool>? isTransient = null)
    {
        _maxRetries = maxRetries;
        _initialDelay = initialDelay;
        _logger = logger;
        _isTransient = isTransient ?? DefaultIsTransient;
    }

    public async Task<T> ExecuteAsync<T>(Func<Task<T>> action, CancellationToken ct = default)
    {
        int attempt = 0;
        while (true)
        {
            try
            {
                return await action();
            }
            catch (Exception ex) when (attempt < _maxRetries && _isTransient(ex) && !ct.IsCancellationRequested)
            {
                attempt++;
                var delay = _initialDelay * Math.Pow(2, attempt - 1);
                _logger.LogWarning(ex, "Transient error (attempt {Attempt}/{Max}). Retrying in {Delay}ms...",
                    attempt, _maxRetries, delay.TotalMilliseconds);

                try
                {
                    await Task.Delay(delay, ct);
                }
                catch (TaskCanceledException)
                {
                    throw; // Re-throw to respect cancellation
                }
            }
        }
    }

    public async Task ExecuteAsync(Func<Task> action, CancellationToken ct = default)
    {
        await ExecuteAsync(async () => { await action(); return true; }, ct);
    }

    public async Task<T> ExecuteValueAsync<T>(Func<ValueTask<T>> action, CancellationToken ct = default)
    {
        return await ExecuteAsync(async () => await action(), ct);
    }

    public async Task ExecuteValueAsync(Func<ValueTask> action, CancellationToken ct = default)
    {
        await ExecuteAsync(async () => { await action(); return true; }, ct);
    }

    private static bool DefaultIsTransient(Exception ex)
    {
        // Common transient exceptions across database providers
        var msg = ex.Message.ToLowerInvariant();
        return msg.Contains("timeout") ||
               msg.Contains("deadlock") ||
               msg.Contains("connection") ||
               msg.Contains("network") ||
               msg.Contains("broken pipe") ||
               msg.Contains("transport") ||
               msg.Contains("io error") ||
               msg.Contains("locked") ||
               msg.Contains("busy") ||
               msg.Contains("lock") ||
               msg.Contains("stream") ||
               msg.Contains("read") ||
               msg.Contains("not open") ||
               msg.Contains("socket");
    }
}
