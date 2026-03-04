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
        // Unwrap AggregateException
        if (ex is AggregateException agg && agg.InnerException != null)
            return DefaultIsTransient(agg.InnerException);

        // 1. Known transient exception types (by type name to avoid hard dependency on provider packages)
        var typeName = ex.GetType().Name;
        if (typeName is "NpgsqlException" or "PostgresException")
        {
            // Npgsql exposes IsTransient property
            var isTransientProp = ex.GetType().GetProperty("IsTransient");
            if (isTransientProp != null)
                return (bool)(isTransientProp.GetValue(ex) ?? false);
        }

        if (typeName is "SqlException")
        {
            // SqlClient exposes Number property with specific error codes
            var numberProp = ex.GetType().GetProperty("Number");
            if (numberProp != null)
            {
                var number = (int)(numberProp.GetValue(ex) ?? 0);
                // Known transient SQL Server error numbers
                return number is -2 or 20 or 64 or 233 or 10053 or 10054 or 10060 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920 or 4060 or 4221 or 40143 or 40540;
            }
        }

        if (typeName is "OracleException")
        {
            var numberProp = ex.GetType().GetProperty("Number");
            if (numberProp != null)
            {
                var number = (int)(numberProp.GetValue(ex) ?? 0);
                // ORA-03113: end-of-file on communication channel
                // ORA-03135: connection lost contact
                // ORA-12170: TNS connect timeout
                // ORA-12571: TNS packet writer failure
                return number is 3113 or 3135 or 12170 or 12571;
            }
        }

        // 2. Standard .NET transient exceptions
        if (ex is TimeoutException or OperationCanceledException)
            return true;

        if (ex is System.IO.IOException)
            return true;

        if (ex is System.Net.Sockets.SocketException)
            return true;

        // 3. Fallback: text-based detection for unknown providers (last resort)
        var msg = ex.Message;
        return msg.Contains("timeout", StringComparison.OrdinalIgnoreCase) ||
               msg.Contains("deadlock", StringComparison.OrdinalIgnoreCase) ||
               msg.Contains("broken pipe", StringComparison.OrdinalIgnoreCase);
    }
}
