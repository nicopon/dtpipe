using System;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Infrastructure.Retry;

namespace DtPipe.Adapters.Shared.Infrastructure.Retry;

public class NoRetryPolicy : IRetryPolicy
{
    public static readonly NoRetryPolicy Instance = new();

    public Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken ct)
    {
        return action(ct);
    }

    public Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        return action(ct);
    }
}
