using System;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using IRetryPolicy = DtPipe.Core.Infrastructure.Retry.IRetryPolicy;

namespace DtPipe.Adapters.Shared.Infrastructure.Retry;

public class DatabaseRetryPolicy : IRetryPolicy
{
    private readonly ResiliencePipeline _pipeline;

    public DatabaseRetryPolicy(int maxRetryAttempts = 3, TimeSpan? initialDelay = null)
    {
        var delay = initialDelay ?? TimeSpan.FromSeconds(1);

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<System.Data.Common.DbException>()
                    .Handle<TimeoutException>()
                    .Handle<System.IO.IOException>()
                    .Handle<System.Net.Http.HttpRequestException>(),
                MaxRetryAttempts = maxRetryAttempts,
                Delay = delay,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true
            })
            .Build();
    }

    public async Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken ct)
    {
        await _pipeline.ExecuteAsync(async token => await action(token), ct);
    }

    public async Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct)
    {
        return await _pipeline.ExecuteAsync(async token => await action(token), ct);
    }
}
