using DtPipe.Core.Resilience;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Resilience;

public class RetryPolicyTests
{
    [Fact]
    public async Task ExecuteAsync_SucceedsFirstTime()
    {
        var policy = new RetryPolicy(3, TimeSpan.FromMilliseconds(1), NullLogger.Instance);
        int calls = 0;

        var result = await policy.ExecuteAsync(async () =>
        {
            calls++;
            await Task.Yield();
            return "ok";
        });

        Assert.Equal("ok", result);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_RetriesOnTransientError_ThenSucceeds()
    {
        var policy = new RetryPolicy(3, TimeSpan.FromMilliseconds(10), NullLogger.Instance);
        int calls = 0;

        var result = await policy.ExecuteAsync(async () =>
        {
            calls++;
            if (calls < 3)
            {
                throw new Exception("transient timeout error");
            }
            await Task.Yield();
            return "success";
        });

        Assert.Equal("success", result);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsIfMaxRetriesExceeded()
    {
        var policy = new RetryPolicy(2, TimeSpan.FromMilliseconds(10), NullLogger.Instance);
        int calls = 0;

        var ex = await Assert.ThrowsAsync<Exception>(async () =>
        {
            await policy.ExecuteAsync(async () =>
            {
                calls++;
                throw new Exception("persistent timeout error");
            });
        });

        Assert.Contains("persistent timeout error", ex.Message);
        Assert.Equal(3, calls); // Initial + 2 retries
    }

    [Fact]
    public async Task ExecuteValueAsync_WorksWithTaskReturnType()
    {
        var policy = new RetryPolicy(3, TimeSpan.FromMilliseconds(1), NullLogger.Instance);
        int calls = 0;

        await policy.ExecuteValueAsync(async () =>
        {
            calls++;
            await ValueTask.CompletedTask;
        });

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRetryOnNonTransientError()
    {
        var policy = new RetryPolicy(3, TimeSpan.FromMilliseconds(1), NullLogger.Instance);
        int calls = 0;

        var ex = await Assert.ThrowsAsync<Exception>(async () =>
        {
            await policy.ExecuteAsync(async () =>
            {
                calls++;
                throw new Exception("CRITICAL DAMAGE");
            });
        });

        Assert.Equal("CRITICAL DAMAGE", ex.Message);
        Assert.Equal(1, calls);
    }
}
