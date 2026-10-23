using System;
using System.Data.Common;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Adapters.Shared.Infrastructure.Retry;
using DtPipe.Core.Infrastructure.Retry;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class DatabaseRetryPolicyTests
{
    private class FakeDbException : DbException
    {
        public FakeDbException(string message) : base(message) { }
    }

    [Fact]
    public async Task NoRetryPolicy_RunsActionExactlyOnce()
    {
        int runCount = 0;
        var policy = NoRetryPolicy.Instance;

        await policy.ExecuteAsync(ct =>
        {
            runCount++;
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.Equal(1, runCount);
    }

    [Fact]
    public async Task NoRetryPolicy_PropagatesExceptionImmediately()
    {
        var policy = NoRetryPolicy.Instance;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            policy.ExecuteAsync(ct => throw new InvalidOperationException("boom"), CancellationToken.None));
    }

    [Fact]
    public async Task DatabaseRetryPolicy_RunsActionExactlyOnce_OnSuccess()
    {
        int runCount = 0;
        var policy = new DatabaseRetryPolicy(3, TimeSpan.FromMilliseconds(1));

        var result = await policy.ExecuteAsync(ct =>
        {
            runCount++;
            return Task.FromResult(42);
        }, CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Equal(1, runCount);
    }

    [Fact]
    public async Task DatabaseRetryPolicy_RetriesOnTransientExceptions_ThenSucceeds()
    {
        int runCount = 0;
        var policy = new DatabaseRetryPolicy(3, TimeSpan.FromMilliseconds(1));

        await policy.ExecuteAsync(ct =>
        {
            runCount++;
            if (runCount < 3)
            {
                throw new FakeDbException("Transient DB issue");
            }
            return Task.CompletedTask;
        }, CancellationToken.None);

        Assert.Equal(3, runCount);
    }

    [Fact]
    public async Task DatabaseRetryPolicy_RetriesOnTransientExceptions_ThenFailsAfterMaxAttempts()
    {
        int runCount = 0;
        var policy = new DatabaseRetryPolicy(3, TimeSpan.FromMilliseconds(1));

        await Assert.ThrowsAsync<FakeDbException>(() =>
            policy.ExecuteAsync(ct =>
            {
                runCount++;
                throw new FakeDbException("Permanent DB issue");
            }, CancellationToken.None));

        // 1 initial run + 3 retries = 4 total attempts
        Assert.Equal(4, runCount);
    }

    [Fact]
    public async Task DatabaseRetryPolicy_DoesNotRetry_OnNonTransientExceptions()
    {
        int runCount = 0;
        var policy = new DatabaseRetryPolicy(3, TimeSpan.FromMilliseconds(1));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            policy.ExecuteAsync(ct =>
            {
                runCount++;
                throw new ArgumentException("Invalid arguments");
            }, CancellationToken.None));

        Assert.Equal(1, runCount);
    }
}
