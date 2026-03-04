using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using DtPipe.Core.Resilience;
using Xunit;

namespace DtPipe.Tests.Unit.Resilience;

public class RetryPolicyTransientTests
{
    // We need to access the private static method DefaultIsTransient
    private static bool InvokeDefaultIsTransient(Exception ex)
    {
        var method = typeof(RetryPolicy).GetMethod("DefaultIsTransient", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("DefaultIsTransient not found");
        return (bool)(method.Invoke(null, new object[] { ex }) ?? false);
    }

    [Fact]
    public void DefaultIsTransient_TimeoutException_ReturnsTrue()
    {
        var ex = new TimeoutException("Operation timed out");
        Assert.True(InvokeDefaultIsTransient(ex));
    }

    [Fact]
    public void DefaultIsTransient_IOException_ReturnsTrue()
    {
        var ex = new IOException("File in use");
        Assert.True(InvokeDefaultIsTransient(ex));
    }

    [Fact]
    public void DefaultIsTransient_InvalidOperationException_ReturnsFalse()
    {
        var ex = new InvalidOperationException("Invalid state");
        Assert.False(InvokeDefaultIsTransient(ex));
    }

    [Fact]
    public void DefaultIsTransient_AggregateExceptionWrappingTimeout_ReturnsTrue()
    {
        var inner = new TimeoutException("Inner timeout");
        var ex = new AggregateException("Multiple errors", inner);
        Assert.True(InvokeDefaultIsTransient(ex));
    }

    [Fact]
    public void DefaultIsTransient_ExceptionWithIsTransientProperty_ReturnsTrue()
    {
        var ex = new NpgsqlException();
        Assert.True(InvokeDefaultIsTransient(ex));
    }

    // A mock exception class to test the property extraction using Reflection
    public class MockTransientException : Exception
    {
        public bool IsTransient => true;

        // Match the rule "NpgsqlException" or "PostgresException"
        // Wait, the RetryPolicy specifically checks for NpgsqlException or PostgresException!
        // So we can't test it unless we name our class one of those...
        // Let's create a nested class with the exact name.
    }
}

// Global namespace to match exactly the required name for the test
public class NpgsqlException : Exception
{
    public bool IsTransient => true;
}
