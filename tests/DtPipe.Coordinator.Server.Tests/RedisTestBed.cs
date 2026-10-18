using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A real Redis for a test, under a prefix of its own so that none sees another's keys. It needs one: <c>DTPIPE_TEST_REDIS</c> names it,
/// and <c>DTPIPE_TEST_REUSE_INFRA</c> stands for the container of <c>tests/infra</c> (<c>localhost:6390</c>). Disposing removes the keys.
/// </summary>
public sealed class RedisTestBed : IDisposable
{
    private const string InfraRedis = "localhost:6390";

    private readonly List<RedisConnection> _opened = [];

    public string Prefix { get; } = "test-" + Guid.NewGuid().ToString("N") + ":";

    public static string? ConnectionString() =>
        Environment.GetEnvironmentVariable("DTPIPE_TEST_REDIS") is { Length: > 0 } named ? named
        : Environment.GetEnvironmentVariable("DTPIPE_TEST_REUSE_INFRA") is "true" or "1" ? InfraRedis
        : null;

    public static void Need() => Assert.SkipUnless(ConnectionString() is not null, "No Redis: set DTPIPE_TEST_REDIS, or DTPIPE_TEST_REUSE_INFRA=true with tests/infra up.");

    /// <summary>A connection under this bed's prefix, or under <paramref name="prefix"/>; a second one stands for another start of the coordinator.</summary>
    public RedisConnection Open(bool initialize = true, string? prefix = null)
    {
        var connection = RedisConnection.Connect(
            new RedisSettings(ConnectionString() ?? throw new InvalidOperationException("no Redis"), prefix ?? Prefix, initialize), NullLogger.Instance);
        _opened.Add(connection);
        return connection;
    }

    public void Dispose()
    {
        foreach (var opened in _opened) opened.Dispose();
        if (ConnectionString() is not { } connection) return;
        using var redis = ConnectionMultiplexer.Connect(connection);
        var db = redis.GetDatabase();
        foreach (var key in redis.GetServer(redis.GetEndPoints()[0]).Keys(pattern: Prefix + "*")) db.KeyDelete(key);
    }
}
