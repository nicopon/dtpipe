using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's one connection to Redis, which the stores that keep its state borrow: none owns it, and the container disposes it. A command sent while
/// Redis is down fails at once and a command that gets no answer fails after <see cref="SyncTimeout"/>, both as
/// <see cref="StoreUnavailableException"/>. A connection string is a secret: no message here carries it, only the endpoints.
/// </summary>
public sealed class RedisConnection : IDisposable
{
    public static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(3);

    private readonly ConnectionMultiplexer _redis;

    private RedisConnection(ConnectionMultiplexer redis, RedisSettings settings)
    {
        (_redis, Db, KeyPrefix, Initialize) = (redis, redis.GetDatabase(), settings.KeyPrefix, settings.Initialize);
    }

    public IDatabase Db { get; }

    /// <summary>The prefix of every key the coordinator writes.</summary>
    public string KeyPrefix { get; }

    /// <summary>Whether a document that is absent may begin: the first start, or after Redis lost its data.</summary>
    public bool Initialize { get; }

    /// <summary>Connects, and refuses when Redis cannot be reached now: a coordinator that cannot keep its state does not start.</summary>
    public static RedisConnection Connect(RedisSettings settings, ILogger logger)
    {
        if (settings.ConnectionString is not { Length: > 0 } connection)
            throw new InvalidOperationException("Coordinator:Redis:ConnectionString is required: the Redis that keeps the coordinator's state.");
        ConfigurationOptions configuration;
        try
        {
            configuration = ConfigurationOptions.Parse(connection);
        }
        catch (ArgumentException)
        {
            // No inner exception: the parser may echo the part of the string it could not read.
            throw new InvalidOperationException("Coordinator:Redis:ConnectionString is not a Redis connection string.");
        }
        ConnectionMultiplexer redis;
        try
        {
            configuration.AbortOnConnectFail = true;
            configuration.SyncTimeout = (int)SyncTimeout.TotalMilliseconds;
            // A command sent while the connection is down fails at once instead of waiting in a backlog.
            configuration.BacklogPolicy = BacklogPolicy.FailFast;
            redis = ConnectionMultiplexer.Connect(configuration);
        }
        catch (RedisException ex)
        {
            throw new InvalidOperationException($"The coordinator could not reach Redis: {ex.GetType().Name}. Check Coordinator:Redis:ConnectionString.", ex);
        }
        logger.LogInformation("Coordinator state on Redis at {Endpoints}, keys under '{Prefix}'", Endpoints(redis), settings.KeyPrefix);
        return new RedisConnection(redis, settings);
    }

    /// <summary>
    /// Runs <paramref name="command"/> for <paramref name="document"/> (the thing that is kept, in the words of a message) and
    /// turns a failure of Redis into <see cref="StoreUnavailableException"/>. A command that timed out may still have been applied.
    /// </summary>
    public T Call<T>(string document, Func<T> command)
    {
        if (!_redis.IsConnected) throw new StoreUnavailableException($"Redis ({Endpoints(_redis)}) is not connected, so {document} cannot be kept.");
        try
        {
            return command();
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            throw new StoreUnavailableException($"Redis ({Endpoints(_redis)}) did not answer ({ex.GetType().Name}), so {document} may not have been kept.", ex);
        }
    }

    public void Dispose() => _redis.Dispose();

    private static string Endpoints(ConnectionMultiplexer redis) => string.Join(", ", redis.GetEndPoints().Select(e => e.ToString()));
}
