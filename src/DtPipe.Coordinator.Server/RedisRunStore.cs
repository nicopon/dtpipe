using System.Text.Json;
using StackExchange.Redis;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// <see cref="IRunStore"/> on Redis, under keys that start with the configured prefix: <c>seq</c> (the counter), <c>run:{id}</c> (a
/// document), <c>active</c> (the ids not over), <c>history</c> (the ids that are over, ranked by when they ended) and <c>owed</c>
/// (the abandonments, ranked by when they fall due). A write is one transaction, and the cut of the history a second step that
/// can be repeated. A document is the truth and the indexes follow it, so the journal heals an index a crash left behind.
/// A connection string is a secret: no message here carries it, only the endpoints.
/// </summary>
public sealed class RedisRunStore : IRunStore, IDisposable
{
    private static readonly TimeSpan SyncTimeout = TimeSpan.FromSeconds(3);

    private const string RaiseScript = """
        local current = redis.call('GET', KEYS[1])
        if current == false or tonumber(current) < tonumber(ARGV[1]) then redis.call('SET', KEYS[1], ARGV[1]) end
        return 1
        """;

    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly bool _initialize;
    private readonly RedisKey _seq, _active, _history, _owed;
    private readonly string _prefix;

    private RedisRunStore(ConnectionMultiplexer redis, RedisSettings settings)
    {
        (_redis, _db, _initialize, _prefix) = (redis, redis.GetDatabase(), settings.Initialize, settings.KeyPrefix);
        (_seq, _active, _history, _owed) = (_prefix + "seq", _prefix + "active", _prefix + "history", _prefix + "owed");
    }

    /// <summary>Connects, and refuses when Redis cannot be reached now: a coordinator that cannot journal does not start.</summary>
    public static RedisRunStore Connect(RedisSettings settings, ILogger logger)
    {
        if (settings.ConnectionString is not { Length: > 0 } connection)
            throw new InvalidOperationException("Coordinator:Redis:ConnectionString is required: the Redis that keeps the run journal.");
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
            throw new InvalidOperationException($"The run journal could not reach Redis: {ex.GetType().Name}. Check Coordinator:Redis:ConnectionString.", ex);
        }
        logger.LogInformation("Run journal on Redis at {Endpoints}, keys under '{Prefix}'", Endpoints(redis), settings.KeyPrefix);
        return new RedisRunStore(redis, settings);
    }

    public long NextSequence() => Call(() => _db.StringIncrement(_seq));

    public void RaiseSequenceTo(long floor) => Call(() =>
    {
        if (floor == 0 && !_initialize && !_db.KeyExists(_seq))
            throw new InvalidOperationException(
                "The run counter is absent and no run is kept: Redis may have lost its data, and run ids would start again. "
                + "Set Coordinator:Redis:Initialize to true to begin a new journal.");
        _db.ScriptEvaluate(RaiseScript, [_seq], [floor]);
        return 0;
    });

    public void Put(StoredRun run) => Call(() =>
    {
        var transaction = _db.CreateTransaction();
        var tasks = new List<Task>
        {
            transaction.StringSetAsync(RunKey(run.RunId), run.Document),
            run.Over
                ? transaction.SortedSetAddAsync(_history, run.RunId, run.At.ToUnixTimeMilliseconds())
                : transaction.SetAddAsync(_active, run.RunId),
        };
        if (run.Over) tasks.Add(transaction.SetRemoveAsync(_active, run.RunId));
        if (!transaction.Execute()) throw new StoreUnavailableException("The run journal's Redis refused the write.");
        Task.WaitAll([.. tasks], SyncTimeout);
        if (run.Over) Cut();
        return 0;
    });

    // The ids past the window, oldest first: their documents go with them.
    private void Cut()
    {
        var past = _db.SortedSetRangeByRank(_history, 0, -(IRunStore.Window + 1));
        if (past.Length == 0) return;
        _db.KeyDelete([.. past.Select(id => RunKey(id!))]);
        _db.SortedSetRemove(_history, past);
    }

    public StoredRuns Load() => Call(() =>
    {
        var active = _db.SetMembers(_active);
        var history = _db.SortedSetRangeByRank(_history, 0, IRunStore.Window - 1, Order.Descending);
        var documents = _db.StringGet([.. active.Concat(history).Select(id => RunKey(id!))]);
        var inFlight = new List<string>();
        var over = new List<string>();
        for (var i = 0; i < documents.Length; i++)
        {
            if (!documents[i].HasValue) continue;
            (i < active.Length ? inFlight : over).Add(documents[i]!);
        }
        return new StoredRuns(inFlight, over);
    });

    public void AddOwed(IReadOnlyCollection<OwedFragment> owed, DateTimeOffset until) => Call(() =>
    {
        if (owed.Count == 0) return 0;
        _db.SortedSetAdd(_owed, [.. owed.Select(o => new SortedSetEntry(JsonSerializer.Serialize(o), until.ToUnixTimeMilliseconds()))]);
        return 0;
    });

    public IReadOnlyList<OwedFragment> ReadOwed(DateTimeOffset now) => Call(() =>
    {
        _db.SortedSetRemoveRangeByScore(_owed, double.NegativeInfinity, now.ToUnixTimeMilliseconds());
        return (IReadOnlyList<OwedFragment>)[.. _db.SortedSetRangeByRank(_owed).Select(m => JsonSerializer.Deserialize<OwedFragment>(m.ToString())!)];
    });

    public void RemoveOwed(OwedFragment owed) => Call(() => _db.SortedSetRemove(_owed, JsonSerializer.Serialize(owed)));

    public void Dispose() => _redis.Dispose();

    private RedisKey RunKey(RedisValue id) => _prefix + "run:" + id;

    private T Call<T>(Func<T> command)
    {
        if (!_redis.IsConnected) throw new StoreUnavailableException($"The run journal's Redis ({Endpoints(_redis)}) is not connected.");
        try
        {
            return command();
        }
        catch (Exception ex) when (ex is RedisException or TimeoutException)
        {
            throw new StoreUnavailableException($"The run journal's Redis ({Endpoints(_redis)}) did not answer: {ex.GetType().Name}.", ex);
        }
    }

    private static string Endpoints(ConnectionMultiplexer redis) => string.Join(", ", redis.GetEndPoints().Select(e => e.ToString()));
}
