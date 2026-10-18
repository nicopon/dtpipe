using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The journal over a real Redis. It needs one: <c>DTPIPE_TEST_REDIS</c> names it, and <c>DTPIPE_TEST_REUSE_INFRA</c> stands for the
/// container of <c>tests/infra</c> (<c>localhost:6390</c>). Each test keys its own prefix, so none sees another's runs.
/// </summary>
public sealed class RedisRunStoreTests : IDisposable
{
    private const string InfraRedis = "localhost:6390";

    private readonly string _prefix = "test-" + Guid.NewGuid().ToString("N") + ":";
    private readonly List<RedisRunStore> _opened = [];

    public void Dispose()
    {
        foreach (var store in _opened) store.Dispose();
        if (ConnectionString() is not { } connection) return;
        using var redis = ConnectionMultiplexer.Connect(connection);
        var db = redis.GetDatabase();
        foreach (var key in redis.GetServer(redis.GetEndPoints()[0]).Keys(pattern: _prefix + "*")) db.KeyDelete(key);
    }

    private static string? ConnectionString() =>
        Environment.GetEnvironmentVariable("DTPIPE_TEST_REDIS") is { Length: > 0 } named ? named
        : Environment.GetEnvironmentVariable("DTPIPE_TEST_REUSE_INFRA") is "true" or "1" ? InfraRedis
        : null;

    private RedisRunStore Open(bool initialize = true)
    {
        var store = RedisRunStore.Connect(
            new RedisSettings(ConnectionString() ?? throw new InvalidOperationException("no Redis"), _prefix, initialize), NullLogger.Instance);
        _opened.Add(store);
        return store;
    }

    private RunJournal Journal(RedisRunStore store) => new(store, NullLogger<RunJournal>.Instance);

    private static RunView Over(string id, DateTimeOffset at, string? key = null) =>
        new(id, "plan", RunState.Succeeded, at, at, null, null, null, "p", at, LaunchedBy: "cron", IdempotencyKey: key);

    private static void NeedRedis() => Assert.SkipUnless(ConnectionString() is not null, "No Redis: set DTPIPE_TEST_REDIS, or DTPIPE_TEST_REUSE_INFRA=true with tests/infra up.");

    [Fact]
    public void An_Id_Is_Never_Given_Again_After_A_Restart()
    {
        NeedRedis();
        var journal = Journal(Open());
        Assert.Equal(["run-1", "run-2", "run-3"], new[] { journal.NextRunId(), journal.NextRunId(), journal.NextRunId() });

        Assert.Equal("run-4", Journal(Open(initialize: false)).NextRunId());
    }

    [Fact]
    public void A_Counter_That_Is_Gone_While_Runs_Remain_Is_Raised_To_Their_Highest_Id_And_Never_Lowered()
    {
        NeedRedis();
        var journal = Journal(Open());
        journal.Record(Over("run-41", DateTimeOffset.UtcNow));
        using (var redis = ConnectionMultiplexer.Connect(ConnectionString()!)) redis.GetDatabase().KeyDelete(_prefix + "seq");

        var again = Journal(Open(initialize: false));
        Assert.Equal("run-42", again.NextRunId());

        // A floor under the counter leaves it where it is.
        Open().RaiseSequenceTo(7);
        Assert.Equal("run-43", Journal(Open(initialize: false)).NextRunId());
    }

    [Fact]
    public void A_Store_With_No_Counter_And_No_Run_Refuses_To_Start_Unless_It_Is_Told_To_Initialize()
    {
        NeedRedis();

        var refused = Assert.Throws<InvalidOperationException>(() => Journal(Open(initialize: false)));
        Assert.Contains("Coordinator:Redis:Initialize", refused.Message);

        Assert.Equal("run-1", Journal(Open(initialize: true)).NextRunId());
    }

    [Fact]
    public void A_Key_Is_Found_After_A_Restart_And_A_Run_In_Flight_Is_Ended_With_Its_Key()
    {
        NeedRedis();
        var journal = Journal(Open());
        journal.Record(Over("run-1", DateTimeOffset.UtcNow, "night-1"));
        var now = DateTimeOffset.UtcNow;
        // On the store before anything else can see it: a restart finds the queued run.
        journal.Record(new RunView("run-2", "plan", RunState.Queued, now, null, null, null, null, "p", now, LaunchedBy: "cron", IdempotencyKey: "night-2"));

        var again = Journal(Open(initialize: false));

        Assert.Equal(RunState.Succeeded, again.Find(r => r.LaunchedBy == "cron" && r.IdempotencyKey == "night-1")!.State);
        var ended = again.Find(r => r.IdempotencyKey == "night-2")!;
        Assert.Equal((RunState.Error, RunJournal.RestartedRefusal), (ended.State, ended.Refusal));
        Assert.Null(again.Find(r => r.LaunchedBy == "other" && r.IdempotencyKey == "night-1"));
        // The store says both are over: a third start ends nothing.
        Assert.All(Journal(Open(initialize: false)).List(), r => Assert.True(r.IsOver));
    }

    [Fact]
    public void An_Abandonment_Owed_Survives_A_Restart_Until_It_Is_Settled_Or_Falls_Due()
    {
        NeedRedis();
        var store = Open();
        var kept = new OwedFragment("run-1", "p@n", "n");
        var gone = new OwedFragment("run-2", "q@m", "m");
        var now = DateTimeOffset.UtcNow;
        store.AddOwed([kept, gone], now.AddHours(1));
        store.AddOwed([new OwedFragment("run-3", "r@n", "n")], now.AddSeconds(-1));

        Assert.Equal([kept, gone], Open().ReadOwed(now).OrderBy(o => o.RunId));

        store.RemoveOwed(gone);
        Assert.Equal([kept], Open().ReadOwed(now));
    }

    [Fact]
    public void The_History_Keeps_The_Latest_Two_Hundred_And_Drops_The_Documents_It_Cuts()
    {
        NeedRedis();
        var journal = Journal(Open());
        var start = DateTimeOffset.UtcNow;
        for (var i = 1; i <= 205; i++) journal.Record(Over($"run-{i}", start.AddSeconds(i)));

        var again = Journal(Open(initialize: false));

        Assert.Equal(200, again.List(limit: 1000).Count);
        Assert.Null(again.Find(r => r.RunId == "run-5"));
        Assert.Equal("run-205", again.List(limit: 1).Single().RunId);
        using var redis = ConnectionMultiplexer.Connect(ConnectionString()!);
        Assert.False(redis.GetDatabase().KeyExists(_prefix + "run:run-5"));
        Assert.True(redis.GetDatabase().KeyExists(_prefix + "run:run-6"));
    }

    [Fact]
    public void A_Connection_String_Is_In_No_Message_When_Redis_Cannot_Be_Reached()
    {
        var logs = new CapturedLogger();
        var secret = "hunter2-secret";

        var failure = Assert.Throws<InvalidOperationException>(() => RedisRunStore.Connect(
            new RedisSettings($"localhost:1,password={secret},connectTimeout=300,connectRetry=1"), logs));

        Assert.DoesNotContain(secret, failure.ToString());
        Assert.DoesNotContain(secret, string.Join("\n", logs.Lines));
    }

    [Theory]
    [InlineData("redis://:hunter2-secret@localhost:1")]
    [InlineData("localhost:notaport,password=hunter2-secret")]
    public void A_Connection_String_That_Does_Not_Parse_Is_In_No_Message(string connection)
    {
        var failure = Assert.ThrowsAny<Exception>(() => RedisRunStore.Connect(new RedisSettings(connection), new CapturedLogger()));

        Assert.DoesNotContain("hunter2-secret", failure.ToString());
    }

    private sealed class CapturedLogger : ILogger
    {
        public List<string> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception) + exception);
    }
}
