using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The hosts set aside over a real Redis (<see cref="RedisTestBed"/>), under a prefix of each test's own.</summary>
public sealed class RedisIgnoredHostStoreTests : IDisposable
{
    private static readonly IReadOnlyDictionary<string, string> NoLabels = new Dictionary<string, string>();
    private static readonly NodeAnnouncement Host = new("proposed", "a host", [], NodeRole.Data, []);

    private readonly RedisTestBed _bed = new();

    public void Dispose() => _bed.Dispose();

    // One start of the coordinator: its own connection, the same prefix.
    private EnrollmentQueue Start(TimeProvider time) => new(new RedisIgnoredHostStore(_bed.Open(initialize: false)), time);

    [Fact]
    public void A_Host_Set_Aside_Stays_Out_After_A_Restart_Until_Its_Lifetime_Ends()
    {
        RedisTestBed.Need();
        var time = new FakeTimeProvider();
        Start(time).Ignore("s");

        time.Advance(TimeSpan.FromHours(20));
        var restarted = Start(time);
        Assert.Equal(Proposal.Ignored, restarted.Propose("s", Host, NoLabels));

        time.Advance(TimeSpan.FromHours(5));
        Assert.Equal(Proposal.Recorded, Start(time).Propose("s", Host, NoLabels));
    }

    [Fact]
    public void The_Set_Is_Bounded_And_The_Soonest_To_Be_Let_Back_In_Goes_First()
    {
        RedisTestBed.Need();
        var store = new RedisIgnoredHostStore(_bed.Open());
        var now = new DateTimeOffset(2026, 10, 18, 12, 0, 0, TimeSpan.Zero);

        foreach (var (subject, hours) in new[] { ("a", 1), ("b", 2), ("c", 3) }) store.Add(subject, now.AddHours(hours), now, max: 2);

        Assert.Equal(["b", "c"], store.Load(now).Keys.Order());
        // What expired is forgotten by the next write.
        store.Add("d", now.AddHours(10), now.AddHours(2.5), max: 2);
        Assert.Equal(["c", "d"], store.Load(now).Keys.Order());
    }

    [Fact]
    public void A_Store_That_Cannot_Be_Reached_Refuses_To_Set_A_Host_Aside_And_It_Keeps_Waiting()
    {
        RedisTestBed.Need();
        var connection = _bed.Open();
        var queue = new EnrollmentQueue(new RedisIgnoredHostStore(connection));
        queue.Propose("s", Host, NoLabels);
        // Redis goes away: the connection is closed under the store.
        connection.Dispose();

        Assert.Throws<StoreUnavailableException>(() => queue.Ignore("s"));

        Assert.Single(queue.List());
        Assert.Equal(Proposal.Unchanged, queue.Propose("s", Host, NoLabels));
    }
}
