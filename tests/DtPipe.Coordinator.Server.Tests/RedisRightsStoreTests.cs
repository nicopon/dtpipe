using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The rights over a real Redis (<see cref="RedisTestBed"/>), under a prefix of each test's own.</summary>
public sealed class RedisRightsStoreTests : IDisposable
{
    private readonly RedisTestBed _bed = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "redis-rights-" + Guid.NewGuid().ToString("N"));

    public RedisRightsStoreTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        _bed.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private CoordinatorServerOptions Options(string? seed = null) => new()
    {
        StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
    };

    private string Seed()
    {
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """
            { "identities": [ { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] } ], "matrix": { "crm": ["crm"] } }
            """);
        return seed;
    }

    // One start of the coordinator: its own connection, the same prefix.
    private RightsStore Start(string? seed = null, bool initialize = true) => new(new RedisRightsStore(_bed.Open(initialize)), Options(seed));

    [Fact]
    public void The_Rights_Are_Read_Back_After_A_Restart_And_The_Seed_Is_Used_Once()
    {
        RedisTestBed.Need();
        var first = Start(Seed());
        first.UpdateIdentity("a", null, "Changed", ["crm", "sales"], null, "someone@test");

        // The seed names the identity as it was: a store that holds the rights does not read it.
        var again = Start(Seed(), initialize: false);

        Assert.Equal(["crm", "sales"], again.Find("a")!.Groups);
        Assert.Equal("someone@test", again.Snapshot().Audit![0].By);
        Assert.Equal(2, again.Snapshot().Audit!.Count);
    }

    [Fact]
    public void Rights_That_Are_Gone_Are_Not_Begun_Again_Unless_Told_To()
    {
        RedisTestBed.Need();

        var failure = Assert.Throws<InvalidOperationException>(() => Start(Seed(), initialize: false));

        Assert.Contains("Initialize", failure.Message);
    }

    [Fact]
    public void Two_Starts_That_Cross_Write_The_Seed_Once()
    {
        RedisTestBed.Need();
        var first = new RedisRightsStore(_bed.Open());
        var second = new RedisRightsStore(_bed.Open());

        Assert.True(first.Create("""{ "identities": [], "matrix": {} }"""));
        Assert.False(second.Create("""{ "identities": [ { "clientId": "x" } ], "matrix": {} }"""));

        Assert.Equal(1, second.Load()!.Version);
        Assert.DoesNotContain("\"x\"", second.Load()!.Document);
    }

    [Fact]
    public void A_Write_Over_A_Version_That_Moved_Is_Refused_And_Changes_Nothing()
    {
        RedisTestBed.Need();
        var store = new RedisRightsStore(_bed.Open());
        store.Create("one");

        Assert.Equal(2, store.Replace("two", 1));
        Assert.Null(store.Replace("three", 1));

        Assert.Equal(new StoredRights("two", 2), store.Load());
    }

    [Fact]
    public void A_Store_That_Cannot_Be_Reached_Refuses_The_Change_And_The_Rights_Stay()
    {
        RedisTestBed.Need();
        var connection = _bed.Open();
        var rights = new RightsStore(new RedisRightsStore(connection), Options(Seed()));
        // Redis goes away: the connection is closed under the store.
        connection.Dispose();

        Assert.Throws<StoreUnavailableException>(() => rights.UpdateIdentity("a", null, "Lost", null, null));

        Assert.Equal("A", rights.Find("a")!.Description);
        Assert.Equal("A", Start(initialize: false).Find("a")!.Description);
    }
}
