using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// An identity's keys in the rights: what opens an identity, what a document written before keys reads as, and the same keys read back
/// from each store after a restart.
/// </summary>
public sealed class IdentityKeyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "identity-keys-" + Guid.NewGuid().ToString("N"));
    private readonly RedisTestBed _redis = new();
    private SqlDatabase? _sqlite;

    public IdentityKeyTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        _sqlite?.Dispose();
        _redis.Dispose();
        SqliteConnection.ClearAllPools();
        Directory.Delete(_dir, recursive: true);
    }

    private CoordinatorServerOptions Options(string? seed = null) => new()
    {
        StateDir = _dir, Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
    };

    private string Seed(string json)
    {
        var seed = Path.Combine(_dir, "seed-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(seed, json);
        return seed;
    }

    private const string Keyed = """
        { "identities": [
            { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"], "auth": { "mode": "key" } },
            { "clientId": "b", "name": "node-b", "description": "B", "groups": ["crm"], "auth": { "mode": "key" } },
            { "clientId": "c", "name": "node-c", "description": "C", "groups": ["crm"] }
          ], "matrix": {} }
        """;

    private RightsStore Rights() => MemoryRightsStore.Rights(Options(Seed(Keyed)));

    private static DateTimeOffset Now => DateTimeOffset.UtcNow;

    [Fact]
    public void An_Identity_Written_Before_Keys_Reads_As_Verified_By_The_IdP_With_No_Key()
    {
        var rights = MemoryRightsStore.Rights(Options(Seed("""
            { "identities": [ { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"] } ], "matrix": {} }
            """)));

        var identity = rights.Find("a")!;

        Assert.Equal(AuthModes.Idp, identity.Auth.Mode);
        Assert.Empty(identity.Auth.Keys);
    }

    [Fact]
    public void An_Identity_The_IdP_Verifies_That_Holds_Keys_Is_Refused_When_The_Rights_Are_Read()
    {
        var failure = Assert.Throws<ConflictException>(() => MemoryRightsStore.Rights(Options(Seed("""
            { "identities": [ { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"],
              "auth": { "mode": "idp", "keys": [ { "id": "1", "hash": "00", "hint": "abcd", "created": "2026-01-01T00:00:00Z" } ] } } ], "matrix": {} }
            """))));

        Assert.Contains("holds keys", failure.Message);
    }

    [Fact]
    public void A_Key_Opens_Its_Identity_And_Nothing_Else_Does()
    {
        var rights = Rights();
        var (key, _) = rights.AddKey("a", null, null);
        var text = key.Reveal();
        var other = rights.AddKey("b", null, null).Key.Reveal();

        Assert.Equal("a", rights.KeyHolder(text, Now)?.ClientId);
        // The secret is judged by its hash, never by the client id it names.
        Assert.Null(rights.KeyHolder("dtp_a_" + other[^ClearKey.SecretLength..], Now));
        Assert.Null(rights.KeyHolder("dtp_b_" + text[^ClearKey.SecretLength..], Now));
        Assert.Null(rights.KeyHolder(text[..^1] + (text[^1] == 'A' ? 'B' : 'A'), Now));
        Assert.Null(rights.KeyHolder("dtp_nobody_" + text[^ClearKey.SecretLength..], Now));
        Assert.Null(rights.KeyHolder(text[..^1], Now));
        Assert.Null(rights.KeyHolder("", Now));
        Assert.Null(rights.KeyHolder(null, Now));
    }

    [Fact]
    public void A_Key_Opens_Nothing_Once_Expired_Or_Once_Its_Identity_Is_Disabled()
    {
        var rights = Rights();
        var expiring = rights.AddKey("a", null, Now.AddHours(1)).Key.Reveal();

        Assert.Equal("a", rights.KeyHolder(expiring, Now)?.ClientId);
        Assert.Null(rights.KeyHolder(expiring, Now.AddHours(2)));

        rights.UpdateIdentity("a", null, null, null, enabled: false);
        Assert.Null(rights.KeyHolder(expiring, Now));
    }

    [Fact]
    public void An_Expired_Key_Leaves_Room_For_A_Rotation()
    {
        var rights = MemoryRightsStore.Rights(Options(Seed("""
            { "identities": [ { "clientId": "a", "name": "node-a", "description": "A", "groups": ["crm"], "auth": { "mode": "key", "keys": [
                { "id": "1", "hash": "00", "hint": "abcd", "created": "2026-01-01T00:00:00Z", "expires": "2026-02-01T00:00:00Z" } ] } } ], "matrix": {} }
            """)));
        rights.AddKey("a", null, null);

        var (_, third) = rights.AddKey("a", null, null);

        Assert.Equal("3", third.Id);
    }

    [Fact]
    public void A_Clear_Key_Shows_Nothing_Of_Itself_Unless_Revealed()
    {
        var key = ClearKey.New("node-a");
        var record = Rights().AddKey("a", "label", null).Record;

        Assert.Equal("dtp_***", key.ToString());
        Assert.Equal("dtp_***", $"{key}");
        Assert.DoesNotContain(record.Hash, record.ToString());
        Assert.Equal("node-a", ClearKey.ClientIdOf(key.Reveal()));
    }

    [Theory]
    [InlineData("dtp_node-a_yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", "node-a")]
    [InlineData("dtp_a_yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", "a")]
    [InlineData("dtp__yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", null)]
    [InlineData("dtp_Node_yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", null)]
    [InlineData("dtp_node-a-yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", null)]
    [InlineData("key_node-a_yMnKy8zNzs_Q0dLT1NXW19jZ2tvc3d7f4OHi4-Tl5uc", null)]
    public void The_Client_Id_Reads_From_The_End_Since_The_Secret_Has_A_Fixed_Length(string presented, string? clientId) =>
        Assert.Equal(clientId, ClearKey.ClientIdOf(presented));

    public static TheoryData<string> Stores => ["memory", "sqlite", "redis"];

    /// <summary>One start of the coordinator over the store named <paramref name="kind"/>; the earlier start over SQLite is over.</summary>
    private IRightsStore Start(string kind)
    {
        switch (kind)
        {
            case "memory":
                return MemoryRightsStore.At(_dir);
            case "sqlite":
                _sqlite?.Dispose();
                _sqlite = SqlDatabase.Open(new SqliteDialect(_dir), true, TimeSpan.Zero, NullLogger.Instance);
                return new SqlRightsStore(_sqlite);
            default:
                RedisTestBed.Need();
                return new RedisRightsStore(_redis.Open());
        }
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public void Every_Store_Gives_The_Keys_Back_As_They_Were_Kept(string kind)
    {
        var seed = Seed(Keyed);
        var first = new RightsStore(Start(kind), Options(seed));
        var (old, _) = first.AddKey("a", "first", null, "root@x.test");
        var (fresh, _) = first.AddKey("a", null, Now.AddDays(30), "root@x.test");
        first.RevokeKey("a", "1", "root@x.test");

        var again = new RightsStore(Start(kind), Options(seed));

        Assert.Equal(first.Find("a")!.Auth.Keys, again.Find("a")!.Auth.Keys);
        Assert.Equal(AuthModes.Key, again.Find("a")!.Auth.Mode);
        Assert.Equal(AuthModes.Idp, again.Find("c")!.Auth.Mode);
        Assert.Null(again.KeyHolder(old.Reveal(), Now));
        Assert.Equal("a", again.KeyHolder(fresh.Reveal(), Now)?.ClientId);
        Assert.Equal(["Identity a: key 1 revoked", "Identity a: key 2 made, key 1 still live (rotation)", "Identity a: key 1 made"],
            again.Snapshot().Audit!.Take(3).Select(e => e.Change));
    }
}
