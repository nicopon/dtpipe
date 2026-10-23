using DtPipe.Sessions;
using AwesomeAssertions;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

/// <summary>
/// The TTL purge is the housekeeping half of the "artefacts do not outlive their usefulness"
/// promise. It has to be silent (housekeeping that asks is housekeeping nobody runs) and it has
/// to destroy the key before the files — that ordering is what makes a half-failed deletion
/// leave inert bytes instead of readable data.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class SessionPurgeTests : IDisposable
{
    private readonly string _tmp;
    private readonly string _root;
    private readonly string? _savedState;

    public SessionPurgeTests()
    {
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_purge_{Guid.NewGuid():N}");
        _root = Path.Combine(_tmp, ".dtpipe");
        Directory.CreateDirectory(_tmp);
        _savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
    }

    private SessionStore Session(string name, int ageDays, int ttlDays = SessionStore.DefaultTtlDays)
    {
        var store = new SessionStore(new SessionIdentity(name, _root, SessionOrigin.Explicit));
        var meta = store.EnsureCreated();
        meta.TtlDays = ttlDays;
        meta.LastUsedAt = DateTime.UtcNow.AddDays(-ageDays).ToString("O");
        store.WriteMetadata(meta);
        SessionKeyStore.GetOrCreateKey(name);
        return store;
    }

    [Fact]
    public void An_Expired_Session_Is_Removed_With_Its_Key()
    {
        var store = Session("stale", ageDays: 30);
        File.Exists(SessionKeyStore.KeyPath("stale")).Should().BeTrue();

        var removed = SessionPurge.PurgeExpired(_root);

        removed.Should().Equal("stale");
        Directory.Exists(store.SessionPath).Should().BeFalse();
        File.Exists(SessionKeyStore.KeyPath("stale")).Should().BeFalse(
            "leaving the key behind would leave a purged session readable if its files survived anywhere");
    }

    [Fact]
    public void A_Fresh_Session_Survives()
    {
        var store = Session("fresh", ageDays: 1);

        SessionPurge.PurgeExpired(_root).Should().BeEmpty();
        Directory.Exists(store.SessionPath).Should().BeTrue();
    }

    [Fact]
    public void A_Session_Uses_Its_Own_Recorded_Ttl()
    {
        Session("short-ttl", ageDays: 3, ttlDays: 1);
        Session("long-ttl", ageDays: 3, ttlDays: 90);

        SessionPurge.PurgeExpired(_root).Should().Equal("short-ttl");
    }

    [Fact]
    public void Purging_Reports_Nothing_On_A_Root_That_Does_Not_Exist()
    {
        var act = () => SessionPurge.PurgeExpired(Path.Combine(_tmp, "no-such-root"));

        act.Should().NotThrow("a purge must never be able to fail the run it is housekeeping for");
    }

    [Fact]
    public void One_Unreadable_Session_Does_Not_Strand_The_Others()
    {
        Session("good", ageDays: 30);
        var broken = Path.Combine(_root, "sessions", "broken");
        Directory.CreateDirectory(broken);
        File.WriteAllText(Path.Combine(broken, "session.json"), "{ not json");

        SessionPurge.PurgeExpired(_root).Should().Contain("good");
    }

    [Fact]
    public void Removing_A_Session_Deletes_The_Key_Before_The_Files()
    {
        var store = Session("ordered", ageDays: 30);
        var keyPath = SessionKeyStore.KeyPath("ordered");

        // Hold the directory open so its removal fails: what survives must already be unreadable.
        using (var held = File.Create(Path.Combine(store.SessionPath, "held.bin")))
        {
            try { SessionPurge.Remove("ordered", store.SessionPath); } catch { /* expected on some platforms */ }
        }

        File.Exists(keyPath).Should().BeFalse(
            "the key goes first, so a failed file deletion leaves inert bytes rather than data");
    }
}
