using System.IO;
using DtPipe.Cli.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

public class KeyringSecretsManagerTests : IDisposable
{
    private readonly string _tmp = Directory.CreateTempSubdirectory("keyring-tests-").FullName;

    private string MarkerPath => Path.Combine(_tmp, "keyring.present");

    public void Dispose()
    {
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
    }

    /// <summary>A keyring in memory: an entry that does not exist throws on read, as KeySharp does, and reads can fail selectively or globally.</summary>
    private sealed class FakeBackend : IKeyringBackend
    {
        public Dictionary<string, string> Entries { get; } = [];
        public bool FailReadsOfBlob { get; set; }
        public bool FailAllReads { get; set; }
        public int Writes { get; private set; }

        public string? Get(string userName)
        {
            if (FailAllReads) throw new InvalidOperationException("keyring locked");
            if (FailReadsOfBlob && userName == "configuration") throw new InvalidOperationException("locked");
            return Entries.TryGetValue(userName, out var value) ? value : throw new InvalidOperationException("absent");
        }

        public bool FailWrites { get; set; }

        public void Set(string userName, string value)
        {
            if (FailWrites) throw new InvalidOperationException("keyring refused");
            Writes++;
            Entries[userName] = value;
        }

        public void Delete(string userName) => Entries.Remove(userName);
    }

    [Fact]
    public void An_absent_entry_reads_as_empty_and_the_first_write_creates_the_blob_and_its_marker()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend, MarkerPath);

        Assert.Empty(secrets.ListSecrets());
        Assert.Null(secrets.GetSecret("a"));

        secrets.SetSecret("a", "1");
        secrets.SetSecret("b", "2");

        Assert.Equal(["a", "b"], secrets.ListSecrets().Keys.Order());
        Assert.True(File.Exists(MarkerPath));
    }

    [Fact]
    public void An_unreadable_entry_throws_and_no_write_follows_so_no_alias_is_lost()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend, MarkerPath);
        secrets.SetSecret("kept", "value");
        var writes = backend.Writes;
        var blob = backend.Entries["configuration"];

        backend.FailAllReads = true;

        Assert.Throws<KeyringUnreadableException>(() => secrets.GetSecret("kept"));
        Assert.Throws<KeyringUnreadableException>(() => secrets.ListSecrets());
        Assert.Throws<KeyringUnreadableException>(() => secrets.SetSecret("new", "x"));
        Assert.Throws<KeyringUnreadableException>(() => secrets.DeleteSecret("kept"));
        Assert.Equal(writes, backend.Writes);
        Assert.Equal(blob, backend.Entries["configuration"]);
    }

    [Fact]
    public void A_save_never_reaches_the_keyring_without_its_marker()
    {
        var blocker = Path.Combine(_tmp, "blocker");
        File.WriteAllText(blocker, "");
        var backend = new FakeBackend();
        var unmarkable = new KeyringSecretsManager(backend, Path.Combine(blocker, "keyring.present"));

        Assert.ThrowsAny<IOException>(() => unmarkable.SetSecret("a", "1"));
        Assert.Equal(0, backend.Writes);

        backend.FailWrites = true;
        var secrets = new KeyringSecretsManager(backend, MarkerPath);
        Assert.Throws<InvalidOperationException>(() => secrets.SetSecret("a", "1"));
        Assert.False(File.Exists(MarkerPath));
        Assert.Empty(secrets.ListSecrets());
    }

    [Fact]
    public void Nuke_removes_the_marker_file_so_a_subsequent_empty_read_starts_clean()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend, MarkerPath);
        secrets.SetSecret("kept", "value");
        Assert.True(File.Exists(MarkerPath));

        secrets.Nuke();

        Assert.False(File.Exists(MarkerPath));
        Assert.Empty(secrets.ListSecrets());
    }

    [Fact]
    public void A_corrupt_blob_is_unreadable_not_empty()
    {
        var backend = new FakeBackend();
        backend.Entries["configuration"] = "{not json";
        var secrets = new KeyringSecretsManager(backend, MarkerPath);

        Assert.Throws<KeyringUnreadableException>(() => secrets.SetSecret("a", "1"));
        Assert.Equal(0, backend.Writes);
    }

    [Fact]
    public void The_studio_aliases_are_not_listed_to_the_person()
    {
        var shown = ReservedAliases.Visible(new Dictionary<string, string>
        {
            ["prod-db"] = "x", ["studio.llm:https://api.openai.com"] = "sk-secret",
        }).Select(s => s.Key);

        Assert.Equal(["prod-db"], shown);
    }

    [Fact]
    public async Task Keyring_interpolator_refuses_reserved_studio_aliases()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend, MarkerPath);
        secrets.SetSecret("prod-db", "postgres://user:pass@host/db");
        secrets.SetSecret("studio.llm:https://api.openai.com", "sk-secret");
        secrets.SetSecret("studio.coordinator:https://coord.example", "dtp_secret");

        var interpolator = new KeyringInterpolator(secrets);

        Assert.Equal("postgres://user:pass@host/db", await interpolator.TryResolveAsync("keyring://prod-db", TestContext.Current.CancellationToken));
        Assert.Null(await interpolator.TryResolveAsync("keyring://studio.llm:https://api.openai.com", TestContext.Current.CancellationToken));
        Assert.Null(await interpolator.TryResolveAsync("keyring://studio.coordinator:https://coord.example", TestContext.Current.CancellationToken));
    }
}
