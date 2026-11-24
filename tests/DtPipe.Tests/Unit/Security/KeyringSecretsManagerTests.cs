using DtPipe.Cli.Security;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

public class KeyringSecretsManagerTests
{
    /// <summary>A keyring in memory: an entry that does not exist throws on read, as KeySharp does, and a read can be made to fail.</summary>
    private sealed class FakeBackend : IKeyringBackend
    {
        public Dictionary<string, string> Entries { get; } = [];
        public bool FailReadsOfBlob { get; set; }
        public int Writes { get; private set; }

        public string? Get(string userName)
        {
            if (FailReadsOfBlob && userName == "configuration") throw new InvalidOperationException("locked");
            return Entries.TryGetValue(userName, out var value) ? value : throw new InvalidOperationException("absent");
        }

        public void Set(string userName, string value) { Writes++; Entries[userName] = value; }

        public void Delete(string userName) => Entries.Remove(userName);
    }

    [Fact]
    public void An_absent_entry_reads_as_empty_and_the_first_write_creates_the_blob_and_its_marker()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend);

        Assert.Empty(secrets.ListSecrets());
        Assert.Null(secrets.GetSecret("a"));

        secrets.SetSecret("a", "1");
        secrets.SetSecret("b", "2");

        Assert.Equal(["a", "b"], secrets.ListSecrets().Keys.Order());
        Assert.Contains("configuration.present", backend.Entries.Keys);
    }

    [Fact]
    public void An_unreadable_entry_throws_and_no_write_follows_so_no_alias_is_lost()
    {
        var backend = new FakeBackend();
        var secrets = new KeyringSecretsManager(backend);
        secrets.SetSecret("kept", "value");
        var writes = backend.Writes;
        var blob = backend.Entries["configuration"];

        backend.FailReadsOfBlob = true;

        Assert.Throws<KeyringUnreadableException>(() => secrets.GetSecret("kept"));
        Assert.Throws<KeyringUnreadableException>(() => secrets.ListSecrets());
        Assert.Throws<KeyringUnreadableException>(() => secrets.SetSecret("new", "x"));
        Assert.Throws<KeyringUnreadableException>(() => secrets.DeleteSecret("kept"));
        Assert.Equal(writes, backend.Writes);
        Assert.Equal(blob, backend.Entries["configuration"]);
    }

    [Fact]
    public void A_corrupt_blob_is_unreadable_not_empty()
    {
        var backend = new FakeBackend();
        backend.Entries["configuration"] = "{not json";
        var secrets = new KeyringSecretsManager(backend);

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
}
