using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>What the studio remembers: a file with no secret that never stops a start, and a key bound to its address.</summary>
public sealed class StudioSettingsTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("studio-settings-").FullName;
    private readonly InMemorySecretsManager _secrets = new();

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string SettingsPath => Path.Combine(_dir, "state", "studio.json");

    [Fact]
    public void An_unreadable_file_or_one_of_another_version_is_ignored_with_a_warning_and_a_round_trip_keeps_the_agent()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        Assert.Null(settings.Read(out var warning).Agent);
        Assert.Null(warning);

        settings.WriteAgent(new AgentSettings("ollama", "http://localhost:11434", "m", 30));
        Assert.Equal(new AgentSettings("ollama", "http://localhost:11434", "m", 30), settings.Read().Agent);

        File.WriteAllText(SettingsPath, "{ not json");
        Assert.Null(settings.Read(out warning).Agent);
        Assert.NotNull(warning);

        File.WriteAllText(SettingsPath, """{"version":99,"agent":{"provider":"ollama","url":"http://localhost:11434"}}""");
        Assert.Null(settings.Read(out warning).Agent);
        Assert.Contains("version 99", warning);
    }

    [Fact]
    public void An_address_reads_alike_whatever_its_spelling_and_a_key_is_not_offered_to_another_address()
    {
        Assert.Equal("https://api.example.com/v1", StudioSettings.NormalizeUrl("HTTPS://API.Example.com:443/v1/"));
        Assert.Equal("http://localhost:8080/v1", StudioSettings.NormalizeUrl("http://localhost:8080/v1"));
        Assert.Equal("studio.llm:https://api.example.com/v1", StudioSettings.LlmAlias("https://API.example.com/v1/"));

        var settings = new StudioSettings(SettingsPath, _secrets);
        Assert.True(settings.StoreLlmKey("https://api.example.com/v1/", "sk-one"));

        Assert.Equal("sk-one", settings.ReadLlmKey("https://API.example.com:443/v1"));
        Assert.Null(settings.ReadLlmKey("https://api.example.com/v2"));
        Assert.Null(settings.ReadLlmKey("http://api.example.com/v1"));
    }

    [Fact]
    public void A_coordinator_round_trips_beside_the_agent_and_its_key_is_not_offered_to_another_origin()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        settings.WriteAgent(new AgentSettings("ollama", "http://localhost:11434", "m", 30));
        settings.WriteCoordinator(new CoordinatorSettings("https://coordinator.example.com", "key"));
        Assert.True(settings.StoreCoordinatorKey("https://Coordinator.example.com:443/", "dtp_key"));

        Assert.Equal(new AgentSettings("ollama", "http://localhost:11434", "m", 30), settings.Read().Agent);
        Assert.Equal(new CoordinatorSettings("https://coordinator.example.com", "key"), settings.Read().Coordinator);
        Assert.Equal("studio.coordinator:https://coordinator.example.com", StudioSettings.CoordinatorAlias("https://Coordinator.example.com:443/"));
        Assert.Equal("dtp_key", settings.ReadCoordinatorKey("https://coordinator.example.com"));
        Assert.Null(settings.ReadCoordinatorKey("https://other.example.com"));
        Assert.DoesNotContain("dtp_key", File.ReadAllText(SettingsPath));

        // Another coordinator becomes the active one, the first stays among the recent ones; removing the active one leaves nothing to connect at start.
        settings.RememberCoordinator(new CoordinatorSettings("https://coordinator.example.com", "key"));
        settings.WriteCoordinator(new CoordinatorSettings("https://other.example.com", "browser"));
        settings.RememberCoordinator(new CoordinatorSettings("https://other.example.com", "browser"));
        Assert.Equal(new CoordinatorSettings("https://other.example.com", "browser"), settings.Read().Coordinator);
        Assert.Equal(["https://other.example.com", "https://coordinator.example.com"], settings.ReadCoordinators().Select(c => c.Origin));

        settings.RemoveCoordinator("https://other.example.com");
        Assert.Null(settings.Read().Coordinator);
        Assert.Equal(["https://coordinator.example.com"], settings.ReadCoordinators().Select(c => c.Origin));
        Assert.DoesNotContain("other.example.com", File.ReadAllText(SettingsPath));
        Assert.NotNull(settings.Read().Agent);

        File.WriteAllText(SettingsPath, """{"version":1,"coordinator":{"origin":"https://x.example.com/path","auth":"key"}}""");
        Assert.Null(settings.Read(out var warning).Coordinator);
        Assert.NotNull(warning);
    }

    [Fact]
    public void The_recent_agents_and_coordinators_come_most_recent_first_each_once_with_no_bound_and_an_unreadable_entry_is_left_out()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        for (var i = 0; i < 12; i++)
        {
            settings.WriteAgent(new AgentSettings("ollama", $"http://host{i}:11434", "m", 30));
            settings.RememberCoordinator(new CoordinatorSettings($"https://c{i}.example.com", "browser"));
        }
        // An address read alike comes back to the top with what it was last used with, and is not listed twice.
        settings.WriteAgent(new AgentSettings("ollama", "HTTP://host3:11434/", "other", 60, 8192));
        settings.RememberCoordinator(new CoordinatorSettings("https://C3.example.com:443", "key"));

        var agents = settings.ReadAgents();
        Assert.Equal(12, agents.Count);
        Assert.Equal(new AgentSettings("ollama", "HTTP://host3:11434/", "other", 60, 8192), agents[0]);
        Assert.Equal("http://host11:11434", agents[1].Url);
        Assert.Single(agents, a => StudioSettings.NormalizeUrl(a.Url) == "http://host3:11434");
        Assert.Contains(agents, a => a.Url == "http://host0:11434");
        Assert.Equal(12, settings.ReadCoordinators().Count);
        Assert.Equal("key", settings.ReadCoordinators()[0].Auth);

        // Removing the remembered agent takes its entry with it.
        settings.WriteAgent(new AgentSettings("ollama", "http://solo:11434", "m", 30));
        settings.RemoveAgent("ollama", "http://solo:11434");
        Assert.Null(settings.Read().Agent);
        Assert.DoesNotContain(settings.ReadAgents(), a => a.Url == "http://solo:11434");

        File.WriteAllText(SettingsPath, """{"version":1,"agents":[{"provider":"nobody","url":"http://a:1"},{"provider":"openai","url":"https://api.example.com/v1"}],"coordinators":[{"origin":"https://x.example.com/path","auth":"key"}]}""");
        Assert.Equal(["https://api.example.com/v1"], settings.ReadAgents(out var agentWarning).Select(a => a.Url));
        Assert.NotNull(agentWarning);
        Assert.Empty(settings.ReadCoordinators(out var coordWarning));
        Assert.NotNull(coordWarning);
    }

    [Fact]
    public void Removing_an_entry_forgets_the_key_of_its_address_and_no_other_and_the_remembered_one_goes_with_its_entry()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        settings.WriteAgent(new AgentSettings("openai", "https://one.example.com/v1", "m", 30));
        settings.WriteAgent(new AgentSettings("openai", "https://two.example.com/v1", "m", 30));
        settings.StoreLlmKey("https://one.example.com/v1", "sk-one");
        settings.StoreLlmKey("https://two.example.com/v1", "sk-two");
        settings.RememberCoordinator(new CoordinatorSettings("https://c1.example.com", "key"));
        settings.RememberCoordinator(new CoordinatorSettings("https://c2.example.com", "key"));
        settings.StoreCoordinatorKey("https://c1.example.com", "dtp_one");
        settings.StoreCoordinatorKey("https://c2.example.com", "dtp_two");
        var kept = settings.KeptAliases();
        Assert.Contains(StudioSettings.LlmAlias("https://one.example.com/v1"), kept);
        Assert.Contains(StudioSettings.CoordinatorAlias("https://c1.example.com"), kept);

        settings.RemoveAgent("openai", "https://ONE.example.com/v1/");
        settings.RemoveCoordinator("https://c1.example.com");

        Assert.Equal(["https://two.example.com/v1"], settings.ReadAgents().Select(a => a.Url));
        Assert.Equal("https://two.example.com/v1", settings.Read().Agent?.Url);
        Assert.Null(settings.ReadLlmKey("https://one.example.com/v1"));
        Assert.Equal("sk-two", settings.ReadLlmKey("https://two.example.com/v1"));
        Assert.Equal(["https://c2.example.com"], settings.ReadCoordinators().Select(c => c.Origin));
        Assert.Null(settings.ReadCoordinatorKey("https://c1.example.com"));
        Assert.Equal("dtp_two", settings.ReadCoordinatorKey("https://c2.example.com"));

        // The remembered agent is the entry removed: nothing is connected again at start, and its key is gone with it.
        settings.RemoveAgent("openai", "https://two.example.com/v1");
        Assert.Null(settings.Read().Agent);
        Assert.Empty(settings.ReadAgents());
        Assert.Null(settings.ReadLlmKey("https://two.example.com/v1"));
        Assert.DoesNotContain("sk-", File.ReadAllText(SettingsPath));
    }

    [Fact]
    public void Recent_folders_come_most_recent_first_each_once_with_no_bound_and_one_that_is_no_full_path_is_left_out()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        settings.WriteAgent(new AgentSettings("ollama", "http://localhost:11434", "m", 30));
        var folders = Enumerable.Range(0, 12).Select(i => Path.Combine(_dir, $"f{i}")).ToList();
        foreach (var folder in folders) settings.RememberFolder(folder);
        settings.RememberFolder(folders[3] + Path.DirectorySeparatorChar);

        var recent = settings.ReadFolders();
        Assert.Equal(12, recent.Count);
        Assert.Equal(folders[3], recent[0]);
        Assert.Equal(folders[^1], recent[1]);
        Assert.Single(recent, f => f == folders[3]);
        Assert.Contains(folders[0], recent);
        Assert.NotNull(settings.Read().Agent);

        settings.RemoveFolder(folders[3] + Path.DirectorySeparatorChar);
        Assert.DoesNotContain(folders[3], settings.ReadFolders());
        Assert.Equal(11, settings.ReadFolders().Count);

        File.WriteAllText(SettingsPath, $$"""{"version":1,"folders":["relative/path","",{{JsonSerializer.Serialize(folders[1])}}]}""");
        Assert.Equal([folders[1]], settings.ReadFolders(out var warning));
        Assert.Contains("full path", warning);
    }

    [Fact]
    public void WriteCoordinator_with_remember_updates_both_active_and_recent_and_concurrent_writes_stay_valid()
    {
        var settings = new StudioSettings(SettingsPath, _secrets);
        var coordinator = new CoordinatorSettings("https://coordinator.example.com", "key");
        settings.WriteCoordinator(coordinator, remember: true);

        Assert.Equal(coordinator, settings.Read().Coordinator);
        Assert.Equal([coordinator], settings.ReadCoordinators());

        Parallel.For(0, 16, i =>
        {
            settings.RememberFolder(Path.Combine(_dir, $"concurrent-{i}"));
            settings.WriteAgent(new AgentSettings("ollama", $"http://h{i}:11434", "m", 30));
        });

        Assert.Equal(16, settings.ReadFolders(out var folderWarning).Count);
        Assert.Null(folderWarning);
        Assert.Equal(16, settings.ReadAgents(out var agentWarning).Count);
        Assert.Null(agentWarning);
    }

    [Fact]
    public void A_failing_keyring_reads_as_unusable_from_a_listing_or_a_quiet_key_read()
    {
        var failing = new ThrowingSecretsManager();
        var settings = new StudioSettings(SettingsPath, failing);

        var aliases = settings.KeptAliases(out var usable);

        Assert.Empty(aliases);
        Assert.False(usable);
        Assert.True(new StudioSettings(SettingsPath, _secrets).KeptAliases(out var ok) is not null && ok);

        // A view with no recent entry lists nothing, so it reads what the last key read found.
        var reading = new StudioSettings(SettingsPath, failing);
        Assert.True(reading.Secrets.Usable);
        Assert.Null(reading.Secrets.ReadCoordinatorKey("https://coord.example"));
        Assert.False(reading.Secrets.Usable);
    }

    private sealed class ThrowingSecretsManager : ISecretsManager
    {
        public void SetSecret(string alias, string value) => throw new InvalidOperationException("locked");
        public string? GetSecret(string alias) => throw new InvalidOperationException("locked");
        public void DeleteSecret(string alias) => throw new InvalidOperationException("locked");
        public Dictionary<string, string> ListSecrets() => throw new InvalidOperationException("locked");
        public void Nuke() => throw new InvalidOperationException("locked");
    }
}
