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
        Assert.Null(settings.ReadAgent());
        Assert.Null(settings.Warning);

        settings.WriteAgent(new AgentSettings("ollama", "http://localhost:11434", "m", 30));
        Assert.Equal(new AgentSettings("ollama", "http://localhost:11434", "m", 30), settings.ReadAgent());

        File.WriteAllText(SettingsPath, "{ not json");
        Assert.Null(settings.ReadAgent());
        Assert.NotNull(settings.Warning);

        File.WriteAllText(SettingsPath, """{"version":99,"agent":{"provider":"ollama","url":"http://localhost:11434"}}""");
        Assert.Null(settings.ReadAgent());
        Assert.Contains("version 99", settings.Warning);
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

        Assert.Equal(new AgentSettings("ollama", "http://localhost:11434", "m", 30), settings.ReadAgent());
        Assert.Equal(new CoordinatorSettings("https://coordinator.example.com", "key"), settings.ReadCoordinator());
        Assert.Equal("studio.coordinator:https://coordinator.example.com", StudioSettings.CoordinatorAlias("https://Coordinator.example.com:443/"));
        Assert.Equal("dtp_key", settings.ReadCoordinatorKey("https://coordinator.example.com"));
        Assert.Null(settings.ReadCoordinatorKey("https://other.example.com"));
        Assert.DoesNotContain("dtp_key", File.ReadAllText(SettingsPath));

        settings.ClearCoordinator();
        Assert.Null(settings.ReadCoordinator());
        Assert.NotNull(settings.ReadAgent());

        File.WriteAllText(SettingsPath, """{"version":1,"coordinator":{"origin":"https://x.example.com/path","auth":"key"}}""");
        Assert.Null(settings.ReadCoordinator());
        Assert.NotNull(settings.Warning);
    }
}
