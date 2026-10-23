using Xunit;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;

namespace DtPipe.Tests.Unit.Cli;

public sealed class StudioVariablesTests : IDisposable
{
    private const string Job = "a:\n  input: \"${{keyring://db}}\"\n";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-variables-").FullName;

    public void Dispose() { try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } }

    private sealed class Counting : ISecretsManager
    {
        public readonly InMemorySecretsManager Inner = new();
        public int Lists;
        public void SetSecret(string alias, string value) => Inner.SetSecret(alias, value);
        public string? GetSecret(string alias) => Inner.GetSecret(alias);
        public void DeleteSecret(string alias) => Inner.DeleteSecret(alias);
        public Dictionary<string, string> ListSecrets() { Lists++; return Inner.ListSecrets(); }
        public void Nuke() => Inner.Nuke();
    }

    [Fact]
    public void The_keyring_is_opened_once_per_session_and_again_only_for_an_alias_the_list_lacks()
    {
        var secrets = new Counting();
        secrets.SetSecret("db", "x");
        var variables = new StudioVariables(new StudioLocalFiles(new StudioFolder(_dir)), secrets);

        Assert.True(variables.Of(null, Job).Single().Ready);
        Assert.True(variables.Of(null, Job).Single().Ready);
        _ = variables.Aliases();
        Assert.Equal(1, secrets.Lists);

        // An alias added meanwhile is found by one more read, kept for the next.
        secrets.SetSecret("other", "y");
        Assert.True(variables.Of(null, Job.Replace("db", "other")).Single().Ready);
        Assert.Equal(2, secrets.Lists);

        // One that does not exist costs one read per composition, never one per variable.
        Assert.False(variables.Of(null, Job.Replace("db", "missing")).Single().Ready);
        Assert.Equal(3, secrets.Lists);
    }
}
