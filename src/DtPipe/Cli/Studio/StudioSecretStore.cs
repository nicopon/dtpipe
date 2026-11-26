using DtPipe.Cli.Security;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Manages studio secrets in the system keyring under aliases bound to the normalized address
/// (<c>studio.llm:&lt;address&gt;</c>, <c>studio.coordinator:&lt;origin&gt;</c>), so a key is never offered to another address.
/// </summary>
public sealed class StudioSecretStore(ISecretsManager secrets)
{
    public static string LlmAlias(string url) => ReservedAliases.StudioPrefix + "llm:" + StudioSettings.NormalizeUrl(url);

    public static string CoordinatorAlias(string origin) => ReservedAliases.StudioPrefix + "coordinator:" + StudioSettings.NormalizeUrl(origin);

    /// <summary>The key kept for this model address, or null when there is none or the keyring cannot be read.</summary>
    public string? ReadLlmKey(string url) => ReadKey(LlmAlias(url));

    /// <summary>Keeps the key for this model address; false when the keyring refuses, and the key then lasts the session alone.</summary>
    public bool StoreLlmKey(string url, string key) => StoreKey(LlmAlias(url), key);

    public void ForgetLlmKey(string url) => ForgetKey(LlmAlias(url));

    /// <summary>The key kept for this coordinator, or null when there is none or the keyring cannot be read.</summary>
    public string? ReadCoordinatorKey(string origin) => ReadKey(CoordinatorAlias(origin));

    /// <summary>Keeps the key for this coordinator; false when the keyring refuses, and the key then lasts the session alone.</summary>
    public bool StoreCoordinatorKey(string origin, string key) => StoreKey(CoordinatorAlias(origin), key);

    public void ForgetCoordinatorKey(string origin) => ForgetKey(CoordinatorAlias(origin));

    /// <summary>
    /// The aliases the keyring holds now, and nothing of what they hold: the page learns from them which recent entries have a key
    /// kept. Empty when the keyring cannot be read.
    /// </summary>
    public IReadOnlySet<string> KeptAliases(out bool keyringUsable)
    {
        try
        {
            var aliases = secrets.ListSecrets().Keys.ToHashSet(StringComparer.Ordinal);
            keyringUsable = true;
            return aliases;
        }
        catch (Exception)
        {
            keyringUsable = false;
            return new HashSet<string>();
        }
    }

    public IReadOnlySet<string> KeptAliases() => KeptAliases(out _);

    private string? ReadKey(string alias)
    {
        try
        {
            return secrets.GetSecret(alias);
        }
        catch (Exception)
        {
            return null;
        }
    }

    private bool StoreKey(string alias, string key)
    {
        try
        {
            secrets.SetSecret(alias, key);
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private void ForgetKey(string alias)
    {
        try { secrets.DeleteSecret(alias); }
        catch (Exception) { }
    }
}
