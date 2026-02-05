using System.Text.Json;
using KeySharp;

namespace DtPipe.Cli.Security;

public class SecretsManager
{
    private const string ServiceName = "dtpipe";
    private const string BlobUserName = "configuration";

    public SecretsManager()
    {
        // No platform checks needed, KeySharp handles it.
    }

    public void SetSecret(string alias, string value)
    {
        var secrets = LoadSecrets();
        secrets[alias] = value;
        SaveSecrets(secrets);
    }

    public string? GetSecret(string alias)
    {
        var secrets = LoadSecrets();
        return secrets.TryGetValue(alias, out var val) ? val : null;
    }

    public void DeleteSecret(string alias)
    {
        var secrets = LoadSecrets();
        if (secrets.Remove(alias))
        {
            SaveSecrets(secrets);
        }
    }
    
    public Dictionary<string, string> ListSecrets()
    {
        return LoadSecrets();
    }

    public void Nuke()
    {
        try
        {
            Keyring.DeletePassword(ServiceName, ServiceName, BlobUserName);
        }
        catch
        {
            // Ignore if not found
        }
    }

    private Dictionary<string, string> LoadSecrets()
    {
        try
        {
            var json = Keyring.GetPassword(ServiceName, ServiceName, BlobUserName);
            if (string.IsNullOrWhiteSpace(json)) return new Dictionary<string, string>();
            return JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
        }
        catch (Exception)
        {
            return new Dictionary<string, string>();
        }
    }

    private void SaveSecrets(Dictionary<string, string> secrets)
    {
        var json = JsonSerializer.Serialize(secrets);
        Keyring.SetPassword(ServiceName, ServiceName, BlobUserName, json);
    }
}
