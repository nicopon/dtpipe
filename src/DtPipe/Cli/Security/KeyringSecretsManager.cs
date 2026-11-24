using System;
using System.Collections.Generic;
using System.Text.Json;
using KeySharp;

namespace DtPipe.Cli.Security;

/// <summary>The operating system's secret service, one entry per user name. A read of an entry that does not exist throws.</summary>
internal interface IKeyringBackend
{
    string? Get(string userName);
    void Set(string userName, string value);
    void Delete(string userName);
}

internal sealed class KeySharpBackend : IKeyringBackend
{
    private const string ServiceName = "dtpipe";

    public string? Get(string userName) => Keyring.GetPassword(ServiceName, ServiceName, userName);

    public void Set(string userName, string value) => Keyring.SetPassword(ServiceName, ServiceName, userName, value);

    public void Delete(string userName) => Keyring.DeletePassword(ServiceName, ServiceName, userName);
}

/// <summary>The keyring holds secrets that cannot be read now; nothing was written.</summary>
public sealed class KeyringUnreadableException(string message, Exception? inner = null) : InvalidOperationException(message, inner);

/// <summary>
/// The secrets of <c>dtpipe secret</c>: one JSON blob in one keyring entry. KeySharp raises the same exception for an entry
/// that does not exist and for one that cannot be read (its error type is not reliable), so a second entry, the marker, is
/// written with every blob: a failed read of the blob with the marker present is "unreadable" and throws, and no write follows
/// it, since a write would replace every alias with the one being set. Without the marker the blob is "absent" and reads as
/// empty. A blob written before the marker existed reads as absent when its read fails.
/// </summary>
public class KeyringSecretsManager : ISecretsManager
{
    private const string BlobUserName = "configuration";
    private const string MarkerUserName = "configuration.present";

    private readonly IKeyringBackend _backend;

    public KeyringSecretsManager() : this(new KeySharpBackend())
    {
    }

    internal KeyringSecretsManager(IKeyringBackend backend)
    {
        _backend = backend;
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
        foreach (var user in new[] { BlobUserName, MarkerUserName })
        {
            try
            {
                _backend.Delete(user);
            }
            catch
            {
                // Ignore if not found
            }
        }
    }

    private Dictionary<string, string> LoadSecrets()
    {
        string? json;
        try
        {
            json = _backend.Get(BlobUserName);
        }
        catch (Exception ex)
        {
            if (MarkerPresent())
                throw new KeyringUnreadableException("The keyring holds dtpipe's secrets but cannot be read now (is it locked?). Nothing was changed.", ex);
            return new Dictionary<string, string>();
        }

        if (string.IsNullOrWhiteSpace(json)) return new Dictionary<string, string>();
        try
        {
            return JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
        }
        catch (JsonException ex)
        {
            throw new KeyringUnreadableException("The keyring entry of dtpipe's secrets is not readable. Nothing was changed; `dtpipe secret nuke` removes it.", ex);
        }
    }

    private bool MarkerPresent()
    {
        try
        {
            return !string.IsNullOrEmpty(_backend.Get(MarkerUserName));
        }
        catch (Exception)
        {
            return false;
        }
    }

    private void SaveSecrets(Dictionary<string, string> secrets)
    {
        var json = JsonSerializer.Serialize(secrets);
        _backend.Set(BlobUserName, json);
        _backend.Set(MarkerUserName, "1");
    }
}
