using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using DtPipe.Sessions;
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
/// that does not exist and for one that cannot be read (its error type is not reliable), so an out-of-keyring marker file
/// is written alongside every save: a failed read of the blob with the marker present is "unreadable" and throws, and no
/// write follows it, since a write would replace every alias with the one being set. Without the marker (neither the file
/// nor a legacy keyring marker), the blob is "absent" and reads as empty.
/// </summary>
public class KeyringSecretsManager : ISecretsManager
{
    private const string BlobUserName = "configuration";
    private const string LegacyMarkerUserName = "configuration.present";

    private readonly IKeyringBackend _backend;
    private readonly string _markerPath;
    private readonly object _gate = new();

    public KeyringSecretsManager() : this(new KeySharpBackend(), DefaultMarkerPath())
    {
    }

    internal KeyringSecretsManager(IKeyringBackend backend, string markerPath)
    {
        _backend = backend;
        _markerPath = markerPath;
    }

    private static string DefaultMarkerPath() => Path.Combine(UserStatePaths.Root(), "keyring.present");

    public void SetSecret(string alias, string value)
    {
        lock (_gate)
        {
            var secrets = LoadSecrets();
            secrets[alias] = value;
            SaveSecrets(secrets);
        }
    }

    public string? GetSecret(string alias)
    {
        lock (_gate)
        {
            var secrets = LoadSecrets();
            return secrets.TryGetValue(alias, out var val) ? val : null;
        }
    }

    public void DeleteSecret(string alias)
    {
        lock (_gate)
        {
            var secrets = LoadSecrets();
            if (secrets.Remove(alias))
            {
                SaveSecrets(secrets);
            }
        }
    }

    public Dictionary<string, string> ListSecrets()
    {
        lock (_gate)
        {
            return LoadSecrets();
        }
    }

    public void Nuke()
    {
        lock (_gate)
        {
            try
            {
                if (File.Exists(_markerPath)) File.Delete(_markerPath);
            }
            catch
            {
                // Ignore if unavailable
            }

            foreach (var user in new[] { LegacyMarkerUserName, BlobUserName })
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
        if (File.Exists(_markerPath)) return true;
        try
        {
            return !string.IsNullOrEmpty(_backend.Get(LegacyMarkerUserName));
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
        WriteMarker();
    }

    private void WriteMarker()
    {
        var dir = Path.GetDirectoryName(_markerPath);
        if (!string.IsNullOrEmpty(dir))
        {
            if (OperatingSystem.IsWindows()) Directory.CreateDirectory(dir);
            else Directory.CreateDirectory(dir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }

        if (File.Exists(_markerPath)) return;

        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
        };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using var stream = new FileStream(_markerPath, options);
            stream.WriteByte((byte)'1');
        }
        catch (IOException) when (File.Exists(_markerPath))
        {
        }
    }
}
