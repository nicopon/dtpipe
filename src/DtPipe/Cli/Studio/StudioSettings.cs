using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Sessions;

namespace DtPipe.Cli.Studio;

/// <summary>The agent's connection as <c>studio.json</c> keeps it: no secret, ever.</summary>
public sealed record AgentSettings(string Provider, string Url, string? Model, int? Timeout);

/// <summary>
/// What the studio remembers for the person on this machine: <c>studio.json</c> under <see cref="UserStatePaths.Root"/>, holding
/// no secret, and the secrets in the keyring's blob under aliases derived from the address they belong to
/// (<c>studio.llm:&lt;address&gt;</c>), so a key is never offered to another address. A file that cannot be read, or that a
/// newer studio wrote, is ignored with a warning and never stops the start. A command-line flag never writes here.
/// </summary>
public sealed class StudioSettings(string path, ISecretsManager secrets)
{
    public const int Version = 1;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public static string DefaultPath() => Path.Combine(UserStatePaths.Root(), "studio.json");

    /// <summary>Why the file was ignored the last time it was read, or null.</summary>
    public string? Warning { get; private set; }

    /// <summary>False once the keyring refused a read or a write: a key typed after that lasts the session only.</summary>
    public bool KeyringUsable { get; private set; } = true;

    private sealed record FileModel(int Version, AgentSettings? Agent);

    /// <summary>The remembered agent, or null: nothing remembered, or a file that is ignored (see <see cref="Warning"/>).</summary>
    public AgentSettings? ReadAgent()
    {
        Warning = null;
        if (!File.Exists(path)) return null;
        try
        {
            var file = JsonSerializer.Deserialize<FileModel>(File.ReadAllText(path), Json);
            if (file is null) return Ignored("it is empty");
            if (file.Version != Version) return Ignored($"it is of version {file.Version}, this studio reads version {Version}");
            if (file.Agent is not { } agent) return null;
            if (agent.Provider is not ("ollama" or "openai")) return Ignored("its agent names no known provider");
            if (!IsAddress(agent.Url)) return Ignored("its agent has no http or https address");
            return agent;
        }
        catch (Exception ex) when (ex is JsonException or IOException or UnauthorizedAccessException)
        {
            return Ignored("it cannot be read");
        }
    }

    private AgentSettings? Ignored(string reason)
    {
        Warning = $"The studio's settings ({path}) are ignored: {reason}.";
        return null;
    }

    public void WriteAgent(AgentSettings agent) => Write(new FileModel(Version, agent));

    /// <summary>Forgets the remembered agent; the file stays only for what else it holds.</summary>
    public void ClearAgent()
    {
        if (File.Exists(path)) Write(new FileModel(Version, null));
    }

    /// <summary>The file is written beside itself and moved over the old one, with the person's rights alone where the system has them.</summary>
    private void Write(FileModel model)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        var temporary = $"{path}.{Environment.ProcessId}.tmp";
        var options = new FileStreamOptions { Mode = FileMode.Create, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using (var stream = new FileStream(temporary, options))
            using (var writer = new StreamWriter(stream))
                writer.Write(JsonSerializer.Serialize(model, Json));
            File.Move(temporary, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporary)) File.Delete(temporary);
        }
    }

    private static bool IsAddress(string? url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https";

    /// <summary>
    /// The address as an alias reads it: scheme and host in lower case, the default port dropped, the trailing slash of the path
    /// dropped, the path and the query kept, no credential and no fragment.
    /// </summary>
    public static string NormalizeUrl(string url)
    {
        var uri = new Uri(url, UriKind.Absolute);
        var port = uri.IsDefaultPort ? "" : $":{uri.Port}";
        var path = uri.AbsolutePath.TrimEnd('/');
        return $"{uri.Scheme.ToLowerInvariant()}://{uri.Host.ToLowerInvariant()}{port}{path}{uri.Query}";
    }

    /// <summary>The address as a person is shown it: neither the credential, nor the query, nor the fragment is part of it.</summary>
    public static string Shown(string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" ? NormalizeUrl(url).Split('?')[0] : "";

    public static string LlmAlias(string url) => ReservedAliases.StudioPrefix + "llm:" + NormalizeUrl(url);

    /// <summary>The key kept for this address, or null when there is none or the keyring cannot be read.</summary>
    public string? ReadLlmKey(string url)
    {
        try
        {
            var key = secrets.GetSecret(LlmAlias(url));
            KeyringUsable = true;
            return key;
        }
        catch (Exception)
        {
            KeyringUsable = false;
            return null;
        }
    }

    /// <summary>Keeps the key for this address; false when the keyring refuses, and the key then lasts the session alone.</summary>
    public bool StoreLlmKey(string url, string key)
    {
        try
        {
            secrets.SetSecret(LlmAlias(url), key);
            KeyringUsable = true;
            return true;
        }
        catch (Exception)
        {
            KeyringUsable = false;
            return false;
        }
    }

    public void ForgetLlmKey(string url)
    {
        try { secrets.DeleteSecret(LlmAlias(url)); }
        catch (Exception) { KeyringUsable = false; }
    }
}
