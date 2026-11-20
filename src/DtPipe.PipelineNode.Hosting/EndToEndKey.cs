using System.Security.Cryptography;
using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// What a node does with end-to-end encryption, resolved once from its configuration. The TransportR client is built from
/// <see cref="Enabled"/> and <see cref="Key"/>, and the announcement says <see cref="Protection"/> and <see cref="KeyId"/>, so the
/// coordinator is told what the client applies and nothing else. <see cref="Key"/> is the key a transfer is sealed with, for the peer
/// client id it is given, or null when the node has none; <see cref="KeyId"/> is the fingerprint of a configured key
/// (<see cref="EndToEndKey.IdOf"/>), null without one and for a provider, whose keys the node cannot name.
/// </summary>
internal sealed record EndToEndSetting(bool Enabled, Func<Guid, byte[]?>? Key, string Protection, string? KeyId);

/// <summary>
/// The node's end-to-end pre-shared key, from its own configuration and never from the coordinator, which runs beside the hub
/// and reaches the node through the same address as the data: a key that travelled that path would protect nothing against whoever
/// holds it. A value or a file holds the key as base64 of 32 bytes (<c>openssl rand -base64 32</c>), read once at start through
/// <see cref="SecretReference"/>; a provider is the host's own code, asked at each transfer. No message names the key, the file or
/// what it holds: the node announces the key's id (<see cref="IdOf"/>), which tells which nodes share it and nothing of it.
/// </summary>
internal static class EndToEndKey
{
    /// <summary>The length TransportR takes.</summary>
    public const int Length = 32;

    /// <summary>How many bytes of the keyed hash the id keeps.</summary>
    private const int IdBytes = DataPlaneProtections.KeyIdLength / 2;

    private const string ValueSetting = "EndToEndEncryption:PreSharedKey";
    private const string FileSetting = "EndToEndEncryption:PreSharedKeyFile";

    /// <summary>
    /// The node's setting, with the key read. Throws an <see cref="InvalidOperationException"/> that names the setting when a file
    /// cannot be read or the text is no key.
    /// </summary>
    public static EndToEndSetting Resolve(NodeEncryptionOptions options)
    {
        var enabled = options.Enabled;
        if (options.PreSharedKeyProvider is { } provider) return new(enabled, provider, ProtectionOf(enabled, keyed: true), null);
        string text, setting;
        if (!string.IsNullOrEmpty(options.PreSharedKeyFile))
            (text, setting) = (SecretReference.ReadFile(options.PreSharedKeyFile, FileSetting), FileSetting);
        else if (!string.IsNullOrEmpty(options.PreSharedKey))
            (text, setting) = (SecretReference.Read(options.PreSharedKey, ValueSetting), ValueSetting);
        else
            return new(enabled, null, ProtectionOf(enabled, keyed: false), null);
        var key = Decode(text) ?? throw new InvalidOperationException($"{setting} is not base64 of {Length} bytes.");
        return new(enabled, _ => key, ProtectionOf(enabled, keyed: true), enabled ? IdOf(key) : null);
    }

    private static string ProtectionOf(bool enabled, bool keyed) =>
        !enabled ? DataPlaneProtections.None : keyed ? DataPlaneProtections.EncryptedPsk : DataPlaneProtections.Encrypted;

    /// <summary>
    /// The fingerprint of <paramref name="key"/>: the first eight bytes of its HMAC-SHA256 over a fixed label, in lowercase
    /// hexadecimal. The key is the HMAC's own key, so the id cannot be turned back into it, and two nodes that share a key share the id.
    /// </summary>
    internal static string IdOf(byte[] key) => Convert.ToHexStringLower(HMACSHA256.HashData(key, "dtpipe end-to-end key id"u8)[..IdBytes]);

    /// <summary>Why <see cref="Resolve"/> would refuse <paramref name="options"/>, or null; read at start, with the other settings.</summary>
    public static string? Refusal(NodeEncryptionOptions options)
    {
        try
        {
            Resolve(options);
            return null;
        }
        catch (InvalidOperationException ex)
        {
            return ex.Message;
        }
    }

    private static byte[]? Decode(string text)
    {
        try
        {
            var key = Convert.FromBase64String(text);
            return key.Length == Length ? key : null;
        }
        catch (FormatException)
        {
            return null;
        }
    }
}
