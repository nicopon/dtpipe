namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The node's end-to-end pre-shared key, from its own configuration and never from the coordinator, which runs beside the hub
/// and reaches the node through the same address as the data: a key that travelled that path would protect nothing against whoever
/// holds it. A value or a file holds the key as base64 of 32 bytes (<c>openssl rand -base64 32</c>), read once at start through
/// <see cref="SecretReference"/>; a provider is the host's own code, asked at each transfer. No message names the key, the file or
/// what it holds.
/// </summary>
internal static class EndToEndKey
{
    /// <summary>The length TransportR takes.</summary>
    public const int Length = 32;

    private const string ValueSetting = "EndToEndEncryption:PreSharedKey";
    private const string FileSetting = "EndToEndEncryption:PreSharedKeyFile";

    /// <summary>
    /// The key a transfer is sealed with, for the peer client id it is given, or null when the node has none. Throws an
    /// <see cref="InvalidOperationException"/> that names the setting when a file cannot be read or the text is no key.
    /// </summary>
    public static Func<Guid, byte[]?>? Resolve(NodeEncryptionOptions options)
    {
        if (options.PreSharedKeyProvider is { } provider) return provider;
        string text, setting;
        if (!string.IsNullOrEmpty(options.PreSharedKeyFile))
            (text, setting) = (SecretReference.ReadFile(options.PreSharedKeyFile, FileSetting), FileSetting);
        else if (!string.IsNullOrEmpty(options.PreSharedKey))
            (text, setting) = (SecretReference.Read(options.PreSharedKey, ValueSetting), ValueSetting);
        else
            return null;
        var key = Decode(text) ?? throw new InvalidOperationException($"{setting} is not base64 of {Length} bytes.");
        return _ => key;
    }

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
