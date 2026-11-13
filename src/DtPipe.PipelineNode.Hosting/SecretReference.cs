namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// A secret the host gives the node in a setting: the text itself, or <c>@path</c> for the content of a file with the white space
/// around it removed (a container reads it from a mounted secret). Read once, when the node starts. No message names what was read
/// nor the path, which could be a key pasted after <c>@</c> by mistake. A secret that begins with <c>@</c> is given in a file or by
/// a provider, never as text.
/// </summary>
internal static class SecretReference
{
    public static string Read(string value, string setting) =>
        value.StartsWith('@') ? ReadFile(value[1..], setting, " (@…)") : value;

    /// <summary>The secret a file holds. <paramref name="reference"/> says how the setting named it, for the message.</summary>
    public static string ReadFile(string path, string setting, string reference = "")
    {
        string text;
        try
        {
            text = File.ReadAllText(path).Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            // The exception's own message names the path.
            throw new InvalidOperationException($"{setting} names a file{reference} that cannot be read: {ex.GetType().Name}.");
        }
        return text.Length > 0 ? text : throw new InvalidOperationException($"{setting} names a file{reference} that is empty.");
    }
}
