namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// A secret a host gives the node in a setting: <c>@path</c> is the content of that file without the white space around it, which a
/// container reads from a mounted secret, and any other text is the secret itself. Read once, when the node starts. No message names
/// what it read, nor the path, which could be a key pasted after the <c>@</c> by mistake. A secret that begins with <c>@</c> is given
/// in a file.
/// </summary>
internal static class SecretReference
{
    public static string Read(string value, string setting)
    {
        if (!value.StartsWith('@')) return value;
        string text;
        try
        {
            text = File.ReadAllText(value[1..]).Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            // The exception's own message names the path.
            throw new InvalidOperationException($"{setting} names a file (@…) that cannot be read: {ex.GetType().Name}.");
        }
        return text.Length > 0 ? text : throw new InvalidOperationException($"{setting} names a file (@…) that is empty.");
    }
}
