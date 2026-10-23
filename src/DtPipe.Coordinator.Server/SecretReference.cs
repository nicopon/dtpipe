using Microsoft.Extensions.Configuration.CommandLine;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// A secret a deployment gives the server in a setting: <c>@path</c> is the content of that file without the white space around it, and
/// any other text is the secret itself, which only the environment may carry, since a command-line argument can be listed by any process
/// of the host. No message names what it read, nor the path, which could be a key pasted after the <c>@</c> by mistake.
/// </summary>
internal static class SecretReference
{
    /// <summary>The secret the setting <paramref name="key"/> of <paramref name="configuration"/> gives, or null when it is unset.</summary>
    public static string? Read(IConfiguration configuration, string key)
    {
        var value = configuration[key];
        if (string.IsNullOrEmpty(value)) return null;
        if (value[0] != '@')
        {
            if (configuration is IConfigurationRoot root && root.Providers.OfType<CommandLineConfigurationProvider>().Any(p => p.TryGet(key, out _)))
                throw new InvalidOperationException($"{key} is given on the command line, which any process of the host can list: set it in the environment, or as @path to a file.");
            return value;
        }
        string text;
        try
        {
            text = File.ReadAllText(value[1..]).Trim();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            // The exception's own message names the path.
            throw new InvalidOperationException($"{key} names a file (@…) that cannot be read: {ex.GetType().Name}.");
        }
        return text.Length > 0 ? text : throw new InvalidOperationException($"{key} names a file (@…) that is empty.");
    }
}
