using Microsoft.Extensions.Configuration;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The git repository the library is pushed to (<c>Coordinator:Library:Remote</c>): an <c>https://</c> URL, a <c>file://</c> URL or an
/// absolute path, never SSH. <see cref="Url"/> carries no identifier and no query, so it is safe to show and to log.
/// <see cref="Token"/> is a secret: it is read from the environment (<c>Coordinator__Library__Remote__Token</c>), never from a
/// command-line argument, which any process can list, and it reaches git only in the environment of the one process that talks
/// to the remote, as a Basic authorization header for <see cref="User"/>. <see cref="Timeout"/> bounds each call to the remote.
/// </summary>
public sealed record LibraryRemoteSettings(string Url, string? Token = null, string User = "git")
{
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>The settings in <paramref name="section"/> (<c>Coordinator</c>), or null when no remote is configured.</summary>
    public static LibraryRemoteSettings? FromConfiguration(IConfigurationSection section)
    {
        var url = section["Library:Remote:Url"];
        var token = section["Library:Remote:Token"] is { Length: > 0 } secret ? secret : null;
        if (string.IsNullOrEmpty(url))
            return token is null ? null : throw new InvalidOperationException("Coordinator:Library:Remote:Token is set without Coordinator:Library:Remote:Url.");
        // No message echoes the URL: whatever was misplaced in it is what must not be shown.
        if (!IsSupported(url, out var https))
            throw new InvalidOperationException("Coordinator:Library:Remote:Url must be an https:// URL, a file:// URL or an absolute path (SSH is not supported), with no user, password, query or fragment.");
        if (token is not null && !https)
            throw new InvalidOperationException("Coordinator:Library:Remote:Token is for an https:// remote: over any other it would travel in clear or be unused.");
        return new LibraryRemoteSettings(url, token, section["Library:Remote:User"] is { Length: > 0 } user ? user : "git")
        {
            Timeout = section.GetValue<TimeSpan?>("Library:Remote:Timeout") is { } timeout && timeout > TimeSpan.Zero ? timeout : TimeSpan.FromSeconds(15),
        };
    }

    private static bool IsSupported(string url, out bool https)
    {
        https = false;
        if (Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" or "file" && uri.IsAbsoluteUri)
        {
            https = uri.Scheme == "https";
            return uri.UserInfo.Length == 0 && uri.Query.Length == 0 && uri.Fragment.Length == 0 && !url.Contains('@');
        }
        // `git@host:path` and `ssh://` are neither a URL of those schemes nor a rooted path.
        return !url.Contains("://") && Path.IsPathRooted(url);
    }
}
