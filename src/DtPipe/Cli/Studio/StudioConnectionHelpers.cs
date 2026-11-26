namespace DtPipe.Cli.Studio;

internal delegate IReadOnlyList<T> HistoryReader<T>(StudioSettings settings, out string? warning);

internal readonly record struct RememberOutcome(string KeyState, string? Warning, bool? KeyringUsable);

/// <summary>
/// Shared flows between agent and coordinator connection endpoints: address validation, key masking,
/// history projection with <c>keyKept</c>, remember-on-save handling, and forgetting active descriptions.
/// </summary>
internal static class StudioConnectionHelpers
{
    /// <summary>A failure's words without the key it was made with, whatever the server echoed.</summary>
    public static string Mask(string text, string? key) =>
        string.IsNullOrEmpty(key) ? text : text.Replace(key, "***", StringComparison.Ordinal);

    /// <summary>Validates an HTTP/HTTPS address (optionally enforcing no userinfo, no query/fragment, or origin-only).</summary>
    public static string ParseHttpAddress(string? raw, bool rejectUserInfo = false, bool rejectQueryOrFragment = false, bool originOnly = false)
    {
        var text = raw?.Trim() ?? "";
        if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new StudioRefusal(400, "The address is an absolute http or https address.");
        if (rejectUserInfo && uri.UserInfo.Length > 0)
            throw new StudioRefusal(400, "An address holds no credential: the key is a field of its own.");
        if (originOnly && (uri.AbsolutePath != "/" || uri.Query.Length > 0 || uri.Fragment.Length > 0))
            throw new StudioRefusal(400, "A coordinator's address is its origin: http(s)://host[:port].");
        if (rejectQueryOrFragment && (uri.Query.Length > 0 || uri.Fragment.Length > 0))
            throw new StudioRefusal(400, "An address holds no query or fragment.");
        return originOnly ? uri.GetLeftPart(UriPartial.Authority) : text;
    }

    public static string InitialKeyState(string? typed, string? stored) =>
        typed is not null ? "session" : stored is not null ? "stored" : "none";

    /// <summary>
    /// Projects recent entries excluding the active connection, listing keyring aliases only when at least one
    /// remaining entry requires a key check.
    /// </summary>
    public static (object[] History, string? SettingsWarning, bool KeyringUsable) ProjectHistory<T>(
        StudioSettings? settings,
        HistoryReader<T> read,
        Func<T, bool> isInPlace,
        Func<T, bool> needsKeyCheck,
        Func<T, IReadOnlySet<string>, object> project)
    {
        if (settings is null) return ([], null, true);
        var recent = read(settings, out var settingsWarning).Where(e => !isInPlace(e)).ToList();
        if (recent.Count == 0) return ([], settingsWarning, true);
        var keyringUsable = true;
        IReadOnlySet<string> kept = recent.Any(needsKeyCheck)
            ? settings.Secrets.KeptAliases(out keyringUsable)
            : new HashSet<string>();
        var history = recent.Select(e => project(e, kept)).ToArray();
        return (history, settingsWarning, keyringUsable);
    }

    /// <summary>
    /// Persists connection settings and optionally stores the typed key in the keyring, reporting user-facing
    /// warnings without failing the active session if settings or keyring writes fail.
    /// </summary>
    public static RememberOutcome RememberConnection(
        bool remember,
        string initialKeyState,
        string? typedKey,
        Action writeSettings,
        Func<string, bool> storeKey)
    {
        if (!remember) return new RememberOutcome(initialKeyState, null, null);
        try
        {
            writeSettings();
            if (typedKey is null) return new RememberOutcome(initialKeyState, null, null);
            if (storeKey(typedKey)) return new RememberOutcome("stored", null, true);
            return new RememberOutcome(initialKeyState, "The keyring cannot keep the key: it lasts for this session only.", false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return new RememberOutcome(initialKeyState, "The connection is made for this session, but the settings file cannot be written: nothing is remembered.", null);
        }
    }

    /// <summary>
    /// Updates the slot's connection description when a forgotten entry matches the active one: leaves it running
    /// for the session and demotes stored/remembered metadata to session/page.
    /// </summary>
    public static TConn? ClearForgottenDescription<TActive, TConn>(
        TActive? active,
        TConn? connection,
        Func<TConn, bool> matches,
        Func<TConn, string> getSource,
        Func<TConn, string> getKey,
        Func<TConn, string, string, TConn> withDemoted)
        where TActive : class
        where TConn : class
    {
        if (connection is null || !matches(connection)) return connection;
        if (active is null) return null;
        var source = getSource(connection);
        var key = getKey(connection);
        return withDemoted(connection, source == "remembered" ? "page" : source, key == "stored" ? "session" : key);
    }
}
