using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The coordinator's connection: what the page may set (an origin, how the person signs in, a key), tested before it is made and
/// remembered only when the person asks. The key travels from the page in a request body and is never sent back: no answer, no
/// refusal and no log of these routes holds it, and it is kept in the keyring only under the alias of its own origin. A change
/// is refused while a publication or another exchange holds the link, or an engine action runs, so nothing is closed under it.
/// </summary>
public sealed partial class StudioServer
{
    private sealed record CoordinatorConnectionRequest(string? Origin, string? Auth, string? Key, bool Remember, bool RemoveKey);

    private sealed record PreparedCoordinator(string Origin, string Auth, string? Typed, string? Stored)
    {
        public string? Key => Typed ?? Stored;
    }

    private async Task CoordinatorConnectionRouteAsync(HttpContext context, string method, string path)
    {
        switch (method, path)
        {
            case ("GET", "/api/coordinator/connection"):
                await JsonAsync(context, CoordinatorConnectionView());
                break;
            case ("POST", "/api/coordinator/connection/test"):
                await JsonAsync(context, await TestCoordinatorAsync(await BodyAsync<CoordinatorConnectionRequest>(context), context.RequestAborted));
                break;
            case ("PUT", "/api/coordinator/connection"):
                await JsonAsync(context, await ConnectCoordinatorAsync(await BodyAsync<CoordinatorConnectionRequest>(context)));
                break;
            case ("DELETE", "/api/coordinator/connection"):
                await JsonAsync(context, await ForgetCoordinatorAsync());
                break;
            default:
                throw new StudioRefusal(404, $"No {method} {path}.");
        }
    }

    private StudioCoordinatorStart CoordinatorStartOrRefuse() =>
        _tools.CoordinatorStart ?? throw new StudioRefusal(404, "This studio cannot connect a coordinator from the page.");

    /// <summary>The connection as the page reads it: never the key, only whether one is kept and where.</summary>
    private object CoordinatorConnectionView(string? warning = null)
    {
        var connection = _coordinator.Connection;
        var link = Coordinator;
        var status = link?.Status();
        return new
        {
            connected = link is not null,
            needsKey = link is null && connection?.NeedsKey == true,
            source = connection?.Source ?? "none",
            origin = connection is null ? null : StudioSettings.Shown(connection.Origin),
            auth = connection?.Auth,
            key = connection?.Key ?? "none",
            state = status?.State,
            unreachable = status?.Unreachable,
            keyring = _tools.CoordinatorStart?.Settings.KeyringUsable ?? true,
            canConnect = _tools.CoordinatorStart is not null,
            settingsWarning = _tools.CoordinatorStart?.Settings.Warning,
            warning,
        };
    }

    private PreparedCoordinator PrepareCoordinator(CoordinatorConnectionRequest request, StudioCoordinatorStart start)
    {
        var auth = request.Auth?.Trim().ToLowerInvariant();
        if (auth is not ("browser" or "key")) throw new StudioRefusal(400, "A coordinator signs people in through the browser or with a key.");
        var text = request.Origin?.Trim() ?? "";
        if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new StudioRefusal(400, "The address is an absolute http or https address.");
        if (uri.UserInfo.Length > 0) throw new StudioRefusal(400, "An address holds no credential: the key is a field of its own.");
        if (uri.AbsolutePath != "/" || uri.Query.Length > 0 || uri.Fragment.Length > 0)
            throw new StudioRefusal(400, "A coordinator's address is its origin: http(s)://host[:port].");
        var origin = uri.GetLeftPart(UriPartial.Authority);
        // A key typed for a browser sign-in is dropped; no key typed uses the one kept for this very origin.
        var typed = auth == "key" && !string.IsNullOrEmpty(request.Key) ? request.Key : null;
        var stored = auth == "key" && typed is null && !request.RemoveKey ? start.Settings.ReadCoordinatorKey(origin) : null;
        if (auth == "key" && typed is null && stored is null)
            throw new StudioRefusal(400, "A key is needed: paste the key the coordinator made for you.");
        return new PreparedCoordinator(origin, auth, typed, stored);
    }

    private static CoordinatorLink NewCoordinatorLink(PreparedCoordinator prepared)
    {
        try { return new CoordinatorLink(new Uri(prepared.Origin), key: prepared.Key); }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(400, Mask(ex.Message, prepared.Key)); }
    }

    private async Task<object> TestCoordinatorAsync(CoordinatorConnectionRequest request, CancellationToken ct)
    {
        var prepared = PrepareCoordinator(request, CoordinatorStartOrRefuse());
        using var link = NewCoordinatorLink(prepared);
        try
        {
            await link.CheckAsync(ct);
            return new { ok = true, signedInAs = prepared.Key is null ? null : await link.SignInWithKeyAsync(ct) };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException)
        {
            throw new StudioRefusal(502, Mask(Shown(ex), prepared.Key));
        }
    }

    private async Task<object> ConnectCoordinatorAsync(CoordinatorConnectionRequest request)
    {
        var start = CoordinatorStartOrRefuse();
        var prepared = PrepareCoordinator(request, start);
        var keyState = prepared.Typed is not null ? "session" : prepared.Stored is not null ? "stored" : "none";
        var link = NewCoordinatorLink(prepared);
        try
        {
            using (TakeSlotFor("A change of coordinator"))
                await _coordinator.ReplaceAsync(link, new CoordinatorConnection(prepared.Origin, prepared.Auth, "page", keyState), signIn: prepared.Auth == "key");
        }
        catch
        {
            link.Dispose();
            throw;
        }

        string? warning = null;
        if (request.RemoveKey) start.Settings.ForgetCoordinatorKey(prepared.Origin);
        if (request.Remember)
        {
            try
            {
                start.Settings.WriteCoordinator(new CoordinatorSettings(prepared.Origin, prepared.Auth));
                if (prepared.Typed is not null && start.Settings.StoreCoordinatorKey(prepared.Origin, prepared.Typed)) keyState = "stored";
                else if (prepared.Typed is not null) warning = "The keyring cannot keep the key: it lasts for this session only.";
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                warning = "The connection is made for this session, but the settings file cannot be written: nothing is remembered.";
            }
        }
        if (request.RemoveKey) keyState = "none";
        _coordinator.Describe(new CoordinatorConnection(prepared.Origin, prepared.Auth, "page", keyState));
        return CoordinatorConnectionView(warning);
    }

    /// <summary>Disconnects, then forgets the file's coordinator and the keys of its origin and of the connection's.</summary>
    private async Task<object> ForgetCoordinatorAsync()
    {
        var start = CoordinatorStartOrRefuse();
        var connection = _coordinator.Connection;
        using (TakeSlotFor("A change of coordinator"))
            await _coordinator.ReplaceAsync(null, null, signIn: false);
        var remembered = start.Settings.ReadCoordinator();
        start.Settings.ClearCoordinator();
        foreach (var origin in new[] { remembered?.Origin, connection?.Origin }.OfType<string>().Distinct())
            start.Settings.ForgetCoordinatorKey(origin);
        return CoordinatorConnectionView();
    }
}
