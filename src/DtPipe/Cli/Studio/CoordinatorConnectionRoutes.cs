using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The coordinator's connection: what the page may set (an origin, how the person signs in, a key), tested before it is made and
/// remembered only when the person asks. The key travels from the page in a request body and is never sent back: no answer, no
/// refusal and no log of these routes holds it, and it is kept in the keyring only under the alias of its own origin. A change
/// is refused while a publication or another exchange holds the link, or an engine action runs, so nothing is closed under it.
/// </summary>
internal sealed class CoordinatorConnectionRoutes(
    CoordinatorSlot coordinator,
    EngineSlot engineSlot,
    StudioSettings? settings)
{
    private sealed record CoordinatorConnectionRequest(string? Origin, string? Auth, string? Key, bool Remember);

    private sealed record CoordinatorEntryRequest(string? Origin);

    private sealed record PreparedCoordinator(string Origin, string Auth, string? Typed, string? Stored)
    {
        public string? Key => Typed ?? Stored;
    }

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/coordinator/connection", context => StudioHttp.JsonAsync(context, View()), RouteLocks.None);
        routes.Map("POST", "/api/coordinator/connection/test", TestAsync, RouteLocks.None);
        routes.Map("POST", "/api/coordinator/connection/disconnect", DisconnectAsync, RouteLocks.None);
        routes.Map("PUT", "/api/coordinator/connection", ConnectAsync, RouteLocks.None);
        routes.Map("DELETE", "/api/coordinator/connection/history", ForgetAsync, RouteLocks.None);
    }

    private StudioSettings SettingsOrRefuse() =>
        settings ?? throw new StudioRefusal(404, "This studio cannot connect a coordinator from the page.");

    private async Task TestAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await TestCoordinatorAsync(await StudioHttp.BodyAsync<CoordinatorConnectionRequest>(context), context.RequestAborted));

    private async Task DisconnectAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await DisconnectCoordinatorAsync(context.RequestAborted));

    private async Task ConnectAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await ConnectCoordinatorAsync(await StudioHttp.BodyAsync<CoordinatorConnectionRequest>(context), context.RequestAborted));

    private async Task ForgetAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, ForgetCoordinatorEntry(await StudioHttp.BodyAsync<CoordinatorEntryRequest>(context)));

    /// <summary>The connection as the page reads it: never the key, only whether one is kept and where.</summary>
    private object View(string? warning = null, bool? keyringUsable = null)
    {
        var connection = coordinator.Connection;
        var link = coordinator.Current;
        var status = link?.Status();
        var (history, settingsWarning, historyKeyring) = History();
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
            keyring = keyringUsable ?? historyKeyring,
            canConnect = settings is not null,
            history,
            settingsWarning,
            warning,
        };
    }

    /// <summary>
    /// The recent coordinators, the most recent first, each with whether a key is kept for its origin: the keyring's aliases are
    /// listed, never its values. The coordinator in place is not listed (it is a display rule: the file keeps its entry); after a
    /// disconnection it is.
    /// </summary>
    private (object[] History, string? SettingsWarning, bool KeyringUsable) History()
    {
        var inPlace = coordinator.Current is not null && coordinator.Connection is { } current
            ? new CoordinatorSettings(current.Origin, current.Auth)
            : null;
        return StudioConnectionHelpers.ProjectHistory(
            settings,
            (StudioSettings s, out string? w) => s.ReadCoordinators(out w),
            c => inPlace is not null && StudioSettings.SameCoordinator(c, inPlace),
            _ => true,
            (c, kept) => new
            {
                origin = StudioSettings.Shown(c.Origin),
                auth = c.Auth,
                keyKept = kept.Contains(StudioSecretStore.CoordinatorAlias(c.Origin)),
            });
    }

    private PreparedCoordinator PrepareCoordinator(CoordinatorConnectionRequest request, StudioSettings store)
    {
        var auth = request.Auth?.Trim().ToLowerInvariant();
        if (auth is not ("browser" or "key")) throw new StudioRefusal(400, "A coordinator signs people in through the browser or with a key.");
        var origin = StudioConnectionHelpers.ParseHttpAddress(request.Origin, rejectUserInfo: true, originOnly: true);
        // A key typed for a browser sign-in is dropped; no key typed uses the one kept for this very origin.
        var typed = auth == "key" && !string.IsNullOrEmpty(request.Key) ? request.Key : null;
        var stored = auth == "key" && typed is null ? store.Secrets.ReadCoordinatorKey(origin) : null;
        if (auth == "key" && typed is null && stored is null)
            throw new StudioRefusal(400, "A key is needed: paste the key the coordinator made for you.");
        return new PreparedCoordinator(origin, auth, typed, stored);
    }

    private static CoordinatorLink NewCoordinatorLink(PreparedCoordinator prepared)
    {
        try { return new CoordinatorLink(new Uri(prepared.Origin), key: prepared.Key); }
        catch (CoordinatorLinkException ex) { throw new StudioRefusal(400, StudioConnectionHelpers.Mask(ex.Message, prepared.Key)); }
    }

    private async Task<object> TestCoordinatorAsync(CoordinatorConnectionRequest request, CancellationToken ct)
    {
        var prepared = PrepareCoordinator(request, SettingsOrRefuse());
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
            throw new StudioRefusal(502, StudioConnectionHelpers.Mask(StudioHttp.Shown(ex), prepared.Key));
        }
    }

    /// <summary>Whether the address is a coordinator that answers, with the checks a sign-in makes, within <see cref="CoordinatorLink.ProbeBound"/>.</summary>
    private static async Task<bool> RecognisedAsync(CoordinatorLink link, CancellationToken ct)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bound.CancelAfter(CoordinatorLink.ProbeBound);
        try
        {
            await link.CheckAsync(bound.Token);
            return true;
        }
        catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException || ex is OperationCanceledException && !ct.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task<object> ConnectCoordinatorAsync(CoordinatorConnectionRequest request, CancellationToken ct)
    {
        var store = SettingsOrRefuse();
        var prepared = PrepareCoordinator(request, store);
        var initialKeyState = StudioConnectionHelpers.InitialKeyState(prepared.Typed, prepared.Stored);
        var link = NewCoordinatorLink(prepared);
        bool recognised;
        try
        {
            // A coordinator joins the recent ones only once it answered as one: asked before the link goes in place, and only to be remembered.
            recognised = request.Remember && await RecognisedAsync(link, ct);
            using (engineSlot.Take("A change of coordinator"))
                await coordinator.ReplaceAsync(link, new CoordinatorConnection(prepared.Origin, prepared.Auth, "page", initialKeyState), signIn: prepared.Auth == "key", ct);
        }
        catch
        {
            link.Dispose();
            throw;
        }

        var outcome = StudioConnectionHelpers.RememberConnection(
            request.Remember,
            initialKeyState,
            prepared.Typed,
            () => store.WriteCoordinator(new CoordinatorSettings(prepared.Origin, prepared.Auth), remember: recognised),
            key => store.Secrets.StoreCoordinatorKey(prepared.Origin, key));
        coordinator.UpdateDescription((current, conn) => ReferenceEquals(current, link) && conn is not null ? conn with { Key = outcome.KeyState } : conn);
        return View(outcome.Warning, outcome.KeyringUsable);
    }

    /// <summary>
    /// Closes the link for this session and keeps the description of the connection, with the memory and the keyring's key as they
    /// are: the next start connects it again. A key held by the link alone went with it.
    /// </summary>
    private async Task<object> DisconnectCoordinatorAsync(CancellationToken ct)
    {
        SettingsOrRefuse();
        if (coordinator.Current is null) return View();
        var connection = coordinator.Connection;
        using (engineSlot.Take("A change of coordinator"))
            await coordinator.ReplaceAsync(null, connection is null ? null : connection with { Key = connection.Key == "stored" ? "stored" : "none", NeedsKey = false }, signIn: false, ct);
        return View();
    }

    /// <summary>
    /// Removes a recent coordinator by its origin, and the key kept for it. A connection in place is not closed: it goes on for this
    /// session, a key the keyring held is now held by the link alone, and the next start no longer connects it.
    /// </summary>
    private object ForgetCoordinatorEntry(CoordinatorEntryRequest request)
    {
        var store = SettingsOrRefuse();
        var origin = StudioConnectionHelpers.ParseHttpAddress(request.Origin);
        store.RemoveCoordinator(origin);
        var targetShown = StudioSettings.Shown(origin);
        coordinator.UpdateDescription((link, connection) => StudioConnectionHelpers.ClearForgottenDescription(
            link,
            connection,
            c => StudioSettings.Shown(c.Origin) == targetShown,
            c => c.Source,
            c => c.Key,
            (c, source, key) => c with { Source = source, Key = key }));
        return View();
    }
}
