using DtPipe.Cli.Agent;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The agent's connection: what the page may set (a provider, an address, a model, a key), tested before it is made and
/// remembered only when the person asks. The key travels from the page in a request body and is never sent back: no answer, no
/// refusal and no log of these routes holds it, and it is kept in the keyring only under the alias of its own address.
/// </summary>
internal sealed class AgentConnectionRoutes(
    AgentSlot agentSlot,
    StudioAgentLink? agentLink,
    StudioSettings? settings,
    Func<StudioAgentSetup, StudioAgent> createAgent)
{
    private sealed record AgentConnectionRequest(string? Provider, string? Url, string? Model, int? Timeout, int? NumCtx, string? Key, bool Remember);

    private sealed record AgentEntryRequest(string? Provider, string? Url);

    private sealed record PreparedConnection(string Provider, string Url, string? Model, TimeSpan Timeout, int? NumCtx, string? Typed, string? Stored, ILlmClient Client)
    {
        public string? Key => Typed ?? Stored;
    }

    public void Register(StudioRouteTable routes)
    {
        routes.Map("GET", "/api/agent/connection", context => StudioHttp.JsonAsync(context, View()), RouteLocks.None);
        routes.Map("POST", "/api/agent/connection/test", TestAsync, RouteLocks.None);
        routes.Map("POST", "/api/agent/connection/disconnect", DisconnectAsync, RouteLocks.None);
        routes.Map("PUT", "/api/agent/connection", ConnectAsync, RouteLocks.None);
        routes.Map("DELETE", "/api/agent/connection/history", ForgetAsync, RouteLocks.None);
    }

    private (StudioAgentLink Link, StudioSettings Settings) LinkAndSettingsOrRefuse() =>
        agentLink is not null && settings is not null
            ? (agentLink, settings)
            : throw new StudioRefusal(404, "This studio cannot connect a model from the page.");

    private async Task TestAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await TestConnectionAsync(await StudioHttp.BodyAsync<AgentConnectionRequest>(context), context.RequestAborted));

    private async Task DisconnectAsync(HttpContext context)
    {
        await agentSlot.CloseAsync();
        await StudioHttp.JsonAsync(context, View());
    }

    private async Task ConnectAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, await ConnectConnectionAsync(await StudioHttp.BodyAsync<AgentConnectionRequest>(context)));

    private async Task ForgetAsync(HttpContext context) =>
        await StudioHttp.JsonAsync(context, ForgetAgentEntry(await StudioHttp.BodyAsync<AgentEntryRequest>(context)));

    /// <summary>The connection as the page reads it: never the key, only whether one is kept and where.</summary>
    private object View(string? warning = null, bool? keyringUsable = null)
    {
        var connection = agentSlot.Connection;
        var agent = agentSlot.Current;
        var (history, settingsWarning, historyKeyring) = History();
        return new
        {
            connected = agent is not null,
            needsKey = agent is null && connection?.NeedsKey == true,
            source = connection?.Source ?? "none",
            provider = connection?.Provider,
            url = connection is null ? null : StudioSettings.Shown(connection.Url),
            model = agent?.Model ?? connection?.Model,
            timeout = connection?.TimeoutSeconds,
            numCtx = connection?.Provider == "ollama" ? connection.NumCtx ?? AgentOptions.DefaultNumCtx : (int?)null,
            numCtxBounds = new { min = StudioAgentClients.MinNumCtx, max = StudioAgentClients.MaxNumCtx, @default = AgentOptions.DefaultNumCtx },
            key = connection?.Key ?? "none",
            keyring = keyringUsable ?? historyKeyring,
            canConnect = agentLink is not null && settings is not null,
            history,
            settingsWarning,
            warning,
        };
    }

    /// <summary>
    /// The recent agents, the most recent first, each with whether a key is kept for its address: the keyring's aliases are listed,
    /// never its values. The agent in place is not listed (it is a display rule: the file keeps its entry); after a disconnection
    /// it is.
    /// </summary>
    private (object[] History, string? SettingsWarning, bool KeyringUsable) History()
    {
        var inPlace = agentSlot.Current is not null && agentSlot.Connection is { } current
            ? new AgentSettings(current.Provider, current.Url, null, null)
            : null;
        return StudioConnectionHelpers.ProjectHistory(
            agentLink is null ? null : settings,
            (StudioSettings s, out string? w) => s.ReadAgents(out w),
            a => inPlace is not null && StudioSettings.SameAgent(a, inPlace),
            a => a.Provider == "openai",
            (a, kept) => new
            {
                provider = a.Provider,
                url = StudioSettings.Shown(a.Url),
                model = a.Model,
                timeout = a.Timeout,
                numCtx = a.NumCtx,
                keyKept = a.Provider == "openai" && kept.Contains(StudioSecretStore.LlmAlias(a.Url)),
            });
    }

    private static PreparedConnection Prepare(AgentConnectionRequest request, StudioAgentLink link, StudioSettings store)
    {
        var provider = request.Provider?.Trim().ToLowerInvariant();
        if (provider is not ("ollama" or "openai")) throw new StudioRefusal(400, "A provider is ollama or openai.");
        var url = StudioConnectionHelpers.ParseHttpAddress(request.Url, rejectUserInfo: true, rejectQueryOrFragment: true);
        var timeout = request.Timeout is > 0 ? TimeSpan.FromSeconds(request.Timeout.Value) : AgentOptions.DefaultLlmTimeout;
        // Only Ollama takes a window: another provider drops it, as a key typed for Ollama is dropped.
        if (provider == "ollama" && request.NumCtx is { } window && !StudioAgentClients.IsNumCtx(window))
            throw new StudioRefusal(400, $"The context size is a whole number of tokens from {StudioAgentClients.MinNumCtx} to {StudioAgentClients.MaxNumCtx}.");
        var numCtx = provider == "ollama" ? request.NumCtx : null;
        // A key typed for a provider that takes none is dropped; no key typed uses the one kept for this very address.
        var typed = provider == "openai" && !string.IsNullOrEmpty(request.Key) ? request.Key : null;
        var stored = provider == "openai" && typed is null ? store.Secrets.ReadLlmKey(url) : null;
        var client = link.NewClient(provider, typed ?? stored, timeout) ?? throw new StudioRefusal(400, "A provider is ollama or openai.");
        var model = string.IsNullOrWhiteSpace(request.Model) ? null : request.Model.Trim();
        return new PreparedConnection(provider, url, model, timeout, numCtx, typed, stored, client);
    }

    private async Task<object> TestConnectionAsync(AgentConnectionRequest request, CancellationToken ct)
    {
        var (link, store) = LinkAndSettingsOrRefuse();
        var prepared = Prepare(request, link, store);
        try
        {
            return new { models = await prepared.Client.ListModelsAsync(prepared.Url, ct) };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new StudioRefusal(502, "The model's server did not list its models: " + StudioConnectionHelpers.Mask(StudioHttp.Shown(ex), prepared.Key));
        }
    }

    private async Task<object> ConnectConnectionAsync(AgentConnectionRequest request)
    {
        var (link, store) = LinkAndSettingsOrRefuse();
        var prepared = Prepare(request, link, store);
        var initialKeyState = StudioConnectionHelpers.InitialKeyState(prepared.Typed, prepared.Stored);
        var timeoutSeconds = (int)prepared.Timeout.TotalSeconds;
        var installed = await agentSlot.ReplaceAsync(
            () => createAgent(new StudioAgentSetup(prepared.Client, prepared.Url, prepared.Model, link.Tools, link.Trace, prepared.NumCtx)),
            new AgentConnection(prepared.Provider, prepared.Url, prepared.Model, timeoutSeconds, "page", initialKeyState, NumCtx: prepared.NumCtx));

        var outcome = StudioConnectionHelpers.RememberConnection(
            request.Remember,
            initialKeyState,
            prepared.Typed,
            () => store.WriteAgent(new AgentSettings(prepared.Provider, prepared.Url, prepared.Model, timeoutSeconds, prepared.NumCtx)),
            key => store.Secrets.StoreLlmKey(prepared.Url, key));
        agentSlot.UpdateDescription((agent, conn) => ReferenceEquals(agent, installed) && conn is not null ? conn with { Key = outcome.KeyState } : conn);
        return View(outcome.Warning, outcome.KeyringUsable);
    }

    /// <summary>
    /// Removes a recent agent by its provider and address, and the key kept for the address. The agent in place is not closed: it
    /// goes on for this session, a key the keyring held is now held by the client alone, and the next start no longer connects it.
    /// </summary>
    private object ForgetAgentEntry(AgentEntryRequest request)
    {
        var (_, store) = LinkAndSettingsOrRefuse();
        var provider = request.Provider?.Trim().ToLowerInvariant();
        if (provider is not ("ollama" or "openai")) throw new StudioRefusal(400, "A provider is ollama or openai.");
        var url = StudioConnectionHelpers.ParseHttpAddress(request.Url);
        store.RemoveAgent(provider, url);
        var targetShown = StudioSettings.Shown(url);
        agentSlot.UpdateDescription((agent, connection) => StudioConnectionHelpers.ClearForgottenDescription(
            agent,
            connection,
            c => c.Provider == provider && StudioSettings.Shown(c.Url) == targetShown,
            c => c.Source,
            c => c.Key,
            (c, source, key) => c with { Source = source, Key = key }));
        return View();
    }
}
