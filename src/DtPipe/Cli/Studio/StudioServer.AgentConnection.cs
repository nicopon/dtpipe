using DtPipe.Cli.Agent;
using Microsoft.AspNetCore.Http;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The agent's connection: what the page may set (a provider, an address, a model, a key), tested before it is made and
/// remembered only when the person asks. The key travels from the page in a request body and is never sent back: no answer, no
/// refusal and no log of these routes holds it, and it is kept in the keyring only under the alias of its own address.
/// </summary>
public sealed partial class StudioServer
{
    private sealed record AgentConnectionRequest(string? Provider, string? Url, string? Model, int? Timeout, int? NumCtx, string? Key, bool Remember);

    private sealed record AgentEntryRequest(string? Provider, string? Url);

    private sealed record PreparedConnection(string Provider, string Url, string? Model, TimeSpan Timeout, int? NumCtx, string? Typed, string? Stored, ILlmClient Client)
    {
        public string? Key => Typed ?? Stored;
    }

    private async Task ConnectionRouteAsync(HttpContext context, string method, string path)
    {
        switch (method, path)
        {
            case ("GET", "/api/agent/connection"):
                await JsonAsync(context, ConnectionView());
                break;
            case ("POST", "/api/agent/connection/test"):
                await JsonAsync(context, await TestConnectionAsync(await BodyAsync<AgentConnectionRequest>(context), context.RequestAborted));
                break;
            case ("POST", "/api/agent/connection/disconnect"):
                await _slot.CloseAsync();
                await JsonAsync(context, ConnectionView());
                break;
            case ("PUT", "/api/agent/connection"):
                await JsonAsync(context, await ConnectAsync(await BodyAsync<AgentConnectionRequest>(context)));
                break;
            case ("DELETE", "/api/agent/connection/history"):
                await JsonAsync(context, ForgetAgentEntry(await BodyAsync<AgentEntryRequest>(context)));
                break;
            default:
                throw new StudioRefusal(404, $"No {method} {path}.");
        }
    }

    private StudioAgentLink AgentLinkOrRefuse() =>
        _tools.AgentLink ?? throw new StudioRefusal(404, "This studio cannot connect a model from the page.");

    /// <summary>The connection as the page reads it: never the key, only whether one is kept and where.</summary>
    private object ConnectionView(string? warning = null)
    {
        var connection = _slot.Connection;
        var agent = Agent;
        var history = AgentHistory();
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
            keyring = _tools.AgentLink?.Settings.KeyringUsable ?? true,
            canConnect = _tools.AgentLink is not null,
            history,
            settingsWarning = _tools.AgentLink?.Settings.Warning,
            warning,
        };
    }

    /// <summary>
    /// The recent agents, the most recent first, each with whether a key is kept for its address: the keyring's aliases are listed,
    /// never its values. The agent in place is not listed (it is a display rule: the file keeps its entry); after a disconnection
    /// it is.
    /// </summary>
    private object[] AgentHistory()
    {
        if (_tools.AgentLink?.Settings is not { } settings) return [];
        var inPlace = Agent is not null && _slot.Connection is { } current ? new AgentSettings(current.Provider, current.Url, null, null) : null;
        var recent = settings.ReadAgents().Where(a => inPlace is null || !StudioSettings.SameAgent(a, inPlace)).ToList();
        if (recent.Count == 0) return [];
        var kept = settings.KeptAliases();
        return recent.Select(a => (object)new
        {
            provider = a.Provider, url = StudioSettings.Shown(a.Url), model = a.Model, timeout = a.Timeout, numCtx = a.NumCtx,
            keyKept = kept.Contains(StudioSettings.LlmAlias(a.Url)),
        }).ToArray();
    }

    private PreparedConnection Prepare(AgentConnectionRequest request, StudioAgentLink link)
    {
        var provider = request.Provider?.Trim().ToLowerInvariant();
        if (provider is not ("ollama" or "openai")) throw new StudioRefusal(400, "A provider is ollama or openai.");
        var url = request.Url?.Trim() ?? "";
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new StudioRefusal(400, "The address is an absolute http or https address.");
        if (uri.UserInfo.Length > 0) throw new StudioRefusal(400, "An address holds no credential: the key is a field of its own.");
        var timeout = request.Timeout is > 0 ? TimeSpan.FromSeconds(request.Timeout.Value) : AgentOptions.DefaultLlmTimeout;
        // Only Ollama takes a window: another provider drops it, as a key typed for Ollama is dropped.
        if (provider == "ollama" && request.NumCtx is { } window && !StudioAgentClients.IsNumCtx(window))
            throw new StudioRefusal(400, $"The context size is a whole number of tokens from {StudioAgentClients.MinNumCtx} to {StudioAgentClients.MaxNumCtx}.");
        var numCtx = provider == "ollama" ? request.NumCtx : null;
        // A key typed for a provider that takes none is dropped; no key typed uses the one kept for this very address.
        var typed = provider == "openai" && !string.IsNullOrEmpty(request.Key) ? request.Key : null;
        var stored = provider == "openai" && typed is null ? link.Settings.ReadLlmKey(url) : null;
        var client = link.NewClient(provider, typed ?? stored, timeout) ?? throw new StudioRefusal(400, "A provider is ollama or openai.");
        var model = string.IsNullOrWhiteSpace(request.Model) ? null : request.Model.Trim();
        return new PreparedConnection(provider, url, model, timeout, numCtx, typed, stored, client);
    }

    /// <summary>A failure's words without the key it was made with, whatever the server echoed.</summary>
    private static string Mask(string text, string? key) =>
        string.IsNullOrEmpty(key) ? text : text.Replace(key, "***", StringComparison.Ordinal);

    private async Task<object> TestConnectionAsync(AgentConnectionRequest request, CancellationToken ct)
    {
        var prepared = Prepare(request, AgentLinkOrRefuse());
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
            throw new StudioRefusal(502, "The model's server did not list its models: " + Mask(Shown(ex), prepared.Key));
        }
    }

    private async Task<object> ConnectAsync(AgentConnectionRequest request)
    {
        var link = AgentLinkOrRefuse();
        var prepared = Prepare(request, link);
        var keyState = prepared.Typed is not null ? "session" : prepared.Stored is not null ? "stored" : "none";
        var timeoutSeconds = (int)prepared.Timeout.TotalSeconds;
        await _slot.ReplaceAsync(
            () => NewAgent(new StudioAgentSetup(prepared.Client, prepared.Url, prepared.Model, link.Tools, link.Trace, prepared.NumCtx)),
            new AgentConnection(prepared.Provider, prepared.Url, prepared.Model, timeoutSeconds, "page", keyState, NumCtx: prepared.NumCtx));

        string? warning = null;
        if (request.Remember)
        {
            try
            {
                link.Settings.WriteAgent(new AgentSettings(prepared.Provider, prepared.Url, prepared.Model, timeoutSeconds, prepared.NumCtx));
                if (prepared.Typed is not null && link.Settings.StoreLlmKey(prepared.Url, prepared.Typed)) keyState = "stored";
                else if (prepared.Typed is not null) warning = "The keyring cannot keep the key: it lasts for this session only.";
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                warning = "The connection is made for this session, but the settings file cannot be written: nothing is remembered.";
            }
        }
        _slot.Describe(new AgentConnection(prepared.Provider, prepared.Url, prepared.Model, timeoutSeconds, "page", keyState, NumCtx: prepared.NumCtx));
        return ConnectionView(warning);
    }

    /// <summary>
    /// Removes a recent agent by its provider and address, and the key kept for the address. The agent in place is not closed: it
    /// goes on for this session, a key the keyring held is now held by the client alone, and the next start no longer connects it.
    /// </summary>
    private object ForgetAgentEntry(AgentEntryRequest request)
    {
        var link = AgentLinkOrRefuse();
        var provider = request.Provider?.Trim().ToLowerInvariant();
        if (provider is not ("ollama" or "openai")) throw new StudioRefusal(400, "A provider is ollama or openai.");
        var url = request.Url?.Trim() ?? "";
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new StudioRefusal(400, "The address is an absolute http or https address.");
        link.Settings.RemoveAgent(provider, url);
        var connection = _slot.Connection;
        if (connection is not null && connection.Provider == provider && StudioSettings.Shown(connection.Url) == StudioSettings.Shown(url))
        {
            if (Agent is null) _slot.Describe(null);
            else _slot.Describe(connection with { Source = connection.Source == "remembered" ? "page" : connection.Source, Key = connection.Key == "stored" ? "session" : connection.Key });
        }
        return ConnectionView();
    }
}
