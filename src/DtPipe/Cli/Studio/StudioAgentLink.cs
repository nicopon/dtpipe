using DtPipe.Cli.Agent;

namespace DtPipe.Cli.Studio;

/// <summary>
/// Where the agent's connection stands, as the page reads it. <see cref="Source"/> is <c>flag</c>, <c>remembered</c> or
/// <c>page</c>; <see cref="Key"/> is <c>none</c>, <c>stored</c> (in the keyring), <c>flag</c> or <c>session</c> (held by this
/// studio alone). <see cref="NeedsKey"/>: the connection was remembered but its key is not to be found, so no agent runs yet.
/// <see cref="NumCtx"/> is the context window asked of a provider that takes one; null is <see cref="AgentOptions.DefaultNumCtx"/>.
/// </summary>
public sealed record AgentConnection(string Provider, string Url, string? Model, int TimeoutSeconds, string Source, string Key, bool NeedsKey = false, int? NumCtx = null);

/// <summary>
/// What the page may do to the agent: the settings it remembers, the client a provider and a key make, and what every agent of
/// this studio shares (the tools, the trace). <see cref="Start"/> describes the connection the studio started with.
/// </summary>
public sealed record StudioAgentLink(
    StudioSettings Settings, IAgentToolProvider Tools, AgentTrace? Trace, Func<string, string?, TimeSpan, ILlmClient?> NewClient, AgentConnection? Start = null);

/// <summary>The clients the studio knows how to make, by provider name.</summary>
public static class StudioAgentClients
{
    public static ILlmClient? Create(string provider, string? key, TimeSpan timeout) => provider.ToLowerInvariant() switch
    {
        "ollama" => new OllamaClient(timeout),
        "openai" => new OpenAiClient(key ?? "", timeout),
        _ => null,
    };

    public static string DefaultUrl(string provider) => provider == "openai" ? "https://api.openai.com" : "http://localhost:11434";

    /// <summary>The bounds of the context window a connection may ask for, in tokens: below the lower one the tool catalogue alone does not fit.</summary>
    public const int MinNumCtx = 4096;

    public const int MaxNumCtx = 1_048_576;

    public static bool IsNumCtx(int value) => value is >= MinNumCtx and <= MaxNumCtx;
}

/// <summary>
/// The studio's agent, which the page may replace. A replacement closes the agent in place (refused while a turn runs or a card
/// waits) and opens a new conversation, as changing the model does.
/// </summary>
internal sealed class AgentSlot(StudioAgent? agent, AgentConnection? connection)
{
    private readonly SemaphoreSlim _swap = new(1, 1);
    private StudioAgent? _agent = agent;
    private AgentConnection? _connection = connection;

    public StudioAgent? Current => Volatile.Read(ref _agent);

    public AgentConnection? Connection => Volatile.Read(ref _connection);

    /// <summary>Another description of the connection in place, as it stands after a change of memory.</summary>
    public void Describe(AgentConnection? updated) => Volatile.Write(ref _connection, updated);

    /// <summary>Puts the agent <paramref name="create"/> makes in place of the current one, or refuses with 409 naming why it stays.</summary>
    public async Task ReplaceAsync(Func<StudioAgent> create, AgentConnection described)
    {
        await _swap.WaitAsync();
        try
        {
            var old = Current;
            var next = create();
            if (old?.TryClose() is { } reason)
            {
                await next.DisposeAsync();
                throw new StudioRefusal(409, reason);
            }
            Volatile.Write(ref _agent, next);
            Volatile.Write(ref _connection, described);
            if (old is not null) await old.DisposeAsync();
        }
        finally
        {
            _swap.Release();
        }
    }

    /// <summary>
    /// Closes the agent in place and leaves none, keeping the description of the connection, or refuses with 409 naming why it
    /// stays. Nothing is forgotten: the memory and the keyring's key are not this slot's.
    /// </summary>
    public async Task CloseAsync()
    {
        await _swap.WaitAsync();
        try
        {
            var old = Current;
            if (old is null) return;
            if (old.TryClose("disconnecting the model") is { } reason) throw new StudioRefusal(409, reason);
            Volatile.Write(ref _agent, null);
            // A key held by the client alone (flag, typed) went with it; the keyring's stays.
            if (Connection is { } held && held.Key != "stored") Volatile.Write(ref _connection, held with { Key = "none" });
            await old.DisposeAsync();
        }
        finally
        {
            _swap.Release();
        }
    }

    /// <summary>
    /// Puts the agent <paramref name="create"/> makes in place of <paramref name="closed"/>, which a change beside it closed: a
    /// new conversation. An agent the page replaced meanwhile is new already and stays.
    /// </summary>
    public async Task RenewAsync(StudioAgent closed, Func<StudioAgent> create)
    {
        await _swap.WaitAsync();
        try
        {
            if (!ReferenceEquals(Current, closed)) return;
            Volatile.Write(ref _agent, create());
        }
        finally
        {
            _swap.Release();
        }
        await closed.DisposeAsync();
    }
}
