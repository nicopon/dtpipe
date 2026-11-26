using System.CommandLine;
using System.Net.Sockets;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Node;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using DtPipe.Cli.Split;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Commands;

/// <summary>
/// <c>dtpipe studio [folder] [--coordinator url [--key reference]]</c>: a local page over the YAML jobs of a folder, sampled
/// through the same path as the MCP <c>dry-run</c> tool, and signed in to a coordinator when one is named (or remembered), through the
/// browser or with a key the coordinator made for the person. The key is a reference, never a value, read once at start. It ends when the page asks
/// to (exit 0) or on Ctrl+C (exit 130: a cancellation never reads as a success).
/// </summary>
public sealed class StudioCommand : Command
{
    private readonly Argument<string?> _folderArgument = new("folder")
    {
        Description = "The folder of jobs (default: the current directory)",
        Arity = ArgumentArity.ZeroOrOne,
    };

    private readonly Option<int> _portOption = new("--port")
    {
        Description = "The port on 127.0.0.1 (default: any free port)",
    };

    private readonly Option<string?> _coordinatorOption = new("--coordinator")
    {
        Description = "A coordinator to sign in to, by its address: its bricks and its library (default: none)",
    };

    private readonly Option<string?> _keyOption = new("--key")
    {
        Description = "Sign in to the coordinator with a key it made for you, as a reference: ${{NAME}}, keyring://alias or @file (default: through the browser)",
    };

    private readonly Option<string?> _llmProviderOption = new("--llm-provider")
    {
        Description = "Open the agent's panel on this model provider: ollama or openai (default: no agent)",
    };

    private readonly Option<string?> _llmUrlOption = new("--llm-url")
    {
        Description = "The model provider's address (default: http://localhost:11434 for ollama, https://api.openai.com for openai)",
    };

    private readonly Option<string?> _llmModelOption = new("--llm-model")
    {
        Description = "The model the agent starts with; the page may choose another the provider lists (default: none)",
    };

    private readonly Option<string?> _llmKeyOption = new("--llm-key")
    {
        Description = "The provider's API key, as a reference: ${{NAME}}, keyring://alias or @file (default: none)",
    };

    private readonly Option<int?> _llmTimeoutOption = new("--llm-timeout")
    {
        Description = "Seconds with no token before a model call fails (default: 300)",
    };

    private readonly Option<string?> _agentTraceOption = new("--agent-trace")
    {
        Description = "Record the agent's session as JSON lines; a directory gets a timestamped file (default: $DTPIPE_AGENT_TRACE, else none)",
    };

    public StudioCommand(IServiceProvider serviceProvider)
        : base("studio", "Open a local page to sample the YAML jobs of a folder through the real engine")
    {
        Arguments.Add(_folderArgument);
        Options.Add(_portOption);
        Options.Add(_coordinatorOption);
        Options.Add(_keyOption);
        Options.Add(_llmProviderOption);
        Options.Add(_llmUrlOption);
        Options.Add(_llmModelOption);
        Options.Add(_llmKeyOption);
        Options.Add(_llmTimeoutOption);
        Options.Add(_agentTraceOption);

        this.SetAction((parseResult, ct) => ExecuteAsync(parseResult, serviceProvider, ct));
    }

    private async Task<int> ExecuteAsync(ParseResult parseResult, IServiceProvider serviceProvider, CancellationToken ct)
    {
        if (!ResolveFolderAndPort(parseResult, out var folder, out var port))
            return 1;

        var secrets = serviceProvider.GetRequiredService<ISecretsManager>();
        var settings = new StudioSettings(StudioSettings.DefaultPath(), secrets);
        var remembered = settings.Read(out var warning);
        if (warning is not null) Console.Error.WriteLine($"[WARN] {warning}");

        var (link, coordinatorStart, coordFailed) = await ResolveCoordinatorAsync(parseResult, secrets, settings, remembered.Coordinator, ct);
        using var _ = link;
        if (coordFailed) return 1;

        var agentTools = new McpToolProvider(serviceProvider.GetRequiredService<DtPipeMcpTools>());
        var trace = AgentTrace.Open(parseResult.GetValue(_agentTraceOption) ?? Environment.GetEnvironmentVariable("DTPIPE_AGENT_TRACE"), out var traceFailure);
        if (traceFailure is not null) Console.Error.WriteLine($"[WARN] The agent's trace cannot be written: {traceFailure}");
        using var __ = trace;

        var (agent, start, agentFailed) = await ResolveAgentAsync(parseResult, secrets, settings, remembered.Agent, agentTools, trace, ct);
        if (agentFailed) return 1;

        // With a key the studio signs in at once; a refusal leaves it signed out with the reason, which the page shows too.
        if (link is { ByKey: true } && coordinatorStart is null)
        {
            try { Console.Error.WriteLine($"Signed in to {link.Origin.GetLeftPart(UriPartial.Authority)} as {await link.SignInWithKeyAsync(ct)}."); }
            catch (CoordinatorLinkException ex) { Console.Error.WriteLine(ex.Message); }
        }

        // A job's relative paths (a query file, a local input) resolve from where it is run:
        // from the folder, as `dtpipe --job` run there would.
        Directory.SetCurrentDirectory(folder!.Root);

        var tools = BuildTools(serviceProvider, secrets, settings, link, coordinatorStart, agent, start, agentTools, trace);
        StudioServer server;
        try
        {
            server = await StudioServer.StartAsync(folder, port, tools, ct);
        }
        catch (Exception ex) when (ex is IOException or SocketException)
        {
            Console.Error.WriteLine($"[ERROR] {ListenFailure(port, ex)}");
            return 1;
        }

        await using (server)
        {
            Console.WriteLine(server.Url);
            Console.Error.WriteLine($"Studio on {folder.Root}. Quit from the page, or Ctrl+C.");
            try
            {
                await server.Quit.WaitAsync(ct);
                return 0;
            }
            catch (OperationCanceledException)
            {
                return 130;
            }
        }
    }

    private bool ResolveFolderAndPort(ParseResult parseResult, out StudioFolder? folder, out int port)
    {
        var path = parseResult.GetValue(_folderArgument) ?? Directory.GetCurrentDirectory();
        port = parseResult.GetValue(_portOption);
        try
        {
            folder = new StudioFolder(path);
        }
        catch (DirectoryNotFoundException ex)
        {
            folder = null;
            Console.Error.WriteLine(ex.Message);
            return false;
        }
        if (port is < 0 or > 65535)
        {
            Console.Error.WriteLine($"[ERROR] --port is a port number from 0 to 65535, not {port}.");
            return false;
        }
        return true;
    }

    private async Task<(CoordinatorLink? Link, CoordinatorConnection? Start, bool Failed)> ResolveCoordinatorAsync(
        ParseResult parseResult, ISecretsManager secrets, StudioSettings settings, CoordinatorSettings? remembered, CancellationToken ct)
    {
        var coordinator = parseResult.GetValue(_coordinatorOption);
        string? key = null;
        if (parseResult.GetValue(_keyOption) is { } reference)
        {
            if (coordinator is not { Length: > 0 })
            {
                Console.Error.WriteLine("[ERROR] --key signs in to a coordinator: name it with --coordinator.");
                return (null, null, true);
            }
            string? refusal;
            (key, refusal) = await ReadKeyAsync(reference, secrets, ct);
            if (refusal is not null)
            {
                Console.Error.WriteLine($"[ERROR] {refusal}");
                return (null, null, true);
            }
        }

        if (coordinator is { Length: > 0 })
        {
            try
            {
                var uri = Uri.TryCreate(coordinator, UriKind.Absolute, out var parsed)
                    ? parsed
                    : throw new CoordinatorLinkException($"'{coordinator}' is no address.");
                return (new CoordinatorLink(uri, key: key), null, false);
            }
            catch (CoordinatorLinkException ex)
            {
                Console.Error.WriteLine(ex.Message);
                return (null, null, true);
            }
        }

        // Nothing named on the command line: the coordinator the person connected in the page and asked to remember.
        if (remembered is { } rememberedCoordinator)
        {
            var (link, start) = RememberedCoordinator(settings, rememberedCoordinator);
            return (link, start, false);
        }
        return (null, null, false);
    }

    private async Task<(StudioAgentSetup? Agent, AgentConnection? Start, bool Failed)> ResolveAgentAsync(
        ParseResult parseResult, ISecretsManager secrets, StudioSettings settings, AgentSettings? remembered,
        IAgentToolProvider agentTools, AgentTrace? trace, CancellationToken ct)
    {
        var provider = parseResult.GetValue(_llmProviderOption);
        if (provider is null)
        {
            if (new Option[] { _llmUrlOption, _llmModelOption, _llmKeyOption, _llmTimeoutOption }
                .FirstOrDefault(o => parseResult.GetResult(o) is not null) is { } orphan)
            {
                Console.Error.WriteLine($"[ERROR] {orphan.Name} sets up the agent: name its provider with --llm-provider.");
                return (null, null, true);
            }
            // Nothing named on the command line: the connection the person made in the page and asked to remember.
            var (remAgent, remStart) = Remembered(remembered, settings, agentTools, trace);
            return (remAgent, remStart, false);
        }

        string? llmKey = null;
        if (parseResult.GetValue(_llmKeyOption) is { } keyReference)
        {
            string? refusal;
            (llmKey, refusal) = NodeBricks.IsSecretReference(keyReference)
                ? await NodeBricks.ReadSecretAsync(keyReference, "key", secrets, ct)
                : (null, NodeBricks.NotAReference("--llm-key", "key", "LLM_API_KEY"));
            if (refusal is not null)
            {
                Console.Error.WriteLine($"[ERROR] {refusal}");
                return (null, null, true);
            }
        }
        var timeout = TimeSpan.FromSeconds(parseResult.GetValue(_llmTimeoutOption) is { } seconds and > 0 ? seconds : (int)AgentOptions.DefaultLlmTimeout.TotalSeconds);
        var client = StudioAgentClients.Create(provider, llmKey, timeout);
        if (client is null)
        {
            Console.Error.WriteLine($"[ERROR] --llm-provider is ollama or openai, not '{provider}'.");
            return (null, null, true);
        }
        var url = parseResult.GetValue(_llmUrlOption) ?? StudioAgentClients.DefaultUrl(client.ProviderName);
        var model = parseResult.GetValue(_llmModelOption);
        var agent = new StudioAgentSetup(client, url, model, agentTools, trace);
        var start = new AgentConnection(client.ProviderName, url, model, (int)timeout.TotalSeconds, "flag", llmKey is null ? "none" : "flag");
        return (agent, start, false);
    }

    private static StudioTools BuildTools(
        IServiceProvider serviceProvider,
        ISecretsManager secrets,
        StudioSettings settings,
        CoordinatorLink? link,
        CoordinatorConnection? coordinatorStart,
        StudioAgentSetup? agent,
        AgentConnection? agentStart,
        IAgentToolProvider agentTools,
        AgentTrace? trace)
    {
        var readers = serviceProvider.GetServices<IStreamReaderFactory>().ToList();
        var writers = serviceProvider.GetServices<IDataWriterFactory>().ToList();
        var endpoints = new ComponentEndpointRules(readers, writers);
        var engine = new StudioEngine(
            serviceProvider.GetRequiredService<JobAnalysis>(), serviceProvider.GetRequiredService<JobRunner>(),
            new DebugCutter(readers, writers, serviceProvider.GetServices<IStreamTransformerFactory>().ToList()));
        return new StudioTools(
            engine,
            new DesignService(IBrickLookup.None, endpoints),
            readers.Select(r => new StudioComponent(r.ComponentName, r.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList(),
            writers.Select(w => new StudioComponent(w.ComponentName, w.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList(),
            secrets,
            link,
            lookup => new DesignService(lookup, endpoints),
            args => CommandLineJob.Export(serviceProvider, args))
        {
            Settings = settings,
            Agent = agent,
            AgentLink = new StudioAgentLink(agentTools, trace, StudioAgentClients.Create, agentStart),
            CoordinatorStart = coordinatorStart,
        };
    }

    /// <summary>
    /// The agent the person asked the studio to remember, built from the file and the key kept for its address. A provider that
    /// needs a key and finds none builds no agent: it waits, described as needing its key, for the person to connect it again.
    /// </summary>
    private static (StudioAgentSetup? Agent, AgentConnection? Start) Remembered(AgentSettings? agent, StudioSettings settings, IAgentToolProvider tools, AgentTrace? trace)
    {
        if (agent is not { } remembered) return (null, null);
        var timeout = TimeSpan.FromSeconds(remembered.Timeout is > 0 ? remembered.Timeout.Value : (int)AgentOptions.DefaultLlmTimeout.TotalSeconds);
        var key = remembered.Provider == "openai" ? settings.Secrets.ReadLlmKey(remembered.Url) : null;
        var seconds = (int)timeout.TotalSeconds;
        if (remembered.Provider == "openai" && key is null)
        {
            Console.Error.WriteLine("[WARN] The remembered model needs its key again: connect it from the page.");
            return (null, new AgentConnection(remembered.Provider, remembered.Url, remembered.Model, seconds, "remembered", "none", NeedsKey: true, NumCtx: remembered.NumCtx));
        }
        var client = StudioAgentClients.Create(remembered.Provider, key, timeout)!;
        var window = remembered.Provider == "ollama" ? remembered.NumCtx : null;
        return (new StudioAgentSetup(client, remembered.Url, remembered.Model, tools, trace, window),
            new AgentConnection(remembered.Provider, remembered.Url, remembered.Model, seconds, "remembered", key is null ? "none" : "stored", NumCtx: window));
    }

    /// <summary>
    /// The coordinator the person asked the studio to remember, as a link and its description. A key sign-in that needs a key and
    /// finds none builds no link: it waits, described as needing its key, for the person to connect it again. It never stops the
    /// start, and its sign-in runs in the background (<c>StudioServer</c>).
    /// </summary>
    private static (CoordinatorLink? Link, CoordinatorConnection Start) RememberedCoordinator(StudioSettings settings, CoordinatorSettings remembered)
    {
        var key = remembered.Auth == "key" ? settings.Secrets.ReadCoordinatorKey(remembered.Origin) : null;
        if (remembered.Auth == "key")
        {
            try
            {
                if (key is not null)
                    return (new CoordinatorLink(new Uri(remembered.Origin), key: key), new CoordinatorConnection(remembered.Origin, "key", "remembered", "stored"));
            }
            catch (CoordinatorLinkException)
            {
                // A key the keyring holds that no coordinator makes: asked again, as one that is missing.
            }
            Console.Error.WriteLine("[WARN] The remembered coordinator needs its key again: connect it from the page.");
            return (null, new CoordinatorConnection(remembered.Origin, "key", "remembered", "none", NeedsKey: true));
        }
        return (new CoordinatorLink(new Uri(remembered.Origin)), new CoordinatorConnection(remembered.Origin, "browser", "remembered", "none"));
    }

    /// <summary>Why the studio could not listen at <paramref name="port"/>: the cause, with the option that changes it.</summary>
    private static string ListenFailure(int port, Exception failure) =>
        failure.GetBaseException() is SocketException { SocketErrorCode: var code } && port != 0 && code is SocketError.AddressAlreadyInUse or SocketError.AccessDenied
            ? code == SocketError.AddressAlreadyInUse
                ? $"Port {port} is already in use: give another with --port, or leave it out to take any free port."
                : $"This user may not listen on port {port}: give another with --port, or leave it out to take any free port."
            : $"dtpipe studio could not listen: {ShownFailure.Of(failure)}";

    /// <summary>
    /// The key <paramref name="reference"/> names, read once (<c>${{NAME}}</c>, <c>keyring://alias</c> or <c>@file</c>), or why not. A
    /// value is refused as <c>--client-secret</c> refuses one: an argument is listed by any process of the host, and a shell keeps it.
    /// </summary>
    public static async Task<(string? Key, string? Refusal)> ReadKeyAsync(string reference, ISecretsManager secrets, CancellationToken ct) =>
        NodeBricks.IsSecretReference(reference)
            ? await NodeBricks.ReadSecretAsync(reference, "key", secrets, ct)
            : (null, NodeBricks.NotAReference("--key", "key", "DTPIPE_KEY"));
}
