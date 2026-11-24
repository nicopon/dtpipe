using System.CommandLine;
using System.Net.Sockets;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Node;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Split;
using DtPipe.Cli.Security;
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
    public StudioCommand(IServiceProvider serviceProvider)
        : base("studio", "Open a local page to sample the YAML jobs of a folder through the real engine")
    {
        var folderArgument = new Argument<string?>("folder")
        {
            Description = "The folder of jobs (default: the current directory)",
            Arity = ArgumentArity.ZeroOrOne,
        };
        var portOption = new Option<int>("--port")
        {
            Description = "The port on 127.0.0.1 (default: any free port)",
        };
        var coordinatorOption = new Option<string?>("--coordinator")
        {
            Description = "A coordinator to sign in to, by its address: its bricks and its library (default: none)",
        };
        var keyOption = new Option<string?>("--key")
        {
            Description = "Sign in to the coordinator with a key it made for you, as a reference: ${{NAME}}, keyring://alias or @file (default: through the browser)",
        };
        var llmProviderOption = new Option<string?>("--llm-provider")
        {
            Description = "Open the agent's panel on this model provider: ollama or openai (default: no agent)",
        };
        var llmUrlOption = new Option<string?>("--llm-url")
        {
            Description = "The model provider's address (default: http://localhost:11434 for ollama, https://api.openai.com for openai)",
        };
        var llmModelOption = new Option<string?>("--llm-model")
        {
            Description = "The model the agent starts with; the page may choose another the provider lists (default: none)",
        };
        var llmKeyOption = new Option<string?>("--llm-key")
        {
            Description = "The provider's API key, as a reference: ${{NAME}}, keyring://alias or @file (default: none)",
        };
        var llmTimeoutOption = new Option<int?>("--llm-timeout")
        {
            Description = "Seconds with no token before a model call fails (default: 300)",
        };
        var agentTraceOption = new Option<string?>("--agent-trace")
        {
            Description = "Record the agent's session as JSON lines; a directory gets a timestamped file (default: $DTPIPE_AGENT_TRACE, else none)",
        };
        Arguments.Add(folderArgument);
        Options.Add(portOption);
        Options.Add(coordinatorOption);
        Options.Add(keyOption);
        Options.Add(llmProviderOption);
        Options.Add(llmUrlOption);
        Options.Add(llmModelOption);
        Options.Add(llmKeyOption);
        Options.Add(llmTimeoutOption);
        Options.Add(agentTraceOption);

        this.SetAction(async (parseResult, ct) =>
        {
            var path = parseResult.GetValue(folderArgument) ?? Directory.GetCurrentDirectory();
            StudioFolder folder;
            try { folder = new StudioFolder(path); }
            catch (DirectoryNotFoundException ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 1;
            }
            var port = parseResult.GetValue(portOption);
            if (port is < 0 or > 65535)
            {
                Console.Error.WriteLine($"[ERROR] --port is a port number from 0 to 65535, not {port}.");
                return 1;
            }
            CoordinatorLink? link = null;
            var coordinator = parseResult.GetValue(coordinatorOption);
            string? key = null;
            if (parseResult.GetValue(keyOption) is { } reference)
            {
                if (coordinator is not { Length: > 0 })
                {
                    Console.Error.WriteLine("[ERROR] --key signs in to a coordinator: name it with --coordinator.");
                    return 1;
                }
                string? refusal;
                (key, refusal) = await ReadKeyAsync(reference, serviceProvider.GetRequiredService<ISecretsManager>(), ct);
                if (refusal is not null)
                {
                    Console.Error.WriteLine($"[ERROR] {refusal}");
                    return 1;
                }
            }
            if (coordinator is { Length: > 0 })
            {
                try { link = new CoordinatorLink(Uri.TryCreate(coordinator, UriKind.Absolute, out var uri) ? uri : throw new CoordinatorLinkException($"'{coordinator}' is no address."), key: key); }
                catch (CoordinatorLinkException ex)
                {
                    Console.Error.WriteLine(ex.Message);
                    return 1;
                }
            }
            var secrets = serviceProvider.GetRequiredService<ISecretsManager>();
            var settings = new StudioSettings(StudioSettings.DefaultPath(), secrets);
            var remembered = settings.Read();
            if (settings.Warning is { } ignored) Console.Error.WriteLine($"[WARN] {ignored}");
            CoordinatorConnection? coordinatorStart = null;
            // Nothing named on the command line: the coordinator the person connected in the page and asked to remember.
            if (link is null && remembered.Coordinator is { } rememberedCoordinator)
                (link, coordinatorStart) = RememberedCoordinator(settings, rememberedCoordinator);
            using var _ = link;
            var agentTools = new McpToolProvider(serviceProvider.GetRequiredService<DtPipeMcpTools>());
            var trace = AgentTrace.Open(parseResult.GetValue(agentTraceOption) ?? Environment.GetEnvironmentVariable("DTPIPE_AGENT_TRACE"), out var traceFailure);
            if (traceFailure is not null) Console.Error.WriteLine($"[WARN] The agent's trace cannot be written: {traceFailure}");
            using var __ = trace;
            StudioAgentSetup? agent = null;
            AgentConnection? start = null;
            var provider = parseResult.GetValue(llmProviderOption);
            if (provider is null)
            {
                if (new Option[] { llmUrlOption, llmModelOption, llmKeyOption, llmTimeoutOption }.FirstOrDefault(o => parseResult.GetResult(o) is not null) is { } orphan)
                {
                    Console.Error.WriteLine($"[ERROR] {orphan.Name} sets up the agent: name its provider with --llm-provider.");
                    return 1;
                }
                // Nothing named on the command line: the connection the person made in the page and asked to remember.
                (agent, start) = Remembered(remembered.Agent, settings, agentTools, trace);
            }
            else
            {
                string? llmKey = null;
                if (parseResult.GetValue(llmKeyOption) is { } keyReference)
                {
                    string? refusal;
                    (llmKey, refusal) = NodeBricks.IsSecretReference(keyReference)
                        ? await NodeBricks.ReadSecretAsync(keyReference, "key", secrets, ct)
                        : (null, NodeBricks.NotAReference("--llm-key", "key", "LLM_API_KEY"));
                    if (refusal is not null)
                    {
                        Console.Error.WriteLine($"[ERROR] {refusal}");
                        return 1;
                    }
                }
                var timeout = TimeSpan.FromSeconds(parseResult.GetValue(llmTimeoutOption) is { } seconds and > 0 ? seconds : (int)AgentOptions.DefaultLlmTimeout.TotalSeconds);
                var client = StudioAgentClients.Create(provider, llmKey, timeout);
                if (client is null)
                {
                    Console.Error.WriteLine($"[ERROR] --llm-provider is ollama or openai, not '{provider}'.");
                    return 1;
                }
                var url = parseResult.GetValue(llmUrlOption) ?? StudioAgentClients.DefaultUrl(client.ProviderName);
                var model = parseResult.GetValue(llmModelOption);
                agent = new StudioAgentSetup(client, url, model, agentTools, trace);
                start = new AgentConnection(client.ProviderName, url, model, (int)timeout.TotalSeconds, "flag", llmKey is null ? "none" : "flag");
            }
            // With a key the studio signs in at once; a refusal leaves it signed out with the reason, which the page shows too.
            if (link is { ByKey: true })
            {
                try { Console.Error.WriteLine($"Signed in to {link.Origin.GetLeftPart(UriPartial.Authority)} as {await link.SignInWithKeyAsync(ct)}."); }
                catch (CoordinatorLinkException ex) { Console.Error.WriteLine(ex.Message); }
            }
            // A job's relative paths (a query file, a local input) resolve from where it is run:
            // from the folder, as `dtpipe --job` run there would.
            Directory.SetCurrentDirectory(folder.Root);

            var readers = serviceProvider.GetServices<IStreamReaderFactory>().ToList();
            var writers = serviceProvider.GetServices<IDataWriterFactory>().ToList();
            var endpoints = new ComponentEndpointRules(readers, writers);
            var engine = new StudioEngine(
                serviceProvider.GetRequiredService<JobAnalysis>(), serviceProvider.GetRequiredService<JobRunner>(),
                new DebugCutter(readers, writers, serviceProvider.GetServices<IStreamTransformerFactory>().ToList()));
            var tools = new StudioTools(
                engine,
                new DesignService(IBrickLookup.None, endpoints),
                readers.Select(r => new StudioComponent(r.ComponentName, r.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList(),
                writers.Select(w => new StudioComponent(w.ComponentName, w.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList(),
                secrets,
                link,
                lookup => new DesignService(lookup, endpoints),
                args => CommandLineJob.Export(serviceProvider, args),
                agent,
                new StudioAgentLink(settings, agentTools, trace, StudioAgentClients.Create, start),
                new StudioCoordinatorStart(settings, coordinatorStart));
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
        });
    }

    /// <summary>
    /// The agent the person asked the studio to remember, built from the file and the key kept for its address. A provider that
    /// needs a key and finds none builds no agent: it waits, described as needing its key, for the person to connect it again.
    /// </summary>
    private static (StudioAgentSetup? Agent, AgentConnection? Start) Remembered(AgentSettings? agent, StudioSettings settings, IAgentToolProvider tools, AgentTrace? trace)
    {
        if (agent is not { } remembered) return (null, null);
        var timeout = TimeSpan.FromSeconds(remembered.Timeout is > 0 ? remembered.Timeout.Value : (int)AgentOptions.DefaultLlmTimeout.TotalSeconds);
        var key = remembered.Provider == "openai" ? settings.ReadLlmKey(remembered.Url) : null;
        var seconds = (int)timeout.TotalSeconds;
        if (remembered.Provider == "openai" && key is null)
        {
            Console.Error.WriteLine("[WARN] The remembered model needs its key again: connect it from the page.");
            return (null, new AgentConnection(remembered.Provider, remembered.Url, remembered.Model, seconds, "remembered", "none", NeedsKey: true));
        }
        var client = StudioAgentClients.Create(remembered.Provider, key, timeout)!;
        return (new StudioAgentSetup(client, remembered.Url, remembered.Model, tools, trace),
            new AgentConnection(remembered.Provider, remembered.Url, remembered.Model, seconds, "remembered", key is null ? "none" : "stored"));
    }

    /// <summary>
    /// The coordinator the person asked the studio to remember, as a link and its description. A key sign-in that needs a key and
    /// finds none builds no link: it waits, described as needing its key, for the person to connect it again. It never stops the
    /// start, and its sign-in runs in the background (<c>StudioServer</c>).
    /// </summary>
    private static (CoordinatorLink? Link, CoordinatorConnection Start) RememberedCoordinator(StudioSettings settings, CoordinatorSettings remembered)
    {
        var key = remembered.Auth == "key" ? settings.ReadCoordinatorKey(remembered.Origin) : null;
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
