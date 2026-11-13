using System.CommandLine;
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
/// through the same path as the MCP <c>dry-run</c> tool, and signed in to a coordinator when one is named, through the browser or with
/// a key the coordinator made for the person. The key is a reference, never a value, read once at start. It ends when the page asks
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
            using var _ = link;
            StudioAgentSetup? agent = null;
            var provider = parseResult.GetValue(llmProviderOption);
            if (provider is null)
            {
                if (new Option[] { llmUrlOption, llmModelOption, llmKeyOption, llmTimeoutOption, agentTraceOption }.FirstOrDefault(o => parseResult.GetResult(o) is not null) is { } orphan)
                {
                    Console.Error.WriteLine($"[ERROR] {orphan.Name} sets up the agent: name its provider with --llm-provider.");
                    return 1;
                }
            }
            else
            {
                string? llmKey = null;
                if (parseResult.GetValue(llmKeyOption) is { } keyReference)
                {
                    string? refusal;
                    (llmKey, refusal) = NodeBricks.IsSecretReference(keyReference)
                        ? await NodeBricks.ReadSecretAsync(keyReference, "key", serviceProvider.GetRequiredService<ISecretsManager>(), ct)
                        : (null, NodeBricks.NotAReference("--llm-key", "key", "DTPIPE_LLM_KEY"));
                    if (refusal is not null)
                    {
                        Console.Error.WriteLine($"[ERROR] {refusal}");
                        return 1;
                    }
                }
                var timeout = TimeSpan.FromSeconds(parseResult.GetValue(llmTimeoutOption) is { } seconds and > 0 ? seconds : (int)AgentOptions.DefaultLlmTimeout.TotalSeconds);
                ILlmClient? client = provider.ToLowerInvariant() switch
                {
                    "ollama" => new OllamaClient(timeout),
                    "openai" => new OpenAiClient(llmKey, timeout),
                    _ => null,
                };
                if (client is null)
                {
                    Console.Error.WriteLine($"[ERROR] --llm-provider is ollama or openai, not '{provider}'.");
                    return 1;
                }
                var url = parseResult.GetValue(llmUrlOption) ?? (client is OpenAiClient ? "https://api.openai.com" : "http://localhost:11434");
                var trace = AgentTrace.Open(parseResult.GetValue(agentTraceOption) ?? Environment.GetEnvironmentVariable("DTPIPE_AGENT_TRACE"), out var traceFailure);
                if (traceFailure is not null) Console.Error.WriteLine($"[WARN] The agent's trace cannot be written: {traceFailure}");
                agent = new StudioAgentSetup(client, url, parseResult.GetValue(llmModelOption),
                    new McpToolProvider(serviceProvider.GetRequiredService<DtPipeMcpTools>()), trace);
            }
            using var __ = agent?.Trace;
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
                serviceProvider.GetRequiredService<ISecretsManager>(),
                link,
                lookup => new DesignService(lookup, endpoints),
                args => CommandLineJob.Export(serviceProvider, args),
                agent);
            StudioServer server;
            try
            {
                server = await StudioServer.StartAsync(folder, parseResult.GetValue(portOption), tools, ct);
            }
            catch (IOException ex)
            {
                Console.Error.WriteLine($"dtpipe studio could not listen: {ex.Message}");
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
    /// The key <paramref name="reference"/> names, read once (<c>${{NAME}}</c>, <c>keyring://alias</c> or <c>@file</c>), or why not. A
    /// value is refused as <c>--client-secret</c> refuses one: an argument is listed by any process of the host, and a shell keeps it.
    /// </summary>
    public static async Task<(string? Key, string? Refusal)> ReadKeyAsync(string reference, ISecretsManager secrets, CancellationToken ct) =>
        NodeBricks.IsSecretReference(reference)
            ? await NodeBricks.ReadSecretAsync(reference, "key", secrets, ct)
            : (null, NodeBricks.NotAReference("--key", "key", "DTPIPE_KEY"));
}
