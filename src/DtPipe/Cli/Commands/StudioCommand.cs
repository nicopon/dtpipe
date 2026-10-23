using System.CommandLine;
using DtPipe.Cli.Expressions;
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
        Arguments.Add(folderArgument);
        Options.Add(portOption);
        Options.Add(coordinatorOption);
        Options.Add(keyOption);

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
                args => CommandLineJob.Export(serviceProvider, args));
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
    public static async Task<(string? Key, string? Refusal)> ReadKeyAsync(string reference, ISecretsManager secrets, CancellationToken ct)
    {
        if (!NodeBricks.IsSecretReference(reference))
            return (null, "--key takes a reference, never the key: ${{NAME}}, keyring://alias or @file.");
        string? key;
        try { key = await CompositeStringContentResolver.Standard(secrets).ResolveAsync(reference, ct); }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, $"The key could not be read: {ex.Message}");
        }
        key = key?.Trim();
        return string.IsNullOrEmpty(key) ? (null, "The key resolves to nothing.") : (key, null);
    }
}
