using System.CommandLine;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Commands;

/// <summary>
/// <c>dtpipe studio [folder]</c>: a local page over the YAML jobs of a folder, sampled through the
/// same path as the MCP <c>dry-run</c> tool. It ends when the page asks to (exit 0) or on Ctrl+C
/// (exit 130: a cancellation never reads as a success).
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
        Arguments.Add(folderArgument);
        Options.Add(portOption);

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
            // A job's relative paths (a query file, a local input) resolve from where it is run:
            // from the folder, as `dtpipe --job` run there would.
            Directory.SetCurrentDirectory(folder.Root);

            var mcp = serviceProvider.GetRequiredService<DtPipeMcpTools>();
            var readers = serviceProvider.GetServices<IStreamReaderFactory>().ToList();
            var writers = serviceProvider.GetServices<IDataWriterFactory>().ToList();
            var tools = new StudioTools(
                (yaml, rows, token) => mcp.SampleAsync(yaml, rows, shownRows: rows, token),
                new DesignService(IBrickLookup.None, new ComponentEndpointRules(readers, writers)),
                readers.Select(r => new StudioComponent(r.ComponentName, r.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList(),
                writers.Select(w => new StudioComponent(w.ComponentName, w.Category)).OrderBy(c => c.Name, StringComparer.Ordinal).ToList());
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
}
