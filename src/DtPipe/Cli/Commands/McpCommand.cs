using System;
using System.CommandLine;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ModelContextProtocol;

namespace DtPipe.Cli.Commands;

public class McpCommand : Command
{
    public McpCommand(IServiceProvider serviceProvider) : base("mcp", "Start the MCP STDIO server for AI assistants")
    {
        this.SetAction((parseResult, ct) => RunAsync(serviceProvider, ct));
    }

    /// <summary>
    /// Serves until cancelled. The server has no end of its own, so a cancellation is the user
    /// stopping it and exits 130 (F16): it never reads as a success.
    /// </summary>
    internal static async Task<int> RunAsync(IServiceProvider serviceProvider, CancellationToken ct)
    {
        // Set security context for MCP
        var securityContext = serviceProvider.GetService<DtPipe.Core.Security.IMcpSecurityContext>();
        if (securityContext != null)
        {
            securityContext.IsMcpSession = true;
        }

        // Resolve all hosted services and start them in registration order.
        // This avoids fragile string-based type resolution.
        var hostedServices = serviceProvider.GetServices<IHostedService>().ToList();
        Console.Error.WriteLine("[MCP] Server starting on STDIO...");

        try
        {
            foreach (var service in hostedServices)
            {
                await service.StartAsync(ct);
            }

            // Wait indefinitely until cancellation
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 130;
        }
        finally
        {
            foreach (var service in hostedServices)
            {
                try { await service.StopAsync(default); }
                catch { /* Best effort cleanup */ }
            }
        }
    }
}
