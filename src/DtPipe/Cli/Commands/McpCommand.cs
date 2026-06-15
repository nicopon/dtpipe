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
        this.SetAction(async (parseResult, ct) =>
        {
            // Set security context for MCP
            DtPipe.Core.Security.McpSecurityContext.IsMcpSession = true;

            // MCP requires full control over stdout
            // dtpipe already sets Spectre.Console to Console.Error in Program.cs.
            // We just need to start the host or the MCP server on stdio.
            
            var hostedServices = serviceProvider.GetServices<IHostedService>();
            var mcpHostedService = hostedServices.FirstOrDefault(s => s.GetType().Name.Contains("McpServerHostedService"));
            if (mcpHostedService == null)
            {
                Console.Error.WriteLine("[MCP] Server is not registered properly.");
                return;
            }
            
            Console.Error.WriteLine("[MCP] Server starting on STDIO...");
            await mcpHostedService.StartAsync(ct);

            // Wait indefinitely until cancellation
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
    }
}
