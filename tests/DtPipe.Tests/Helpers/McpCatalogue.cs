using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// The real MCP tool surface, as the agent offers it. Nothing is invoked here, so the factories and
/// the service provider can be empty: the shape of the catalogue comes from reflection over the
/// tool methods, not from what they would do.
/// </summary>
internal static class McpCatalogue
{
    internal static DtPipeMcpTools Tools()
    {
        var readers = Array.Empty<IStreamReaderFactory>();
        var transformers = Array.Empty<IDataTransformerFactory>();
        var writers = Array.Empty<IDataWriterFactory>();

        return new DtPipeMcpTools(
            readers, transformers, writers,
            new McpHelpService(readers, transformers, writers),
            new ServiceCollection().BuildServiceProvider());
    }

    /// <summary>Every tool name the agent can offer, the <c>ask-user</c> terminator included.</summary>
    internal static IReadOnlyList<string> Names() =>
        new McpToolProvider(Tools()).GetToolDefinitions().Select(t => t.Name).ToList();
}
