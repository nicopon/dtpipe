using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// The real MCP tool surface. Nothing is invoked here, so the factories and
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
            new McpHelpService(readers, transformers, writers, Array.Empty<IStreamTransformerFactory>()),
            new ServiceCollection().BuildServiceProvider());
    }

    /// <summary>
    /// Every tool name the server carries, the writing ones included, and <c>ask-user</c>, which the
    /// agent adds. The names the model is offered are fewer: <see cref="McpToolProvider"/> leaves out
    /// those that write to a target.
    /// </summary>
    internal static IReadOnlyList<string> Names() =>
        McpToolReflector.BuildToolDefinitions(typeof(DtPipeMcpTools))
            .Select(t => t.Name)
            .Append(McpToolProvider.AskUserTool.Name)
            .ToList();
}
