using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Configuration;
using DtPipe.Cli;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Security;
using ModelContextProtocol.Server;
namespace DtPipe.Cli.Mcp;

public partial class DtPipeMcpTools
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IMcpHelpService _mcpHelpService;
    private readonly IServiceProvider _serviceProvider;

       /// <summary>
        /// Optional agent-scoped options (F1/F2). When set by the agent command, governs the
        /// guardrails: <see cref="DtPipe.Cli.Agent.AgentOptions.Apply"/> enables a real write (subject
        /// to the approval gate), and the allow-* flags configure the SQL safety policy.
        /// </summary>
     public DtPipe.Cli.Agent.AgentOptions? AgentOptions { get; set; }

    /// <summary>
    /// Overrides the working directory session resolution starts from (checkpoint tools). Null in
    /// production — <see cref="Sessions.SessionResolver"/> then falls back to the process's own
    /// <see cref="Directory.GetCurrentDirectory"/>, which is the real MCP-server behaviour. Exists
    /// so a test can hand this an isolated temp directory instead of calling
    /// <see cref="Directory.SetCurrentDirectory"/>, which is process-global and races any test
    /// running concurrently in another collection.
    /// </summary>
    internal string? WorkingDirectoryOverride { get; set; }


    public DtPipeMcpTools(
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories,
        IMcpHelpService mcpHelpService,
        IServiceProvider serviceProvider)
    {
        _readerFactories = readerFactories;
        _transformerFactories = transformerFactories;
        _writerFactories = writerFactories;
        _mcpHelpService = mcpHelpService;
        _serviceProvider = serviceProvider;
    }
}
