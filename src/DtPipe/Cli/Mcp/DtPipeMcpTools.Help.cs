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

    [McpServerTool(Name = "list-providers")]
    [System.ComponentModel.Description("List available data source providers, writers, and transformers in dtpipe, each with what it does")]
    public string ListProviders()
    {
        // Names alone force a caller to open every component's help to find the one it needs.
        // The description is the component's own attribute, so this listing cannot drift from
        // what 'get-adapter-help' and 'get-transformer-help' print.
        var adapters = _mcpHelpService.Adapters();

        return JsonSerializer.Serialize(new
        {
            readers = adapters.Where(a => a.Roles.Contains("reader")).Select(a => new { name = a.Name, description = a.Description }).ToList(),
            transformers = _mcpHelpService.Transformers().Select(t => new { name = t.Name, description = t.Description }).ToList(),
            writers = adapters.Where(a => a.Roles.Contains("writer")).Select(a => new { name = a.Name, description = a.Description }).ToList()
        }, new JsonSerializerOptions { WriteIndented = true });
    }


    [McpServerTool(Name = "help")]
    [System.ComponentModel.Description("Show general usage guidelines, YAML job structures, connection string rules, DAG capabilities, and list available adapters and transformers.")]
    public string Help()
    {
        return _mcpHelpService.GetGeneralHelp();
    }


    [McpServerTool(Name = "get-adapter-help")]
    [System.ComponentModel.Description("Show detailed help on a specific data adapter, including its usage as a reader or writer, and its specific options/flags in YAML.")]
    public string GetAdapterHelp(
        [System.ComponentModel.Description("Name of the adapter (e.g. 'csv', 'sqlite'). Call the 'list-providers' tool to discover all available reader/writer adapter names.")] string adapterName)
    {
        return _mcpHelpService.GetAdapterHelp(adapterName);
    }


    [McpServerTool(Name = "get-transformer-help")]
    [System.ComponentModel.Description("Show detailed help on a specific transformer, including its YAML options and examples.")]
    public string GetTransformerHelp(
        [System.ComponentModel.Description("Name of the transformer (e.g. 'compute', 'fake'). Call the 'list-providers' tool to discover all available transformer names.")] string transformerName)
    {
        return _mcpHelpService.GetTransformerHelp(transformerName);
    }


    [McpServerTool(Name = "get-anonymization-help")]
    [System.ComponentModel.Description("Show detailed help on data faking (anonymization) via Bogus, including available datasets, methods, and options for the YAML job schema.")]
    public string GetAnonymizationHelp()
    {
        return _mcpHelpService.GetAnonymizationHelp();
    }

}
