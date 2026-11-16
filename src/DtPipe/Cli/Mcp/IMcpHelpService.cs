using System.Collections.Generic;
using DtPipe.Core.Abstractions;

namespace DtPipe.Cli.Mcp;

/// <summary>One component of the catalogue as a caller discovering it sees it: its name, the
/// roles it can take, and its own <see cref="System.ComponentModel.DescriptionAttribute"/>.</summary>
public readonly record struct ComponentSummary(string Name, string Roles, string Description);

/// <summary>One stream processor as a caller discovering it sees it: how a job selects it, what it
/// asks of the branches around it, and the notes its factory owns. All of it comes off the factory.</summary>
public readonly record struct ProcessorSummary(
    string Name,
    string Description,
    string YamlKey,
    IReadOnlyList<string> CliFlags,
    string Streams,
    string Lookups,
    string UsageNotes);

public interface IMcpHelpService
{
    string GetGeneralHelp();

    /// <summary>Every adapter with the roles it supports and what it does.</summary>
    IReadOnlyList<ComponentSummary> Adapters();

    /// <summary>Every transformer with what it does.</summary>
    IReadOnlyList<ComponentSummary> Transformers();

    /// <summary>Every stream processor (what a branch with a <c>ref</c> runs), apart from the transformers.</summary>
    IReadOnlyList<ProcessorSummary> Processors();

    string GetAdapterHelp(string adapterName);
    string GetTransformerHelp(string transformerName);
    string GetAnonymizationHelp();
}
