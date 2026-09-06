using System.Collections.Generic;
using DtPipe.Core.Abstractions;

namespace DtPipe.Cli.Mcp;

/// <summary>One component of the catalogue as a caller discovering it sees it: its name, the
/// roles it can take, and its own <see cref="System.ComponentModel.DescriptionAttribute"/>.</summary>
public readonly record struct ComponentSummary(string Name, string Roles, string Description);

public interface IMcpHelpService
{
    string GetGeneralHelp();

    /// <summary>Every adapter with the roles it supports and what it does.</summary>
    IReadOnlyList<ComponentSummary> Adapters();

    /// <summary>Every transformer with what it does.</summary>
    IReadOnlyList<ComponentSummary> Transformers();

    string GetAdapterHelp(string adapterName);
    string GetTransformerHelp(string transformerName);
    string GetAnonymizationHelp();
}
