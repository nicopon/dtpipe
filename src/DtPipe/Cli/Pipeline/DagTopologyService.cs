using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// The one place a job file turns into a DAG: parse the YAML, build the <see cref="BranchDefinition"/>
/// list, wrap it in a <see cref="JobDagDefinition"/>. The MCP validate/execute path, the agent's
/// Spectre topology box, the <c>get-dag-topology</c> tool and the full-screen plan panel all read it
/// from here — the <c>yaml → DAG</c> construction is not re-typed at each call site (the duplication
/// CLAUDE.md's init-SQL note warns about).
/// </summary>
public sealed class DagTopologyService
{
    private readonly IEnumerable<IStreamTransformerFactory> _streamTransformerFactories;
    private readonly DtPipe.Cli.Security.ISecretsManager? _secretsManager;

    public DagTopologyService(
        IEnumerable<IStreamTransformerFactory> streamTransformerFactories,
        DtPipe.Cli.Security.ISecretsManager? secretsManager = null)
    {
        _streamTransformerFactories = streamTransformerFactories;
        _secretsManager = secretsManager;
    }

    /// <summary>Builds the service from a DI container — the shape every call site already has.</summary>
    public static DagTopologyService FromServices(IServiceProvider services) => new(
        services.GetRequiredService<IEnumerable<IStreamTransformerFactory>>(),
        services.GetService<DtPipe.Cli.Security.ISecretsManager>());

    /// <summary>
    /// Parses <paramref name="yamlContent"/> and builds both the hydrated jobs and the DAG.
    /// Propagates whatever the YAML parser throws on malformed input.
    /// </summary>
    public DagBuild Build(string yamlContent)
    {
        var jobs = JobFileParser.ParseContent(yamlContent, _secretsManager);

        var branches = jobs.Select(kv => new BranchDefinition
        {
            Alias = kv.Key,
            Input = kv.Value.Input,
            Output = kv.Value.Output,
            StreamingAliases = kv.Value.From != null
                ? kv.Value.From.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : Array.Empty<string>(),
            RefAliases = kv.Value.Ref ?? Array.Empty<string>(),
            Arguments = Array.Empty<string>(),
            ProcessorName = _streamTransformerFactories
                .FirstOrDefault(f => f.IsApplicable(kv.Value))
                ?.ComponentName,
            PreParsedJob = kv.Value,
        }).ToList();

        return new DagBuild(jobs, new JobDagDefinition { Branches = branches });
    }

    /// <summary>
    /// The serialisation-friendly view — alias, input, output, processor, from[], ref[] per branch.
    /// This is data: the <c>get-dag-topology</c> tool and the plan panel read it; neither formats
    /// text from the model.
    /// </summary>
    public DagTopology Describe(string yamlContent) => DagTopology.From(Build(yamlContent).Dag);

    /// <summary>
    /// <see cref="Describe"/> that folds a parse failure into <c>null</c> — the plan panel polls a
    /// plan still under construction, which is not always valid YAML yet.
    /// </summary>
    public DagTopology? TryDescribe(string? yamlContent)
    {
        if (string.IsNullOrWhiteSpace(yamlContent)) return null;
        try { return Describe(yamlContent); }
        catch { return null; }
    }
}

/// <summary>The hydrated jobs and the DAG built from one job file.</summary>
public sealed record DagBuild(Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag);

/// <summary>A pipeline's branch topology as plain data.</summary>
public sealed record DagTopology(IReadOnlyList<BranchTopology> Branches)
{
    public static DagTopology From(JobDagDefinition dag) => new(
        dag.Branches.Select(b => new BranchTopology(
            b.Alias,
            b.Input,
            b.Output,
            b.ProcessorName,
            b.StreamingAliases.ToArray(),
            b.RefAliases.ToArray())).ToArray());
}

/// <summary>One branch: its alias, endpoints, stream processor, and the aliases it reads or materialises.</summary>
public sealed record BranchTopology(
    string Alias,
    string? Input,
    string? Output,
    string? Processor,
    IReadOnlyList<string> From,
    IReadOnlyList<string> Ref);
