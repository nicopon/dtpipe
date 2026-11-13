using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Processors.Merge;

/// <summary>
/// Factory for <see cref="MergeTransformer"/>.
/// Activated when branch arguments contain the boolean flag <c>--merge</c> (no value).
/// Streaming sources are declared via <c>--from a,b,c</c> (comma-separated).
/// </summary>
public class MergeTransformerFactory : IStreamTransformerFactory, IStreamProcessorMarker
{
    public string ComponentName => "merge";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;

    public int MinStreams => 2;
    public int MaxStreams => -1;
    public int MinLookups => 0;
    public int MaxLookups => 0;

    public string Description => "Concatenate the rows of several branches into one stream.";

    public string UsageNotes =>
        """
        'from' lists the branches to concatenate, comma-separated.
        Their schemas must agree on column count, names and structural types: the merge refuses
        channels that differ and never converts.
        Example:
          main:
            from: "north,south"
            provider-options:
              merge: {}
            output: "<adapter>:<target>"
        """;

    public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => [("--merge", true)];

    public bool IsApplicable(string[] branchArgs)
        => branchArgs.Any(a => a.Equals("--merge", StringComparison.OrdinalIgnoreCase));

    public Dictionary<string, object?>? ExportToProviderOptions(string[] branchArgs)
        => IsApplicable(branchArgs) ? new Dictionary<string, object?>() : null;

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var fromValue = BranchArgParser.ExtractValue(branchArgs, "--from")
            ?? throw new ArgumentException("--from <aliases> is required for MergeTransformer");

        return CreateFromOptions(Split(fromValue), ctx, serviceProvider);
    }

    public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var fromValue = job.From
            ?? throw new ArgumentException("'from' with at least 2 comma-separated sources is required for the merge stream processor.");

        return CreateFromOptions(Split(fromValue), ctx, serviceProvider);
    }

    private static string[] Split(string csv)
        => csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    // Shared convergence point for the CLI (Create) and YAML (CreateFromJob) surfaces.
    private static IStreamTransformer CreateFromOptions(string[] fromAliases, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        // Resolve logical→physical aliases via AliasMap (fan-out sub-channels).
        var aliases = fromAliases
            .Select(a => ctx.AliasMap.GetValueOrDefault(a, a))
            .ToList();

        if (aliases.Count < 2)
            throw new ArgumentException($"MergeTransformer requires at least 2 streaming sources via 'from a,b,...', got {aliases.Count}.");

        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        return new MergeTransformer(registry, aliases);
    }
}
