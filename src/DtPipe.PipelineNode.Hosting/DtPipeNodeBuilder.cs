using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// A read or a write the node's owner offers: <see cref="Branch"/> is its piece of a dtpipe branch, read once when it was added.
/// Nothing writes to it: whoever runs the brick works on a copy, since a run sets its endpoints on the branch it gets.
/// </summary>
internal sealed record BrickDeclaration(string Id, BrickKind Kind, string Title, string? Description, YamlMappingNode Branch);

/// <summary>
/// A variable described to the coordinator by its name; its value stays on the node.
/// </summary>
public sealed record NodeVariable(string Name, string Value, string? Description);

/// <summary>
/// Why a data node must not run <paramref name="fragment"/>'s job as written, or <c>null</c> when it may. A host registers
/// one with <see cref="DtPipeNodeBuilder.WithFragmentGate"/> to run fragments that are no brick of its own;
/// the node still reads every branch against the structure rule of <c>BrickRules</c> only if the gate does.
/// </summary>
public delegate string? FragmentGate(FragmentSpec fragment, IReadOnlyList<BrickForm> bricks);

/// <summary>What the builder accumulates beside the bound options.</summary>
internal sealed class NodeDefinition
{
    public List<BrickDeclaration> Bricks { get; } = [];
    public List<NodeVariable> Variables { get; } = [];
    public FragmentPolicy? Policy { get; set; }
    public FragmentGate? Gate { get; set; }
}

/// <summary>Adds bricks and variables to the node <see cref="DtPipeNodeServiceCollectionExtensions.AddDtPipeNode(IServiceCollection, IConfiguration)"/> registered.</summary>
public sealed class DtPipeNodeBuilder
{
    private readonly NodeDefinition _definition;

    internal DtPipeNodeBuilder(IServiceCollection services, NodeDefinition definition)
    {
        Services = services;
        _definition = definition;
    }

    public IServiceCollection Services { get; }

    /// <summary>A variable every dtpipe child sees, described to the coordinator.</summary>
    public DtPipeNodeBuilder WithVariable(string name, string value, string? description = null)
    {
        if (_definition.Variables.Any(v => v.Name == name))
            throw new InvalidOperationException($"The variable '{name}' is already declared: a second value would silently replace the first.");
        _definition.Variables.Add(new NodeVariable(name, value, description));
        return this;
    }

    /// <summary>
    /// A brick whose <paramref name="branch"/> is its piece of a dtpipe branch, as YAML text: a JSON object is YAML too, and reads
    /// the same.
    /// </summary>
    public DtPipeNodeBuilder AddBrick(string id, BrickKind kind, string title, string branch, string? description = null)
    {
        // Read now, so a malformed brick fails the host's start, not the first deployment.
        YamlMappingNode read;
        try { read = JobYaml.ParseBranch(branch); }
        catch (Exception ex) when (ex is YamlException or FormatException)
        {
            throw new ArgumentException($"Brick '{id}': the branch is not a YAML mapping: {ex.Message}", nameof(branch), ex);
        }
        return AddBrick(id, kind, title, read, description);
    }

    internal DtPipeNodeBuilder AddBrick(string id, BrickKind kind, string title, YamlMappingNode branch, string? description)
    {
        _definition.Bricks.Add(new BrickDeclaration(id, kind, title, description, branch));
        return this;
    }

    /// <summary>
    /// Bricks from configuration: each child has <c>Id</c>, <c>Kind</c> (<c>Source</c> or <c>Sink</c>),
    /// <c>Title</c>, <c>Description</c> and a <c>Branch</c> section, read as a branch. Configuration
    /// holds every value as text, which a brick's match ignores: it compares scalars by their text.
    /// </summary>
    public DtPipeNodeBuilder AddBricks(IConfiguration bricks)
    {
        foreach (var brick in bricks.GetChildren())
        {
            var id = brick["Id"] ?? throw new InvalidOperationException($"Brick {brick.Path}: Id is required.");
            if (!Enum.TryParse<BrickKind>(brick["Kind"], ignoreCase: true, out var kind))
                throw new InvalidOperationException($"Brick '{id}': Kind must be Source or Sink.");
            var branch = ConfigurationYaml.Of(brick.GetSection("Branch")) as YamlMappingNode is { Children.Count: > 0 } mapping
                ? mapping
                : throw new InvalidOperationException($"Brick '{id}': Branch is required.");
            AddBrick(id, kind, brick["Title"] ?? id, branch, brick["Description"]);
        }
        return this;
    }

    /// <summary>
    /// Takes the node's secret at the identity provider from <paramref name="provider"/> (an environment variable, a secret manager,
    /// a vault), asked each time the node requests a token: a key the host rotates is presented by the next request, a retry after
    /// a 401 included. The text it returns is used as it is. It replaces <c>Oidc:ClientSecret</c> and <c>Oidc:ClientSecretFile</c>,
    /// which are then left unset.
    /// </summary>
    public DtPipeNodeBuilder UseClientSecret(Func<CancellationToken, ValueTask<string>> provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        Services.PostConfigure<DtPipeNodeOptions>(o => o.Oidc.ClientSecretProvider = provider);
        return this;
    }

    /// <summary>
    /// Replaces the check a data node makes of a fragment before it runs it. The host's own word, like
    /// <see cref="FragmentPolicy"/>: nothing the coordinator sends reaches it. The node announces the
    /// <see cref="NodeAnnouncement.RawFragments"/> capability.
    /// </summary>
    public DtPipeNodeBuilder WithFragmentGate(FragmentGate gate)
    {
        _definition.Gate = gate;
        return this;
    }

    /// <summary>The node is a runner: it hosts no data and runs any fragment it is sent.</summary>
    public DtPipeNodeBuilder RunAnyFragment()
    {
        _definition.Policy = FragmentPolicy.AnyFragment;
        return this;
    }
}

public static class DtPipeNodeServiceCollectionExtensions
{
    /// <summary>Adds a node host, bound to <paramref name="configuration"/> (the <c>DtPipeNode</c> section).</summary>
    public static DtPipeNodeBuilder AddDtPipeNode(this IServiceCollection services, IConfiguration configuration) =>
        services.AddDtPipeNode(o => configuration.Bind(o));

    /// <summary>Adds a node host configured in code.</summary>
    public static DtPipeNodeBuilder AddDtPipeNode(this IServiceCollection services, Action<DtPipeNodeOptions> configure)
    {
        if (services.Any(d => d.ServiceType == typeof(NodeDefinition)))
            throw new InvalidOperationException("A process hosts one dtpipe node: AddDtPipeNode was already called.");

        var definition = new NodeDefinition();
        services.AddSingleton(definition);
        services.AddOptions<DtPipeNodeOptions>()
            .Configure(configure)
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<DtPipeNodeOptions>, DescribedValidation>();
        services.AddHostedService<NodeAgent>();
        return new DtPipeNodeBuilder(services, definition);
    }

    /// <summary>Names every missing setting at start, rather than a generic validation failure.</summary>
    private sealed class DescribedValidation : IValidateOptions<DtPipeNodeOptions>
    {
        public ValidateOptionsResult Validate(string? name, DtPipeNodeOptions options)
        {
            var errors = options.Validate().ToList();
            return errors.Count > 0 ? ValidateOptionsResult.Fail(errors.Select(e => "DtPipeNode: " + e)) : ValidateOptionsResult.Success;
        }
    }
}

/// <summary>
/// A configuration section as a branch's tree: children with indices become a sequence, values stay text. A section that holds
/// nothing is an empty mapping, which is what configuration keeps of an empty object or array.
/// </summary>
internal static class ConfigurationYaml
{
    public static YamlNode Of(IConfigurationSection section)
    {
        var children = section.GetChildren().ToList();
        if (children.Count == 0) return section.Value is { } value ? JobYaml.Text(value) : new YamlMappingNode();
        if (children.All(c => int.TryParse(c.Key, out _)))
            return new YamlSequenceNode(children.OrderBy(c => int.Parse(c.Key)).Select(Of));
        return new YamlMappingNode(children.Select(c => new KeyValuePair<YamlNode, YamlNode>(new YamlScalarNode(c.Key), Of(c))));
    }
}
