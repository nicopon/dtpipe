using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>A read or a write the node's owner offers: <see cref="BranchJson"/> is its piece of a dtpipe branch, as JSON.</summary>
public sealed record BrickDeclaration(string Id, BrickKind Kind, string Title, string? Description, string BranchJson);

/// <summary>
/// A variable described to the coordinator by its name; its value stays on the node.
/// <see cref="Engine"/> makes it a database the node queries for the coordinator's lab mode
/// (<c>engine:${{Name}}</c>).
/// </summary>
public sealed record NodeVariable(string Name, string Value, string? Description, string? Engine);

/// <summary>What the builder accumulates beside the bound options.</summary>
internal sealed class NodeDefinition
{
    public List<BrickDeclaration> Bricks { get; } = [];
    public List<NodeVariable> Variables { get; } = [];
    public FragmentPolicy? Policy { get; set; }
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
    public DtPipeNodeBuilder WithVariable(string name, string value, string? description = null, string? engine = null)
    {
        _definition.Variables.Add(new NodeVariable(name, value, description, engine));
        return this;
    }

    public DtPipeNodeBuilder AddBrick(string id, BrickKind kind, string title, string branchJson, string? description = null)
    {
        // Read now, so a malformed brick fails the host's start, not the first deployment.
        if (JsonNode.Parse(branchJson) is not JsonObject)
            throw new ArgumentException($"Brick '{id}': the branch must be a JSON object.", nameof(branchJson));
        _definition.Bricks.Add(new BrickDeclaration(id, kind, title, description, branchJson));
        return this;
    }

    /// <summary>
    /// Bricks from configuration: each child has <c>Id</c>, <c>Kind</c> (<c>Source</c> or <c>Sink</c>),
    /// <c>Title</c>, <c>Description</c> and a <c>Branch</c> section, read back as JSON. Configuration
    /// holds every value as text, which a brick's match ignores: it compares scalars by their text.
    /// </summary>
    public DtPipeNodeBuilder AddBricks(IConfiguration bricks)
    {
        foreach (var brick in bricks.GetChildren())
        {
            var id = brick["Id"] ?? throw new InvalidOperationException($"Brick {brick.Path}: Id is required.");
            if (!Enum.TryParse<BrickKind>(brick["Kind"], ignoreCase: true, out var kind))
                throw new InvalidOperationException($"Brick '{id}': Kind must be Source or Sink.");
            var branch = ConfigurationJson.ToJson(brick.GetSection("Branch")) as JsonObject
                ?? throw new InvalidOperationException($"Brick '{id}': Branch is required.");
            AddBrick(id, kind, brick["Title"] ?? id, branch.ToJsonString(new JsonSerializerOptions { WriteIndented = false }), brick["Description"]);
        }
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
        public ValidateOptionsResult Validate(string? name, DtPipeNodeOptions options) =>
            options.Validate().ToList() is { Count: > 0 } errors
                ? ValidateOptionsResult.Fail(errors.Select(e => "DtPipeNode: " + e))
                : ValidateOptionsResult.Success;
    }
}

/// <summary>A configuration section as JSON: children with indices become an array, values stay text.</summary>
internal static class ConfigurationJson
{
    public static JsonNode? ToJson(IConfigurationSection section)
    {
        var children = section.GetChildren().ToList();
        if (children.Count == 0) return section.Value is null ? null : JsonValue.Create(section.Value);
        if (children.All(c => int.TryParse(c.Key, out _)))
            return new JsonArray(children.OrderBy(c => int.Parse(c.Key)).Select(ToJson).ToArray());
        var json = new JsonObject();
        foreach (var child in children) json[child.Key] = ToJson(child);
        return json;
    }
}
