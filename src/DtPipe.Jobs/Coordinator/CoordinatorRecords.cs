using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace DtPipe.Jobs.Coordinator;

/// <summary>
/// The coordinator's protected resource metadata (RFC 9728), with what a studio needs besides:
/// the contract it speaks and the provider's client a studio signs people in with, absent when the
/// coordinator accepts no studio. Its names are the RFC's, in snake case.
/// </summary>
public sealed record ResourceMetadata(
    [property: JsonPropertyName("resource")] string Resource,
    [property: JsonPropertyName("authorization_servers")] IReadOnlyList<string> AuthorizationServers,
    [property: JsonPropertyName("bearer_methods_supported")] IReadOnlyList<string> BearerMethodsSupported,
    [property: JsonPropertyName("dtpipe_contract")] int DtpipeContract,
    [property: JsonPropertyName("dtpipe_studio_client_id"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    string? StudioClientId = null);

/// <summary>Who a signed-in person is, and the roles the coordinator's configuration gives them.</summary>
public sealed record CoordinatorMe(string? Name, string? Email, IReadOnlyList<string> Roles);

/// <summary>A node as the studio sees it: <see cref="Role"/> is <c>data</c> or <c>runner</c>.</summary>
public sealed record CatalogNode(string Name, bool Online, string Role, string? DtpipeVersion);

public sealed record CatalogColumn(string Name, string Type, bool IsNullable);

/// <summary>
/// One brick of the catalog. <see cref="Kind"/> is <see cref="StepKinds.Source"/> or
/// <see cref="StepKinds.Sink"/>; <see cref="Branch"/> is the branch its node declares, as JSON;
/// <see cref="Version"/> is the coordinator's, which changes exactly when what dtpipe would run does.
/// </summary>
public sealed record CatalogBrick(
    string Key, string Node, string Id, string Kind, string Title, string Description, JsonObject Branch, string Version,
    IReadOnlyList<CatalogColumn>? Schema, string? SchemaError, bool Online);

public sealed record BrickCatalogView(IReadOnlyList<CatalogNode> Nodes, IReadOnlyList<CatalogBrick> Bricks);

/// <summary>A job whose top-level branches are to be recognised, as <see cref="JobYaml.Serialize"/> writes it.</summary>
public sealed record RecognizeRequest(string Job);

/// <summary>
/// What a branch is, by brick key: the brick it is whole (<see cref="Match"/>), or the brick it starts
/// with (<see cref="Source"/>) or ends with (<see cref="Sink"/>). The coordinator decides; the studio
/// only reads the answer back.
/// </summary>
public sealed record Recognition(string? Match, string? Source, string? Sink);

/// <summary>What each top-level branch of the job is, by alias.</summary>
public sealed record RecognizeResponse(IReadOnlyDictionary<string, Recognition> Branches);

/// <summary>A job saved into the library, with the message of its commit.</summary>
public sealed record LibrarySave(string Yaml, JsonObject? Layout, string? Message);

/// <summary>
/// A save's outcome. <see cref="Commit"/> is the last commit of the pipeline's job, the version a later
/// <c>If-Match</c> names: a plan saved beside it does not change it. <see cref="Redistribute"/>: the plan saved for it no longer matches its job.
/// <see cref="Deployed"/>: a deployment still runs the version before.
/// </summary>
public sealed record LibrarySaved(string Id, string? Commit, bool Redistribute, bool Deployed);

/// <summary>A save refused by its precondition (412), with the pipeline's commit in place, if any.</summary>
public sealed record LibraryConflict(string Error, string? Commit);
