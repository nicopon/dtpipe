namespace DtPipe.Coordinator.Server;

/// <summary>
/// Rights: who may send what to whom. The page edits them; every change is audited, naming its author by <see cref="TrustedIdp.CallerOf"/>
/// (an email, or the id of a person verified by a key), and applies at once.
/// </summary>
internal static class RightsEndpoints
{
    public static void MapRights(this RouteGroupBuilder api, RouteGroupBuilder admin)
    {
        api.MapGet("/flow-matrix", (RightsStore rights) => new { groups = rights.Snapshot().Matrix });

        api.MapGet("/rights", (RightsStore rights, NodeInventory inventory, EnrollmentQueue enrollment, CoordinatorServerOptions options) =>
        {
            var doc = rights.Snapshot();
            var online = inventory.Snapshot().Where(n => n.Online).Select(n => n.Name).ToHashSet();
            var now = DateTimeOffset.UtcNow;
            return new
            {
                // None without a provider: every identity is verified by a key.
                idp = options.Authority is { } authority ? new { issuer = authority, audience = options.Audience, grant = "client_credentials" } : null,
                identities = doc.Identities.Select(i => new
                {
                    i.ClientId, i.Name, i.Description, i.Groups, i.Enabled, i.Kind, i.Pipelines, i.Role, i.Account,
                    auth = i.Auth.Mode, hasKey = i.Auth.LiveAt(now).Any(),
                    connected = i.IsNode && online.Contains(i.Name) && inventory.ClientIdOf(i.Name) == i.ClientId,
                }),
                groups = rights.Groups(),
                pending = enrollment.List(),
                matrix = doc.Matrix,
                audit = doc.Audit,
            };
        });
        admin.MapPut("/rights/matrix", (MatrixRequest request, RightsStore rights, EventBus bus, HttpContext http) =>
        {
            rights.SetMatrix(request.Matrix, TrustedIdp.CallerOf(http));
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapPost("/rights/identities", (IdentityRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http, CoordinatorServerOptions options) =>
        {
            if (!PipelineLibrary.IsValidId(request.ClientId)) throw new ConflictException($"'{request.ClientId}' is not a client id: lowercase letters, digits and '-'.");
            // An identity needs the issuer that verifies it, or it could never connect: a key needs the key issuer, a person's key people
            // allowed to hold one too, and an identity of the provider a provider.
            var auth = request.Auth?.Trim() is { Length: > 0 } mode ? mode : AuthModes.Idp;
            if (auth == AuthModes.Key && KeyIssuer.Unavailable(options) is { } reason)
                throw new ConflictException($"'{request.ClientId}' cannot be verified by a key: {reason}.");
            if (auth == AuthModes.Key && request.Kind?.Trim() == IdentityKinds.Person && KeyIssuer.PersonUnavailable(options) is { } refusal)
                throw new ConflictException($"'{request.ClientId}' cannot be a person verified by a key: {refusal}.");
            if (auth == AuthModes.Idp && TrustedIdp.ProviderUnavailable(options) is { } none)
                throw new ConflictException($"'{request.ClientId}' cannot be verified by the identity provider: {none}.");
            rights.AddIdentity(new Identity(request.ClientId, string.IsNullOrWhiteSpace(request.Name) ? request.ClientId : request.Name, request.Description ?? "",
                request.Groups ?? [], request.Enabled ?? true, request.Kind ?? IdentityKinds.Node, request.Pipelines)
            {
                Auth = new IdentityAuth(auth, []),
                Role = request.Role,
                Account = request.Account,
            }, TrustedIdp.CallerOf(http));
            enrollment.Remove(request.ClientId);
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapPut("/rights/identities/{id}", (string id, IdentityRequest request, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
        {
            var before = rights.Find(id);
            rights.UpdateIdentity(id, request.Name, request.Description, request.Groups, request.Enabled, TrustedIdp.CallerOf(http), request.Pipelines, request.Role,
                request.Account);
            // A node renamed connects again under its new name. A node whose groups changed connects again too, and its data
            // plane client registers again: until then a replica's registration holds the groups it had, and admission
            // places no run on it. A node whose identity is disabled loses its connections: its hosts are refused when they
            // announce again, and the runs placed on them end. Enabling it again drops nothing, no host being connected.
            if (before is not null)
            {
                var after = rights.Find(id)!;
                if (after.Name != before.Name || !after.Groups.ToHashSet().SetEquals(before.Groups) || (before.Enabled && !after.Enabled))
                    inventory.Disconnect(id);
            }
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapDelete("/rights/identities/{id}", (string id, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
        {
            rights.RemoveIdentity(id, TrustedIdp.CallerOf(http));
            inventory.Disconnect(id);
            inventory.Forget(id);
            bus.Publish("rights", new { });
            return Results.Accepted();
        });
        // A host no identity knows waits here: the administrator names it and gives it its groups, or sets it aside.
        admin.MapPost("/rights/pending/{subject}/accept", (string subject, AcceptRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http) =>
        {
            var pending = enrollment.Find(subject) ?? throw new ConflictException($"'{subject}' does not wait for approval.");
            rights.AddIdentity(new Identity(subject, request.Name ?? "", request.Description ?? pending.Description, request.Groups ?? [], true), TrustedIdp.CallerOf(http));
            enrollment.Remove(subject);
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapPost("/rights/pending/{subject}/ignore", (string subject, EnrollmentQueue enrollment, EventBus bus) =>
        {
            enrollment.Ignore(subject);
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        // TransportR records a node's group when it connects and when it renews its token: dropping the control
        // connection of its replicas makes them register their data plane clients again under the current group.
        admin.MapPost("/rights/identities/{id}/reconnect", (string id, NodeInventory inventory) =>
            Results.Ok(new { disconnected = inventory.Disconnect(id) }));
        // An identity verified by a key: the call that makes a key answers it, once, and the rights keep its hash. No answer carries a
        // hash; a key is listed by its id, its last four characters and its dates. A key made while one is live starts a rotation.
        admin.MapPost("/rights/identities/{id}/keys", (string id, KeyRequest? request, RightsStore rights, EventBus bus, HttpContext http) =>
        {
            var (key, made) = rights.AddKey(id, request?.Label, request?.Expires, TrustedIdp.CallerOf(http));
            bus.Publish("rights", new { });
            http.Response.Headers.CacheControl = "no-store";
            return Results.Ok(new { key = key.Reveal(), made.Id, made.Hint, made.Label, made.Created, made.Expires });
        });
        admin.MapGet("/rights/identities/{id}/keys", (string id, RightsStore rights) =>
        {
            if (rights.Find(id) is not { } identity) return Results.NotFound(new { error = $"'{id}' does not exist." });
            var now = DateTimeOffset.UtcNow;
            return Results.Ok(new
            {
                auth = identity.Auth.Mode,
                hasKey = identity.Auth.LiveAt(now).Any(),
                keys = identity.Auth.Keys.Select(k => new { k.Id, k.Hint, k.Label, k.Created, k.Expires, k.Revoked, live = k.LiveAt(now) }),
            });
        });
        // A revoked key opens nothing more, and the identity's replicas are cut so that none goes on under what it opened.
        admin.MapPost("/rights/identities/{id}/keys/{keyId}/revoke", (string id, string keyId, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
        {
            rights.RevokeKey(id, keyId, TrustedIdp.CallerOf(http));
            inventory.Disconnect(id);
            bus.Publish("rights", new { });
            return Results.Ok();
        });
    }
}

internal sealed record MatrixRequest(Dictionary<string, List<string>> Matrix);
internal sealed record AcceptRequest(string? Name, string? Description, List<string>? Groups);
/// <summary>
/// An identity to add or edit. <see cref="Auth"/> is read when it is added, and an edit leaves it as it was, as it does <see cref="Kind"/>;
/// <see cref="Role"/> is a person's, and <see cref="Account"/> a person's of the identity provider.
/// </summary>
internal sealed record IdentityRequest(
    string ClientId, string? Name, string? Description, List<string>? Groups, bool? Enabled, string? Kind = null, List<string>? Pipelines = null, string? Auth = null,
    string? Role = null, string? Account = null);
internal sealed record KeyRequest(string? Label, DateTimeOffset? Expires);
