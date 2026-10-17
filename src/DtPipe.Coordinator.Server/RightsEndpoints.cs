namespace DtPipe.Coordinator.Server;

/// <summary>Rights: who may send what to whom. The page edits them; every change is audited and applies at once.</summary>
internal static class RightsEndpoints
{
    public static void MapRights(this RouteGroupBuilder api, RouteGroupBuilder admin)
    {
        api.MapGet("/flow-matrix", (RightsStore rights) => new { groups = rights.Snapshot().Matrix });

        api.MapGet("/rights", (RightsStore rights, NodeInventory inventory, EnrollmentQueue enrollment, CoordinatorServerOptions options) =>
        {
            var doc = rights.Snapshot();
            var online = inventory.Snapshot().Where(n => n.Online).Select(n => n.Name).ToHashSet();
            return new
            {
                idp = new { issuer = options.Authority, audience = options.Audience, grant = "client_credentials" },
                identities = doc.Identities.Select(i => new
                {
                    i.ClientId, i.Name, i.Description, i.Groups, i.Enabled, i.Kind, i.Pipelines,
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
            rights.SetMatrix(request.Matrix, TrustedIdp.EmailOf(http));
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapPost("/rights/identities", (IdentityRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http) =>
        {
            if (!PipelineLibrary.IsValidId(request.ClientId)) throw new ConflictException($"'{request.ClientId}' is not a client id: lowercase letters, digits and '-'.");
            rights.AddIdentity(new Identity(request.ClientId, string.IsNullOrWhiteSpace(request.Name) ? request.ClientId : request.Name, request.Description ?? "",
                request.Groups ?? [], request.Enabled ?? true, request.Kind ?? IdentityKinds.Node, request.Pipelines), TrustedIdp.EmailOf(http));
            enrollment.Remove(request.ClientId);
            bus.Publish("rights", new { });
            return Results.Ok();
        });
        admin.MapPut("/rights/identities/{id}", (string id, IdentityRequest request, RightsStore rights, NodeInventory inventory, EventBus bus, HttpContext http) =>
        {
            var before = rights.Find(id);
            rights.UpdateIdentity(id, request.Name, request.Description, request.Groups, request.Enabled, TrustedIdp.EmailOf(http), request.Pipelines);
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
            rights.RemoveIdentity(id, TrustedIdp.EmailOf(http));
            inventory.Disconnect(id);
            inventory.Forget(id);
            bus.Publish("rights", new { });
            return Results.Accepted();
        });
        // A host no identity knows waits here: the administrator names it and gives it its groups, or sets it aside.
        admin.MapPost("/rights/pending/{subject}/accept", (string subject, AcceptRequest request, RightsStore rights, EnrollmentQueue enrollment, EventBus bus, HttpContext http) =>
        {
            var pending = enrollment.Find(subject) ?? throw new ConflictException($"'{subject}' does not wait for approval.");
            rights.AddIdentity(new Identity(subject, request.Name ?? "", request.Description ?? pending.Description, request.Groups ?? [], true), TrustedIdp.EmailOf(http));
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
    }
}

internal sealed record MatrixRequest(Dictionary<string, List<string>> Matrix);
internal sealed record AcceptRequest(string? Name, string? Description, List<string>? Groups);
internal sealed record IdentityRequest(string ClientId, string? Name, string? Description, List<string>? Groups, bool? Enabled, string? Kind = null, List<string>? Pipelines = null);
