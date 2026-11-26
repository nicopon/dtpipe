namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// Where the coordinator mounts TransportR's hub, the data plane. The routes of the hub (its control channel and its
/// streams) live under this prefix and never under the people's <c>/api</c>, so a proxy can keep <c>/api</c> internal and a
/// route of the coordinator can never collide with one of the hub. A node's TransportR client is given the coordinator's
/// address followed by this prefix, with no trailing slash, the shape of <see cref="NodeChannel.Path"/>.
/// </summary>
public static class DataPlaneRoutes
{
    public const string Prefix = "/transportr";
}
