using System.Collections.Concurrent;
using System.Security.Claims;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;

namespace DtPipe.Coordinator.Tests.Infrastructure;

/// <summary>
/// Groups per subject, read at every registration: the rights document of the real coordinator
/// (<c>RightsIdentityProvider</c>), which a test changes between two registrations of a client. The subject is the token's
/// <c>sub</c>; a subject with no entry holds no group.
/// </summary>
public sealed class MutableGroups : IClientIdentityProvider
{
    public readonly ConcurrentDictionary<string, string[]> BySubject = new();
    private readonly JwtIdentityProvider _token = new();
    private int _resolutions;

    /// <summary>How many times the hub asked for an identity: once for each client that registers or renews its registration.</summary>
    public int Resolutions => Volatile.Read(ref _resolutions);

    public ClientIdentity? ResolveIdentity(ClaimsPrincipal? user, Guid? providedClientId, IEnumerable<string>? providedGroups)
    {
        Interlocked.Increment(ref _resolutions);
        var identity = _token.ResolveIdentity(user, providedClientId, providedGroups);
        if (identity is null) return null;
        return identity with { Groups = BySubject.TryGetValue(identity.Subject ?? "", out var groups) ? groups.ToHashSet() : [] };
    }
}
