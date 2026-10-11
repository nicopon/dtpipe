using DtPipe.Coordinator.Contracts;
using TransportR.Models;

namespace DtPipe.Coordinator;

/// <summary>
/// Whether a replica may run a fragment. Pure: it reads what a replica announced and what TransportR holds of its
/// data plane client, and answers with the reason a replica is out, or null when it is in. Whether the replica is
/// connected, and which of the eligible ones a run takes, is the registry's (<see cref="ReplicaRegistry.TryReserve"/>).
/// </summary>
internal static class ReplicaChooser
{
    /// <summary>
    /// Why the replica cannot compose <paramref name="spec"/> from what it announced, or null. A fragment made of
    /// bricks must compose, from the bricks the replica declares, to the version of the plan; a fragment written as a
    /// job needs a replica that runs any fragment or accepts one as written. A replica at another version of a brick
    /// (a rolling update) is out for that fragment, not a reason to refuse the run while another is in.
    /// </summary>
    public static string? CompositionRefusal(FragmentSpec spec, NodeAnnouncement announcement)
    {
        if (spec.Units is not null)
        {
            if (announcement.Role == NodeRole.Runner) return "a runner has no brick to compose a job from.";
            return FragmentJob.ComposeFor(spec, FragmentJob.Over(announcement.Bricks)).Reason;
        }
        if (spec.Yaml is null) return $"{spec.Fragment} carries neither bricks nor a job.";
        if (announcement.Role == NodeRole.Runner || announcement.Offers(NodeAnnouncement.RawFragments)) return null;
        return "this node runs the bricks its owner declared, by id, and takes no job as written.";
    }

    /// <summary>
    /// Why the replica cannot be placed on a run for want of a launch token, or null: it announced
    /// <see cref="NodeAnnouncement.LaunchToken"/>, so its report carries the token of the launch it answers. A replica
    /// that does not (an older version) is out, as a replica at another version of a brick is, and not a reason to refuse
    /// the run while another replica of the node is in.
    /// </summary>
    public static string? LaunchTokenRefusal(NodeAnnouncement announcement) =>
        announcement.Offers(NodeAnnouncement.LaunchToken)
            ? null
            : "it runs a version that does not return a launch's token: update it.";

    /// <summary>
    /// Why the replica's TransportR client cannot carry the run's bytes, or null: it announced a client, that client
    /// is registered, still connected, under the subject of the replica's connection, with exactly the groups the
    /// rights give the node today. Closed by default, so a registration written before the rights changed is out until
    /// the client registers again. <paramref name="admitUnbound"/> is a hub that attests no subject: there is no
    /// subject to compare, and groups the caller did not name are not compared.
    /// </summary>
    public static string? DataPlaneRefusal(ReplicaView replica, ClientRegistration? registration, IReadOnlyList<string>? expectedGroups, bool admitUnbound)
    {
        if (replica.DataPlaneClientId is null) return "it announced no data plane client.";
        if (registration is null) return "its data plane client is not registered.";
        if (registration.DisconnectedAtUtc is not null) return "its data plane client is disconnected.";
        if (!admitUnbound && !string.Equals(registration.Subject, replica.Key.Node, StringComparison.Ordinal))
            return "its data plane client is registered under another subject than its connection.";
        if (expectedGroups is { } expected && !registration.Groups.SetEquals(expected))
            return $"its data plane client holds the groups [{string.Join(", ", registration.Groups.Order(StringComparer.Ordinal))}], the node's are [{string.Join(", ", expected.Order(StringComparer.Ordinal))}].";
        return null;
    }
}
