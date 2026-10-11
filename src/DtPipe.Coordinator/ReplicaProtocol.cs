using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>
/// What the coordinator does with the messages a replica sends on the node channel. A hub forwards each of its
/// methods here, one line apiece, and decides on its own side what the library cannot know (the rights, an
/// enrolment queue). The caller of a message is the connection it arrived on: the replica is found through it, and an
/// argument names nothing but the run and fragment the report is about.
/// </summary>
public sealed class ReplicaProtocol
{
    private readonly ReplicaRegistry _registry;
    private readonly IRunOrchestrator _orchestrator;
    private readonly IStateStore _state;
    private readonly AdmissionOptions _options;
    private readonly ILogger<ReplicaProtocol> _logger;

    public ReplicaProtocol(
        ReplicaRegistry registry, IRunOrchestrator orchestrator, IStateStore state, ILogger<ReplicaProtocol> logger, AdmissionOptions? options = null)
    {
        _registry = registry;
        _orchestrator = orchestrator;
        _state = state;
        _logger = logger;
        _options = options ?? new AdmissionOptions();
    }

    /// <summary>
    /// A replica announced itself. <paramref name="subject"/> is the identity its connection attests, and the node's key;
    /// a hub that attests none (see <see cref="AdmissionOptions.AdmitUnboundFragments"/>) files the replica under the name it
    /// proposes. The announcement is refused, and the reason returned, when no key can be found for it, when its data
    /// plane client is registered under another subject than its connection's, or when another replica announced the same
    /// client. The caller closes the connection of a refused one.
    /// </summary>
    public async Task<(ReplicaKey? Replica, string? Refusal)> OnAnnouncedAsync(
        string connectionId, string? subject, NodeAnnouncement announcement, IReadOnlyList<string> groups, Action abort)
    {
        var node = subject ?? (_options.AdmitUnboundFragments ? announcement.Name : null);
        if (node is null) return (null, "its connection attests no subject.");

        if (announcement.DataPlaneClientId is { } client)
        {
            // The authority is the admission, which reads the registration at the moment of choosing; this only
            // refuses early a replica that claims a client some other identity holds, so it is never admitted.
            var registration = await _state.GetClientAsync(client);
            if (registration is not null && !_options.AdmitUnboundFragments
                && !string.Equals(registration.Subject, subject, StringComparison.Ordinal))
                return (null, $"its data plane client {client} is registered under another subject.");
            var candidate = new ReplicaKey(node, announcement.InstanceId?.ToString("N") ?? "connection-" + connectionId);
            if (_registry.DataPlaneClientHeldByOther(client, candidate))
                return (null, $"its data plane client {client} is announced by another replica.");
        }

        return (_registry.Announce(connectionId, node, announcement, groups, abort), null);
    }

    /// <summary>The replica reports how one fragment of one run ended. Only the replica the run was placed on is heard, and only under the token of the launch that started the child.</summary>
    public async Task<ExitedAck> OnExitedAsync(string connectionId, FragmentExit exit)
    {
        if (_registry.OfConnection(connectionId) is not { } replica)
        {
            _logger.LogWarning("Exited for {Fragment} of run {RunId} on a connection that announced nothing", exit.Fragment, exit.RunId);
            return ExitedAck.UnknownConnection;
        }
        return await _orchestrator.OnExitedAsync(replica.Key, exit);
    }

    /// <summary>The replica is leaving for good: its runs end now, with no grace.</summary>
    public void OnLeave(string connectionId) => _registry.Leave(connectionId);

    /// <summary>A connection closed. The replica waits out its grace period for the same host to announce again.</summary>
    public void OnDisconnected(string connectionId) => _registry.Disconnected(connectionId);
}
