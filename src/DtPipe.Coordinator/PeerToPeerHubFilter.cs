using Microsoft.AspNetCore.SignalR;
using TransportR.Hub.SignalR.Hubs;

namespace DtPipe.Coordinator;

/// <summary>
/// Refuses the two hub methods a peer could use to reach another peer directly instead of going
/// through the coordinator: discovery (<see cref="CommandHub.GetReceivers"/>) and opening a
/// transfer itself (<see cref="CommandHub.InitTransfer"/>). <c>ITransferInitiator</c>, which the
/// coordinator uses to open a transfer on a peer's behalf, invokes the client directly and never
/// calls a hub method, so this filter cannot see or block it.
/// </summary>
public class PeerToPeerHubFilter : IHubFilter
{
    private static readonly HashSet<string> ForbiddenMethods = new(StringComparer.OrdinalIgnoreCase)
    {
        nameof(CommandHub.GetReceivers),
        nameof(CommandHub.InitTransfer),
    };

    public ValueTask<object?> InvokeMethodAsync(
        HubInvocationContext invocationContext, Func<HubInvocationContext, ValueTask<object?>> next)
    {
        if (ForbiddenMethods.Contains(invocationContext.HubMethodName))
        {
            throw new HubException(
                $"'{invocationContext.HubMethodName}' is reserved to the coordinator; peers do not address each other directly.");
        }

        return next(invocationContext);
    }
}
