using System.Net;
using Microsoft.AspNetCore.HttpOverrides;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The peers whose <c>X-Forwarded-Proto</c> and <c>X-Forwarded-Host</c> the server believes (<c>Coordinator:TrustedProxies</c>, a list of
/// addresses and CIDR networks). The server listens in plain HTTP and a proxy in front terminates TLS: what the sign-in redirect, the
/// protected-resource document and the cookies' <c>Secure</c> flag say about the scheme and the host is only right when the proxy's headers are read.
/// Empty, nothing is read and the loopback is no exception, so a header sent straight to the server changes nothing.
/// </summary>
internal static class TrustedProxies
{
    /// <summary>The middleware's options, or none when no proxy is trusted. A malformed entry stops the start, naming it.</summary>
    public static ForwardedHeadersOptions? OptionsFor(IReadOnlyList<string> entries)
    {
        if (entries.Count == 0) return null;
        var options = new ForwardedHeadersOptions
        {
            // Proto and host only: nothing here reads the caller's address.
            ForwardedHeaders = ForwardedHeaders.XForwardedProto | ForwardedHeaders.XForwardedHost,
        };
        // The defaults trust the loopback, which would believe a header sent from the container itself.
        options.KnownProxies.Clear();
        options.KnownIPNetworks.Clear();
        foreach (var entry in entries)
        {
            if (entry.Contains('/') && System.Net.IPNetwork.TryParse(entry, out var network)) options.KnownIPNetworks.Add(network);
            else if (!entry.Contains('/') && IPAddress.TryParse(entry, out var address)) options.KnownProxies.Add(address);
            else throw new InvalidOperationException($"Coordinator:TrustedProxies: '{entry}' is neither an address nor a network in CIDR form (10.0.0.0/8).");
        }
        return options;
    }
}
