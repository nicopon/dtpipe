using System.Net;
using System.Net.Sockets;
using DtPipe.Coordinator.Server;
using DtPipe.Coordinator.Server.Access;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// A coordinator listening on <paramref name="url"/>, with its state in SQLite under a fresh directory. With an external provider no
/// request here reaches; without one, the deployment gives it its administrator and <paramref name="bootstrap"/>.
/// </summary>
internal sealed class KestrelCoordinator(string url, string state, bool provider = true, IReadOnlyDictionary<string, string>? bootstrap = null) : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        if (provider)
        {
            builder.UseSetting("Coordinator:Authority", "http://idp.invalid");
            builder.UseSetting("Coordinator:Ui:ClientSecret", "ui-secret");
        }
        else
        {
            // The test host hands its settings over as command-line arguments, where a key itself is refused: a file's path is not one.
            var admin = Path.Combine(state, "admin.key");
            File.WriteAllText(admin, ClearKey.New("boss").Reveal());
            builder.UseSetting("Coordinator:Bootstrap:AdminKey", "@" + admin);
        }
        foreach (var (key, value) in bootstrap ?? new Dictionary<string, string>()) builder.UseSetting("Coordinator:Bootstrap:" + key, value);
        builder.UseSetting("Coordinator:StateDirectory", state);
        builder.UseSetting("Coordinator:Initialize", "true");
        builder.UseSetting("Coordinator:PublicUrl", url);
        builder.UseSetting("Coordinator:Auth:AllowHttpKeys", "true");
    }

    internal static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
