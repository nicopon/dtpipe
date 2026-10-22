using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Xunit;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// <c>dtpipe node</c> as a process: the real binary reads its bricks from a job file, takes its client secret from a
/// reference, announces to a coordinator, and writes the secret nowhere a reader of the announcement or the log could find it.
/// </summary>
[Collection(ChildExitTimingCollection.Name)]
public sealed class NodeCommandTests
{
    private const string Secret = "node-secret-0123456789-abcdef";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_Node_Started_By_The_Command_Announces_Its_Bricks_And_Shows_Its_Secret_Nowhere()
    {
        var received = new ConcurrentBag<string>();
        await using var idp = await StartIdpAsync(received);
        await using var coordinator = await CoordinatorTestHost.StartAsync(_ => { });
        var dir = Directory.CreateTempSubdirectory("node-command-").FullName;
        var job = Path.Combine(dir, "bricks.yaml");
        await File.WriteAllTextAsync(job, "numbers:\n  input: generate:5\ntotals:\n  output: csv:" + Path.Combine(dir, "totals.csv") + "\n", Ct);

        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in new[] { "node", "--job", job, "--name", "node-cmd", "--coordinator", coordinator.Url, "--client-secret", "${{NODE_COMMAND_SECRET}}" })
            psi.ArgumentList.Add(arg);
        psi.Environment["NODE_COMMAND_SECRET"] = Secret;
        psi.Environment["DtPipeNode__Oidc__Authority"] = idp.Url;
        psi.Environment["DtPipeNode__StateDirectory"] = dir;

        using var process = Process.Start(psi)!;
        var log = new StringBuilder();
        process.OutputDataReceived += (_, e) => { lock (log) log.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { lock (log) log.AppendLine(e.Data); };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        try
        {
            var registry = coordinator.Host.Services.GetRequiredService<ReplicaRegistry>();
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (registry.Replicas("node-cmd").Count == 0 && DateTime.UtcNow < deadline && !process.HasExited)
                await Task.Delay(100, Ct);

            var replica = Assert.Single(registry.Replicas("node-cmd"));
            var bricks = replica.Announcement.Bricks.OrderBy(b => b.Id, StringComparer.Ordinal).ToList();
            Assert.Equal(["numbers", "totals"], bricks.Select(b => b.Id));
            Assert.Equal([BrickKind.Source, BrickKind.Sink], bricks.Select(b => b.Kind));
            Assert.All(bricks, b => Assert.Matches("^[0-9a-f]{12}$", b.Version));
            Assert.DoesNotContain(Secret, JsonSerializer.Serialize(replica.Announcement), StringComparison.Ordinal);
            Assert.Contains(Secret, received);
        }
        finally
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(Ct);
        }
        lock (log) Assert.DoesNotContain(Secret, log.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void A_Secret_Given_As_A_Value_Is_Refused_Before_Anything_Starts()
    {
        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true };
        foreach (var arg in new[] { "node", "-i", "generate:1", "--alias", "a", "--client-secret", Secret })
            psi.ArgumentList.Add(arg);

        using var process = Process.Start(psi)!;
        var stderr = process.StandardError.ReadToEnd();
        Assert.True(process.WaitForExit(30_000));

        Assert.Equal(1, process.ExitCode);
        Assert.Contains("reference", stderr, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, stderr, StringComparison.Ordinal);
    }

    /// <summary>An identity provider reduced to the two documents a node reads; it records the secret each token request carried.</summary>
    private static async Task<Idp> StartIdpAsync(ConcurrentBag<string> secrets)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var app = builder.Build();
        app.MapGet("/.well-known/openid-configuration", (HttpRequest r) =>
            Results.Json(new { token_endpoint = $"{r.Scheme}://{r.Host}/token" }));
        app.MapPost("/token", async (HttpRequest r) =>
        {
            var form = await r.ReadFormAsync();
            secrets.Add(form["client_secret"].ToString());
            return Results.Json(new { access_token = "test-token", expires_in = 3600 });
        });
        await app.StartAsync();
        return new Idp(app, app.Urls.First());
    }

    private sealed class Idp(WebApplication app, string url) : IAsyncDisposable
    {
        public string Url => url;
        public ValueTask DisposeAsync() => app.DisposeAsync();
    }
}
