using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.PipelineNode.Launcher;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// <c>dtpipe node</c> as a process: the real binary reads its bricks from a job file, takes its client secret and its end-to-end
/// key from references, announces to a coordinator, and puts neither where a reader of the announcement or the log could find it.
/// </summary>
[Collection(ChildExitTimingCollection.Name)]
public sealed class NodeCommandTests
{
    private const string Secret = "node-secret-0123456789-abcdef";
    private const string RowsValue = "73117";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_Node_Started_By_The_Command_Announces_Its_Bricks_And_Shows_Its_Secret_Nowhere()
    {
        var received = new ConcurrentBag<string>();
        await using var idp = await StartIdpAsync(received);
        await using var coordinator = await CoordinatorTestHost.StartAsync(_ => { });
        var dir = Directory.CreateTempSubdirectory("node-command-").FullName;
        var job = Path.Combine(dir, "bricks.yaml");
        await File.WriteAllTextAsync(job, "numbers:\n  input: generate:5\ntotals:\n  output: csv:" + Path.Combine(dir, "totals.csv") + "\n"
            + "tokens:\n  input: generate:${{NODE_COMMAND_ROWS}}\nkept:\n  output: csv:keyring://node-command-alias.csv\n", Ct);

        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in new[] { "node", "--job", job, "--name", "node-cmd", "--coordinator", coordinator.Url, "--client-secret", "${{NODE_COMMAND_SECRET}}" })
            psi.ArgumentList.Add(arg);
        psi.Environment["NODE_COMMAND_SECRET"] = Secret;
        psi.Environment["NODE_COMMAND_ROWS"] = RowsValue;
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
            Assert.Equal(["kept", "numbers", "tokens", "totals"], bricks.Select(b => b.Id));
            Assert.Equal([BrickKind.Sink, BrickKind.Source, BrickKind.Source, BrickKind.Sink], bricks.Select(b => b.Kind));
            // A reference in the node's file is announced as written: the version hashes the token, never its value.
            Assert.Equal(BrickRules.VersionOf(BrickKind.Source, JobYaml.ParseBranch("{\"input\":\"generate:${{NODE_COMMAND_ROWS}}\",\"batch-size\":32768,\"sampling-rate\":1}")), bricks.Single(b => b.Id == "tokens").Version);
            Assert.Equal(BrickRules.VersionOf(BrickKind.Sink, JobYaml.ParseBranch("{\"output\":\"csv:keyring://node-command-alias.csv\",\"batch-size\":32768,\"sampling-rate\":1}")), bricks.Single(b => b.Id == "kept").Version);
            Assert.DoesNotContain(RowsValue, JsonSerializer.Serialize(replica.Announcement), StringComparison.Ordinal);
            Assert.All(bricks, b => Assert.Matches("^[0-9a-f]{12}$", b.Version));
            Assert.DoesNotContain(Secret, JsonSerializer.Serialize(replica.Announcement), StringComparison.Ordinal);
            Assert.Contains(Secret, received);
        }
        finally
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(Ct);
        }
        lock (log)
        {
            Assert.DoesNotContain(Secret, log.ToString(), StringComparison.Ordinal);
            // The encryption is off by default, and a node that does not ask for it says nothing of it.
            Assert.DoesNotContain("[WARN]", log.ToString(), StringComparison.Ordinal);
        }
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

    [Theory]
    [InlineData("-i", "generate:1", "--alias", "a")]
    [InlineData("--job", "bricks.yaml")]
    public void A_Runner_Given_A_Brick_Is_Refused_By_Naming_Both(params string[] given)
    {
        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true };
        foreach (var arg in new[] { "node", "--runner" }.Concat(given))
            psi.ArgumentList.Add(arg);

        using var process = Process.Start(psi)!;
        var stderr = process.StandardError.ReadToEnd();
        Assert.True(process.WaitForExit(30_000));

        Assert.Equal(1, process.ExitCode);
        Assert.Contains("--runner hosts no data", stderr, StringComparison.Ordinal);
        Assert.Contains($"'{given[0]}'", stderr, StringComparison.Ordinal);
        Assert.Contains("remove --runner", stderr, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_Runner_Needs_No_Brick_And_Announces_Itself_As_A_Runner()
    {
        var received = new ConcurrentBag<string>();
        await using var idp = await StartIdpAsync(received);
        await using var coordinator = await CoordinatorTestHost.StartAsync(_ => { });
        var dir = Directory.CreateTempSubdirectory("node-runner-").FullName;

        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { "node", "--runner", "--name", "runner-cmd", "--coordinator", coordinator.Url, "--client-secret", "${{NODE_COMMAND_SECRET}}" })
            psi.ArgumentList.Add(arg);
        psi.Environment["NODE_COMMAND_SECRET"] = Secret;
        psi.Environment["DtPipeNode__Oidc__Authority"] = idp.Url;
        psi.Environment["DtPipeNode__StateDirectory"] = dir;

        using var process = Process.Start(psi)!;
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        try
        {
            var registry = coordinator.Host.Services.GetRequiredService<ReplicaRegistry>();
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (registry.Replicas("runner-cmd").Count == 0 && DateTime.UtcNow < deadline && !process.HasExited)
                await Task.Delay(100, Ct);

            var announcement = Assert.Single(registry.Replicas("runner-cmd")).Announcement;
            Assert.Equal(NodeRole.Runner, announcement.Role);
            Assert.Empty(announcement.Bricks);
            Assert.Contains("hosts no data", announcement.Description, StringComparison.Ordinal);
        }
        finally
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(Ct);
        }
    }

    [Fact]
    public void An_End_To_End_Key_Given_As_A_Value_Is_Refused_Before_Anything_Starts()
    {
        var key = NewKey();

        var (exit, output) = RunUntilExit(["node", "--runner", "--e2e", "--e2e-key", key], []);

        Assert.Equal(1, exit);
        Assert.Contains("--e2e-key takes a reference", output, StringComparison.Ordinal);
        Assert.DoesNotContain(key, output, StringComparison.Ordinal);
        Assert.All(Lines(output), line => Assert.StartsWith("[ERROR]", line, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("flag", "flag")]
    [InlineData("variable", "flag")]
    [InlineData("flag", "environment")]
    public async Task An_End_To_End_Key_Given_As_A_Reference_Starts_The_Node_And_Shows_Nowhere(string keyFrom, string modeFrom)
    {
        var key = NewKey();
        var args = new List<string>();
        var env = new Dictionary<string, string> { ["NODE_COMMAND_E2E_KEY"] = key };
        if (keyFrom == "flag") args.AddRange(["--e2e-key", "${{NODE_COMMAND_E2E_KEY}}"]);
        else env["DtPipeNode__EndToEndEncryption__PreSharedKey"] = "${{NODE_COMMAND_E2E_KEY}}";
        if (modeFrom == "flag") args.Add("--e2e");
        else env["DtPipeNode__EndToEndEncryption__Enabled"] = "true";

        var (log, announcement) = await RunAnnouncedAsync("node-e2e-key", args, env);

        Assert.DoesNotContain(key, log, StringComparison.Ordinal);
        Assert.DoesNotContain(key, announcement, StringComparison.Ordinal);
        Assert.DoesNotContain("[WARN]", log, StringComparison.Ordinal);
    }

    [Fact]
    public void An_End_To_End_Key_Without_The_Mode_Is_Refused_Naming_Both_Flags()
    {
        var key = NewKey();

        var (exit, output) = RunUntilExit(["node", "--runner", "--e2e-key", "${{NODE_COMMAND_E2E_KEY}}"], new() { ["NODE_COMMAND_E2E_KEY"] = key });

        Assert.Equal(1, exit);
        Assert.Contains("--e2e-key", output, StringComparison.Ordinal);
        Assert.Contains("Add --e2e", output, StringComparison.Ordinal);
        Assert.DoesNotContain(key, output, StringComparison.Ordinal);
        Assert.All(Lines(output), line => Assert.StartsWith("[ERROR]", line, StringComparison.Ordinal));
    }

    [Fact]
    public async Task The_Encryption_Without_A_Key_Warns_Once_And_Starts_The_Node()
    {
        var (log, _) = await RunAnnouncedAsync("node-e2e-nokey", ["--e2e"], []);

        Assert.Equal(1, Regex.Count(log, "End-to-end encryption is on without a key"));
        Assert.Contains("not against a hub that rewrites batches", log, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("EndToEndEncryption:PreSharedKey is not base64 of 32 bytes", "--coordinator", "http://127.0.0.1:1", "--e2e", "--e2e-key", "${{NODE_COMMAND_E2E_KEY}}")]
    [InlineData("CoordinatorUrl must be an absolute URL", "--coordinator", "not-a-url")]
    public void A_Start_Refused_For_An_Option_Prints_Only_Error_Lines(string reason, params string[] given)
    {
        var (exit, output) = RunUntilExit(
            ["node", "--runner", "--name", "node-refused", "--client-secret", "${{NODE_COMMAND_SECRET}}", .. given],
            new() { ["NODE_COMMAND_SECRET"] = Secret, ["NODE_COMMAND_E2E_KEY"] = "not-a-key" });

        Assert.Equal(1, exit);
        Assert.Contains(reason, output, StringComparison.Ordinal);
        Assert.DoesNotContain("Hosting failed to start", output, StringComparison.Ordinal);
        Assert.All(Lines(output), line => Assert.StartsWith("[ERROR]", line, StringComparison.Ordinal));
    }

    [Fact]
    public void A_Setting_Of_The_Wrong_Type_Is_Refused_By_Name_Without_Repeating_Its_Value()
    {
        var key = NewKey();

        var (exit, output) = RunUntilExit(
            ["node", "--runner", "--name", "node-malformed", "--coordinator", "http://127.0.0.1:1", "--client-secret", "${{NODE_COMMAND_SECRET}}"],
            new() { ["NODE_COMMAND_SECRET"] = Secret, ["DtPipeNode__EndToEndEncryption__Enabled"] = key });

        Assert.Equal(1, exit);
        Assert.Contains("DtPipeNode:EndToEndEncryption:Enabled", output, StringComparison.Ordinal);
        Assert.DoesNotContain(key, output, StringComparison.Ordinal);
        Assert.Empty(Regex.Matches(output, @"^\s+at ", RegexOptions.Multiline));
        Assert.All(Lines(output), line => Assert.StartsWith("[ERROR]", line, StringComparison.Ordinal));
    }

    [Fact]
    public void The_Launch_Settings_Print_Neither_The_Key_Nor_The_Client_Secret()
    {
        var key = NewKey();

        var text = new LaunchSettings("node", "http://coordinator", "described", Secret, "dtpipe", Runner: true, EndToEnd: true, EndToEndKey: key).ToString();

        Assert.DoesNotContain(key, text, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, text, StringComparison.Ordinal);
        Assert.Contains("Name = node", text, StringComparison.Ordinal);
    }

    private static string NewKey() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

    private static string[] Lines(string output) => output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Runs the real binary to its end, with the environment it is given, and returns its code and what it printed.</summary>
    private static (int ExitCode, string Output) RunUntilExit(string[] args, Dictionary<string, string> environment)
    {
        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path) { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true };
        foreach (var arg in args)
            psi.ArgumentList.Add(arg);
        foreach (var (name, value) in environment)
            psi.Environment[name] = value;

        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEndAsync(Ct);
        var stderr = process.StandardError.ReadToEndAsync(Ct);
        Assert.True(process.WaitForExit(30_000));
        return (process.ExitCode, stdout.GetAwaiter().GetResult() + stderr.GetAwaiter().GetResult());
    }

    /// <summary>
    /// Starts <c>dtpipe node --runner</c> with <paramref name="args"/> and <paramref name="environment"/> beside the node's own, waits
    /// until it announces itself to a coordinator, stops it, and returns what it printed and the announcement it made.
    /// </summary>
    private static async Task<(string Log, string Announcement)> RunAnnouncedAsync(string name, IEnumerable<string> args, Dictionary<string, string> environment)
    {
        var received = new ConcurrentBag<string>();
        await using var idp = await StartIdpAsync(received);
        await using var coordinator = await CoordinatorTestHost.StartAsync(_ => { });
        var dir = Directory.CreateTempSubdirectory("node-e2e-").FullName;

        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path) { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in new[] { "node", "--runner", "--name", name, "--coordinator", coordinator.Url, "--client-secret", "${{NODE_COMMAND_SECRET}}" }.Concat(args))
            psi.ArgumentList.Add(arg);
        psi.Environment["NODE_COMMAND_SECRET"] = Secret;
        psi.Environment["DtPipeNode__Oidc__Authority"] = idp.Url;
        psi.Environment["DtPipeNode__StateDirectory"] = dir;
        foreach (var (variable, value) in environment)
            psi.Environment[variable] = value;

        using var process = Process.Start(psi)!;
        var log = new StringBuilder();
        process.OutputDataReceived += (_, e) => { lock (log) log.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { lock (log) log.AppendLine(e.Data); };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        string announcement;
        try
        {
            var registry = coordinator.Host.Services.GetRequiredService<ReplicaRegistry>();
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (registry.Replicas(name).Count == 0 && DateTime.UtcNow < deadline && !process.HasExited)
                await Task.Delay(100, Ct);

            announcement = JsonSerializer.Serialize(Assert.Single(registry.Replicas(name)).Announcement);
        }
        finally
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(Ct);
        }
        lock (log) return (log.ToString(), announcement);
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
