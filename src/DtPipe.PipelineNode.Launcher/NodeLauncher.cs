using System.Text;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DtPipe.PipelineNode.Launcher;

/// <summary>A brick the command line declared: <see cref="Yaml"/> is its branch, as YAML text.</summary>
public sealed record LaunchedBrick(string Id, bool IsSource, string Yaml);

/// <summary>
/// What the command line gives a node beyond its environment (<c>DtPipeNode__*</c>); a null leaves the environment's value.
/// <see cref="ClientSecret"/> and <see cref="EndToEndKey"/> are the secrets already resolved from their references, and
/// <see cref="ToString"/> prints neither. A <see cref="Runner"/> hosts no data and runs any fragment it is sent.
/// <see cref="EndToEnd"/> turns on the end-to-end encryption of the data plane; the key is the node's own and goes nowhere else.
/// </summary>
public sealed record LaunchSettings(string? Name, string? CoordinatorUrl, string? Description, string? ClientSecret, string DtPipe, bool Runner = false,
    bool EndToEnd = false, string? EndToEndKey = null)
{
    // The record's own text would print every member, the two secrets included.
    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append($"Name = {Name}, CoordinatorUrl = {CoordinatorUrl}, Description = {Description}, DtPipe = {DtPipe}, Runner = {Runner}, EndToEnd = {EndToEnd}");
        return true;
    }
}

/// <summary>Runs the node host <c>dtpipe node</c> stands for, in this assembly so that no other starts TransportR.</summary>
public static class NodeLauncher
{
    private const string RunnerDescription = "Runner: hosts no data, runs every step between the bricks";
    private const string EncryptionSection = "DtPipeNode:EndToEndEncryption";

    /// <summary>
    /// Serves until the process is asked to stop, which is a cancellation (130, as <c>dtpipe mcp</c> ends), or says why it cannot
    /// start (1). A node has no work of its own to finish, so a stop that returned 0 would read as a success.
    /// </summary>
    public static async Task<int> RunAsync(LaunchSettings settings, IReadOnlyList<LaunchedBrick> bricks, CancellationToken ct)
    {
        foreach (var brick in bricks)
        {
            if (BrickRules.DeclaredRefusal(brick.Id, JobYaml.ParseBranch(brick.Yaml)) is { } refusal)
            {
                Console.Error.WriteLine($"[ERROR] Brick '{brick.Id}': {refusal}");
                return 1;
            }
        }

        // No command-line provider: the host reads its environment and what is set below, so a flag cannot reach a setting
        // that `dtpipe node` does not name.
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { ContentRootPath = AppContext.BaseDirectory });
        var overrides = new Dictionary<string, string?>();
        void Set(string key, string? value) { if (value is { Length: > 0 }) overrides[key] = value; }
        Set("DtPipeNode:Name", settings.Name);
        Set("DtPipeNode:CoordinatorUrl", settings.CoordinatorUrl);
        Set("DtPipeNode:Description", settings.Description);
        // A runner says what it is unless it was given a description, by option or environment.
        if (settings.Runner && !overrides.ContainsKey("DtPipeNode:Description") && builder.Configuration["DtPipeNode:Description"] is not { Length: > 0 })
            Set("DtPipeNode:Description", RunnerDescription);
        if (builder.Configuration["DtPipeNode:DtPipe"] is not { Length: > 0 }) Set("DtPipeNode:DtPipe", settings.DtPipe);

        Set("DtPipeNode:Oidc:ClientSecret", settings.ClientSecret);
        if (settings.EndToEnd) Set($"{EncryptionSection}:Enabled", "true");
        Set($"{EncryptionSection}:PreSharedKey", settings.EndToEndKey);
        builder.Configuration.AddInMemoryCollection(overrides);

        // The mode and the key are read from the configuration the host will see, so the flags and the environment agree. The
        // host refuses a key without a mode too, in the words of its settings; this names the flags.
        var sealedEndToEnd = bool.TryParse(builder.Configuration[$"{EncryptionSection}:Enabled"], out var enabled) && enabled;
        var hasKey = builder.Configuration[$"{EncryptionSection}:PreSharedKey"] is { Length: > 0 }
            || builder.Configuration[$"{EncryptionSection}:PreSharedKeyFile"] is { Length: > 0 };
        if (settings.EndToEndKey is { Length: > 0 } && !sealedEndToEnd)
        {
            Console.Error.WriteLine("[ERROR] A key is given (--e2e-key, or DtPipeNode__EndToEndEncryption__PreSharedKey) and the encryption is off: a key alone seals nothing. Add --e2e, or drop the key.");
            return 1;
        }

        builder.Logging.AddFilter("TransportR", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);
        builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });

        var node = builder.Services.AddDtPipeNode(builder.Configuration.GetSection("DtPipeNode"));
        foreach (var brick in bricks)
            node.AddBrick(brick.Id, brick.IsSource ? BrickKind.Source : BrickKind.Sink, brick.Id, brick.Yaml);
        if (settings.Runner) node.RunAnyFragment();

        using var host = builder.Build();
        if (sealedEndToEnd && !hasKey)
            Console.Error.WriteLine("[WARN] End-to-end encryption is on without a key: it protects against whoever reads the path, not against a hub that rewrites batches. Add --e2e-key (the same on every node) so that a rewrite fails the transfer.");
        try
        {
            await host.RunAsync(ct);
            return Environment.ExitCode != 0 ? Environment.ExitCode : 130;
        }
        catch (OperationCanceledException)
        {
            return 130;
        }
        catch (OptionsValidationException ex)
        {
            Console.Error.WriteLine("[ERROR] " + string.Join("\n[ERROR] ", ex.Failures));
            return 1;
        }
    }
}
