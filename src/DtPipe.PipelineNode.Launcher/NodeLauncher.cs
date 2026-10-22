using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DtPipe.PipelineNode.Launcher;

/// <summary>A brick the command line declared: <see cref="BranchJson"/> is its branch, as JSON.</summary>
public sealed record LaunchedBrick(string Id, bool IsSource, string BranchJson);

/// <summary>
/// What the command line gives a node beyond its environment (<c>DtPipeNode__*</c>); a null leaves the environment's value.
/// <see cref="ClientSecret"/> is the secret already resolved from its reference.
/// </summary>
public sealed record LaunchSettings(string? Name, string? CoordinatorUrl, string? Description, string? ClientSecret, string DtPipe);

/// <summary>Runs the node host <c>dtpipe node</c> stands for, in this assembly so that no other starts TransportR.</summary>
public static class NodeLauncher
{
    /// <summary>
    /// Serves until the process is asked to stop, which is a cancellation (130, as <c>dtpipe mcp</c> ends), or says why it cannot
    /// start (1). A node has no work of its own to finish, so a stop that returned 0 would read as a success.
    /// </summary>
    public static async Task<int> RunAsync(LaunchSettings settings, IReadOnlyList<LaunchedBrick> bricks, CancellationToken ct)
    {
        foreach (var brick in bricks)
        {
            if (BrickRules.DeclaredRefusal(brick.Id, brick.BranchJson) is { } refusal)
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
        if (builder.Configuration["DtPipeNode:DtPipe"] is not { Length: > 0 }) Set("DtPipeNode:DtPipe", settings.DtPipe);

        Set("DtPipeNode:Oidc:ClientSecret", settings.ClientSecret);
        builder.Configuration.AddInMemoryCollection(overrides);

        builder.Logging.AddFilter("TransportR", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);
        builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });

        var node = builder.Services.AddDtPipeNode(builder.Configuration.GetSection("DtPipeNode"));
        foreach (var brick in bricks)
            node.AddBrick(brick.Id, brick.IsSource ? BrickKind.Source : BrickKind.Sink, brick.Id, brick.BranchJson);

        using var host = builder.Build();
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
