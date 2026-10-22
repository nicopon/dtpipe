using System.CommandLine;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Node;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Commands;

/// <summary>
/// <c>dtpipe node [options] (-i … | -o …) --alias id …</c>: this process is a data node of a distributed pipeline. Each branch of
/// the pipeline grammar is a brick the node offers, named by its alias; <c>--job</c> reads them from a file. Its settings are
/// <c>DtPipeNode__*</c> in the environment; the options here only override the three that name it. A secret is never an argument:
/// the client secret is a reference (<c>keyring://alias</c>, <c>${{NAME}}</c>), read once at start.
/// </summary>
public sealed class NodeCommand : Command
{
    private const string EnvironmentPrefix = "DtPipeNode";

    public NodeCommand(IServiceProvider serviceProvider)
        : base("node", "Run as a data node of a distributed pipeline: each branch given is a brick it offers")
    {
        var name = new Option<string?>("--name") { Description = "The node's name (default: DtPipeNode__Name)" };
        var coordinator = new Option<string?>("--coordinator") { Description = "The coordinator's address (default: DtPipeNode__CoordinatorUrl)" };
        var description = new Option<string?>("--description") { Description = "What the node is, shown beside its name" };
        var clientSecret = new Option<string?>("--client-secret")
        {
            Description = "The node's secret at the identity provider, as a reference: keyring://alias, ${{NAME}} or @file (default: DtPipeNode__Oidc__ClientSecret)",
        };
        Options.Add(name);
        Options.Add(coordinator);
        Options.Add(description);
        Options.Add(clientSecret);
        // The rest is the pipeline grammar, read by the lexer.
        TreatUnmatchedTokensAsErrors = false;

        this.SetAction(async (parseResult, ct) =>
        {
            var tokens = parseResult.UnmatchedTokens.ToList();
            if (tokens.FirstOrDefault(IsSettingToken) is { } setting)
            {
                Console.Error.WriteLine($"[ERROR] '{setting}': a node's settings come from the environment (DtPipeNode__*) or from --name, --coordinator, --description and --client-secret.");
                return 1;
            }

            var secrets = serviceProvider.GetRequiredService<ISecretsManager>();
            var reference = parseResult.GetValue(clientSecret);
            if (reference is { } literal && !NodeBricks.IsSecretReference(literal))
            {
                Console.Error.WriteLine("[ERROR] --client-secret takes a reference, never the secret: keyring://alias, ${{NAME}} or @file.");
                return 1;
            }
            reference ??= Environment.GetEnvironmentVariable($"{EnvironmentPrefix}__Oidc__ClientSecret") is { } fromEnvironment
                && NodeBricks.IsSecretReference(fromEnvironment) ? fromEnvironment : null;

            string? secret = null;
            if (reference is not null)
            {
                try { secret = await CompositeStringContentResolver.Standard(secrets).ResolveAsync(reference, ct); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Console.Error.WriteLine($"[ERROR] The client secret could not be read: {ex.Message}");
                    return 1;
                }
                if (string.IsNullOrEmpty(secret))
                {
                    Console.Error.WriteLine("[ERROR] The client secret resolves to nothing.");
                    return 1;
                }
            }

            string yaml;
            try { yaml = CommandLineJob.Export(serviceProvider, tokens); }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Console.Error.WriteLine($"[ERROR] {ex.Message}");
                return 1;
            }

            var (bricks, refusal) = NodeBricks.Read(yaml);
            if (refusal is not null)
            {
                Console.Error.WriteLine($"[ERROR] {refusal}");
                return 1;
            }

            // The binary every fragment runs as: this one, unless the process is the dotnet host (dotnet run).
            var self = Environment.ProcessPath;
            var dtpipe = self is null || Path.GetFileNameWithoutExtension(self).Equals("dotnet", StringComparison.OrdinalIgnoreCase) ? "dtpipe" : self;

            return await NodeHostEntry.RunAsync(
                parseResult.GetValue(name), parseResult.GetValue(coordinator), parseResult.GetValue(description), secret, dtpipe, bricks, ct);
        });
    }

    // A token that names a node setting: only the environment and the four options above set one.
    private static bool IsSettingToken(string token) =>
        token.Contains($"{EnvironmentPrefix}:", StringComparison.OrdinalIgnoreCase)
        || token.Contains($"{EnvironmentPrefix}__", StringComparison.OrdinalIgnoreCase);
}
