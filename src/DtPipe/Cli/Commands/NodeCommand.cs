using System.CommandLine;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Node;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Security;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Commands;

/// <summary>
/// <c>dtpipe node [options] (-i … | -o …) --alias id …</c>: this process is a data node of a distributed pipeline. Each branch of
/// the pipeline grammar is a brick the node offers, named by its alias; <c>--job</c> reads them from a file. <c>dtpipe node
/// --runner</c> is the other kind of node: it hosts no data, so it takes no brick, and runs any fragment the coordinator sends.
/// Its settings are <c>DtPipeNode__*</c> in the environment; the options here only override the few that name them. A secret is
/// never an argument: the client secret and the end-to-end key are references (<c>keyring://alias</c>, <c>${{NAME}}</c>, <c>@file</c>),
/// read once at start.
/// </summary>
public sealed class NodeCommand : Command
{
    private const string EnvironmentPrefix = "DtPipeNode";

    public NodeCommand(IServiceProvider serviceProvider)
        : base("node", """
            Run as a node of a distributed pipeline.

            Modes:
              --runner                    Host no data; run any coordinator fragment
              --job <file.yaml>           Host bricks (sources/sinks) from a job file
              -i/-o <target> --alias <id> Host an ad-hoc brick from the command line
            """)
    {
        var runner = new Option<bool>("--runner") { Description = "Host no data and run any fragment the coordinator sends; takes no brick" };
        var job = new Option<string?>("--job")
        {
            Description = "YAML job file declaring the bricks (sources and sinks) offered by this node",
        };
        job.Aliases.Add("-j");
        var name = new Option<string?>("--name") { Description = "The node's name (default: DtPipeNode__Name)" };
        var coordinator = new Option<string?>("--coordinator") { Description = "The coordinator's address (default: DtPipeNode__CoordinatorUrl)" };
        var description = new Option<string?>("--description") { Description = "What the node is, shown beside its name" };
        var clientSecret = new Option<string?>("--client-secret")
        {
            Description = "The node's secret at the identity provider, as a reference: keyring://alias, ${{NAME}} or @file (default: DtPipeNode__Oidc__ClientSecret)",
        };
        var e2e = new Option<bool>("--e2e")
        {
            Description = "Seal every batch of the node's transfers end to end (default: DtPipeNode__EndToEndEncryption__Enabled); every node a transfer joins needs the same setting",
        };
        var e2eKey = new Option<string?>("--e2e-key")
        {
            Description = "The key the transfers are sealed with, shared by every node, as a reference: keyring://alias, ${{NAME}} or @file; needs --e2e (default: DtPipeNode__EndToEndEncryption__PreSharedKey)",
        };
        Options.Add(runner);
        Options.Add(job);
        Options.Add(name);
        Options.Add(coordinator);
        Options.Add(description);
        Options.Add(clientSecret);
        Options.Add(e2e);
        Options.Add(e2eKey);
        // The rest is the pipeline grammar, read by the lexer.
        TreatUnmatchedTokensAsErrors = false;

        this.SetAction(async (parseResult, ct) =>
        {
            var tokens = parseResult.UnmatchedTokens.ToList();
            if (tokens.FirstOrDefault(IsSettingToken) is { } setting)
            {
                Console.Error.WriteLine($"[ERROR] '{setting}': a node's settings come from the environment (DtPipeNode__*) or from --name, --coordinator, --description, --client-secret, --e2e and --e2e-key.");
                return 1;
            }

            var secrets = serviceProvider.GetRequiredService<ISecretsManager>();
            var secretOption = parseResult.GetValue(clientSecret);
            if (secretOption is { } literal && !NodeBricks.IsSecretReference(literal))
            {
                Console.Error.WriteLine($"[ERROR] {NodeBricks.NotAReference("--client-secret", "secret", "DTPIPE_NODE_KEY")}");
                return 1;
            }
            var keyOption = parseResult.GetValue(e2eKey);
            if (keyOption is { } keyLiteral && !NodeBricks.IsSecretReference(keyLiteral))
            {
                Console.Error.WriteLine($"[ERROR] {NodeBricks.NotAReference("--e2e-key", "key", "DTPIPE_E2E_KEY")}");
                return 1;
            }

            var (secret, secretRefusal) = await ReadReferenceAsync(secretOption, "Oidc__ClientSecret", "client secret", secrets, ct);
            if (secretRefusal is not null)
            {
                Console.Error.WriteLine($"[ERROR] {secretRefusal}");
                return 1;
            }
            var (endToEndKey, keyRefusal) = await ReadReferenceAsync(keyOption, "EndToEndEncryption__PreSharedKey", "end-to-end key", secrets, ct);
            if (keyRefusal is not null)
            {
                Console.Error.WriteLine($"[ERROR] {keyRefusal}");
                return 1;
            }

            var isRunner = parseResult.GetValue(runner);
            IReadOnlyList<DeclaredBrick> bricks = [];
            var jobFile = parseResult.GetValue(job);
            if (isRunner)
            {
                var declared = jobFile is not null ? "--job" : tokens.FirstOrDefault();
                if (declared is not null)
                {
                    Console.Error.WriteLine($"[ERROR] --runner hosts no data, so it takes no brick, and '{declared}' declares one: remove --runner to host it, or remove '{declared}' to run as a runner.");
                    return 1;
                }
            }
            else
            {
                if (jobFile is not null)
                    tokens.InsertRange(0, ["--job", jobFile]);

                string yaml;
                try { yaml = CommandLineJob.Export(serviceProvider, tokens); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Console.Error.WriteLine($"[ERROR] {ex.Message}");
                    return 1;
                }

                var (read, refusal) = NodeBricks.Read(yaml);
                if (refusal is not null)
                {
                    Console.Error.WriteLine($"[ERROR] {refusal}");
                    return 1;
                }
                bricks = read;
            }

            // The binary every fragment runs as: this one, unless the process is the dotnet host (dotnet run).
            var self = Environment.ProcessPath;
            var dtpipe = self is null || Path.GetFileNameWithoutExtension(self).Equals("dotnet", StringComparison.OrdinalIgnoreCase) ? "dtpipe" : self;

            return await NodeHostEntry.RunAsync(
                parseResult.GetValue(name), parseResult.GetValue(coordinator), parseResult.GetValue(description), secret, dtpipe, isRunner,
                parseResult.GetValue(e2e), endToEndKey, bricks, ct);
        });
    }

    /// <summary>
    /// The secret an option or, without it, the setting's environment variable names as a reference, read once; null when neither does.
    /// The option wins. A variable that holds no reference is left to the host, which reads it as the setting's own text.
    /// </summary>
    private static async Task<(string? Secret, string? Refusal)> ReadReferenceAsync(
        string? option, string setting, string what, ISecretsManager secrets, CancellationToken ct)
    {
        var reference = option ?? (Environment.GetEnvironmentVariable($"{EnvironmentPrefix}__{setting}") is { } fromEnvironment
            && NodeBricks.IsSecretReference(fromEnvironment) ? fromEnvironment : null);
        return reference is null ? (null, null) : await NodeBricks.ReadSecretAsync(reference, what, secrets, ct);
    }

    // A token that names a node setting: only the environment and the options above set one.
    private static bool IsSettingToken(string token) =>
        token.Contains($"{EnvironmentPrefix}:", StringComparison.OrdinalIgnoreCase)
        || token.Contains($"{EnvironmentPrefix}__", StringComparison.OrdinalIgnoreCase);
}
