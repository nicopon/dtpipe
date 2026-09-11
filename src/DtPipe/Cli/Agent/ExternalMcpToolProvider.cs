using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Offers the tools of an MCP server dtpipe did not write, over stdio.
///
/// <para>
/// The point is measurement: dtpipe's loop already records the role prompt and the catalogue as the
/// model was offered it (<see cref="AgentTrace"/>), so pointing it at another server turns that
/// instrumentation on a surface nobody here designed. What a catalogue's shape costs a weak model —
/// fifteen flat tools against a handful of parents behind an action discriminant — is a question
/// about someone else's catalogue, and it cannot be answered by reading their documentation.
/// </para>
///
/// <para>
/// <b>Nothing here can execute a dtpipe pipeline.</b> The tools come from the child process and go
/// back to it, and <see cref="CanRunDtPipePlans"/> is false, so the plan path refuses rather than
/// relying on no foreign tool happening to be called <c>execute-yaml-job</c>. F1 (the planner /
/// executor split) and F6 (the single YAML path) are therefore not in play — there is no plan to
/// extract and no engine step to hand it to — which is what makes the flag safe to add without
/// reopening the hardening. What the child process does when a tool is called is the child's
/// business and its own guardrails': dtpipe vouches for none of it.
/// </para>
///
/// <para>
/// <see cref="AgentMode"/> is deliberately ignored by <see cref="GetToolDefinitions(AgentMode)"/>.
/// Plan mode hides a tool by name, and that name belongs to dtpipe's catalogue; applying it to a
/// foreign one would either do nothing or hide an unrelated tool that happens to share the spelling.
/// A mode filter that is right by accident is worse than none, so the whole catalogue is offered
/// and the caller is responsible for not asking the model to act on it.
/// </para>
/// </summary>
public sealed class ExternalMcpToolProvider : IAgentToolProvider, IAsyncDisposable
{
    private readonly McpClient _client;
    private readonly List<ToolDefinition> _definitions;

    private ExternalMcpToolProvider(McpClient client, List<ToolDefinition> definitions)
    {
        _client = client;
        _definitions = definitions;
    }

    /// <summary>The server's own name and version, as it introduced itself.</summary>
    public string ServerName { get; private init; } = "unknown";

    /// <summary>
    /// What the server told the client about itself at the handshake. Recorded beside the catalogue
    /// because hosts feed it to the model as a system message: a tool call cannot be read against
    /// the wrong frame.
    /// </summary>
    public string? ServerInstructions { get; private init; }

    /// <summary>
    /// Starts <paramref name="command"/> as a child process and completes the MCP handshake.
    /// The command is split on spaces, which covers <c>"sometool mcp"</c> and
    /// <c>"npx -y some-server"</c>; a path containing a space must be quoted by the shell that
    /// built the argument, not guessed at here.
    /// </summary>
    public static async Task<ExternalMcpToolProvider> ConnectAsync(
        string command, string? workingDirectory = null, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(command))
            throw new ArgumentException("An MCP server command is required.", nameof(command));

        var parts = command.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var transport = new StdioClientTransport(new StdioClientTransportOptions
        {
            Name = parts[0],
            Command = parts[0],
            Arguments = parts.Skip(1).ToArray(),
            WorkingDirectory = workingDirectory,
        });

        var client = await McpClient.CreateAsync(transport, cancellationToken: ct).ConfigureAwait(false);

        var tools = await client.ListToolsAsync(cancellationToken: ct).ConfigureAwait(false);
        var definitions = tools.Select(t => new ToolDefinition(
            t.Name,
            t.Description ?? string.Empty,
            SchemaOf(t))).ToList();

        return new ExternalMcpToolProvider(client, definitions)
        {
            ServerName = client.ServerInfo is { } info ? $"{info.Name} {info.Version}".Trim() : parts[0],
            ServerInstructions = client.ServerInstructions,
        };
    }

    /// <summary>
    /// A server is free to omit its input schema. The loop sends whatever is here straight to the
    /// model, and a missing "properties" makes some providers reject the whole request, so an empty
    /// object stands in rather than a null that fails one call later with an unrelated message.
    /// </summary>
    private static JsonElement SchemaOf(McpClientTool tool)
    {
        var schema = tool.JsonSchema;
        if (schema.ValueKind == JsonValueKind.Object) return schema.Clone();

        using var empty = JsonDocument.Parse("""{"type":"object","properties":{}}""");
        return empty.RootElement.Clone();
    }

    /// <inheritdoc />
    public bool CanRunDtPipePlans => false;

    public List<ToolDefinition> GetToolDefinitions() => _definitions;

    /// <inheritdoc />
    /// <remarks>The mode is ignored; see the type-level remarks for why.</remarks>
    public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => _definitions;

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
    {
        try
        {
            var result = await _client
                .CallToolAsync(toolName, ToArguments(args), cancellationToken: ct)
                .ConfigureAwait(false);

            var text = string.Join("\n", result.Content
                .OfType<TextContentBlock>()
                .Select(b => b.Text));

            return new ToolResult(text.Length == 0 ? "{}" : text, result.IsError ?? false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The child is a process that can die mid-call. Reporting that as a failed tool call
            // keeps the turn readable and the trace attributable; letting it escape would end the
            // session on an exception the transcript never explains.
            return ToolResult.Error(JsonSerializer.Serialize(new
            {
                error = $"The MCP server failed to answer '{toolName}': {ex.Message}",
            }));
        }
    }

    /// <summary>
    /// The loop carries tool arguments as a JSON object; the client wants a dictionary. Values stay
    /// as <see cref="JsonElement"/> so a nested object or array survives the crossing untouched —
    /// flattening them to strings is how a YAML payload arrives quoted at the far end.
    /// </summary>
    private static IReadOnlyDictionary<string, object?> ToArguments(JsonElement args)
    {
        if (args.ValueKind != JsonValueKind.Object)
            return new Dictionary<string, object?>();

        return args.EnumerateObject().ToDictionary(
            p => p.Name,
            p => (object?)p.Value.Clone());
    }

    public async ValueTask DisposeAsync()
    {
        try { await _client.DisposeAsync().ConfigureAwait(false); }
        catch (Exception) { /* the child is already gone */ }
    }
}
