using System;
using System.Collections.Generic;
using System.Text.Json;

namespace DtPipe.Cli.Agent;

// Message in the conversation (role: system, user, assistant, tool)
public record ChatMessage(
    string Role,
    string? Content,
    string? Name = null, // tool name (if Role == "tool")
    List<ToolCall>? ToolCalls = null,
    string? ToolCallId = null // tool call ID (required if Role == "tool" for OpenAI)
);

// Tool call in the LLM response
public record ToolCall(string Id, string Name, JsonElement Arguments);

// Tool definition for the LLM
public record ToolDefinition(string Name, string Description, JsonElement ParametersSchema);

/// <summary>
/// Provider-reported token counts and timing for one generation. Every field is best-effort:
/// a provider that does not report a value leaves it null / 0.
/// </summary>
public record LlmUsage(
    int PromptTokens = 0,
    int CompletionTokens = 0,
    TimeSpan? PromptEvalTime = null,
    TimeSpan? GenerationTime = null)
{
    /// <summary>Generation throughput, or null when it cannot be computed.</summary>
    public double? TokensPerSecond =>
        GenerationTime is { TotalSeconds: > 0 } g && CompletionTokens > 0
            ? CompletionTokens / g.TotalSeconds
            : null;
}

// LLM response
public record LlmResponse(
    ChatMessage Message,
    bool Done,
    string? Error,
    LlmUsage? Usage = null,
    // Model reasoning kept OUT of Message.Content: it is never sent back in the conversation, only
    // shown to the user and stored on the trajectory step.
    string? Thinking = null
);
