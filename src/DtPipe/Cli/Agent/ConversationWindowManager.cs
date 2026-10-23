using System;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Cli.Agent;

public class ConversationWindowManager
{
    private readonly int _maxMessages;
    private readonly int _keepSystemMessages;
    private readonly int _keepRecentMessages;

    public ConversationWindowManager(
        int maxMessages = 40,
        int keepSystemMessages = 1,
        int keepRecentMessages = 10)
    {
        _maxMessages = maxMessages;
        _keepSystemMessages = keepSystemMessages;
        _keepRecentMessages = keepRecentMessages;
    }

    /// <summary>
    /// If the message count exceeds maxMessages, compacts the middle of the conversation
    /// while keeping system + recent messages. When a non-null <paramref name="contextStore"/>
    /// is supplied, a structured FACTS block (inspected schemas, sample rows, recent errors)
    /// is emitted instead of a lossy one-line summary, so no facts are discarded (F4).
    /// The full journal is always preserved separately in <see cref="AgentTrajectory"/>.
    /// </summary>
    public List<ChatMessage> Compact(List<ChatMessage> messages, AgentContextStore? contextStore = null)
    {
        if (messages.Count <= _maxMessages) return messages;

        var systemMessages = messages.Take(_keepSystemMessages).ToList();
        var recentMessages = messages.TakeLast(_keepRecentMessages).ToList();
        var middleMessages = messages
             .Skip(_keepSystemMessages)
             .Take(messages.Count - _keepSystemMessages - _keepRecentMessages)
             .ToList();

        var result = new List<ChatMessage>();
        result.AddRange(systemMessages);

        string factsBlock = contextStore?.BuildFactsBlock() ?? string.Empty;
        if (!string.IsNullOrEmpty(factsBlock))
        {
            // Non-destructive: preserve all cached facts (schemas, samples, errors).
            result.Add(new ChatMessage("assistant", factsBlock));
        }
        else
        {
            // No context store available: fall back to a lossy summary of the compacted tools.
            var toolCalls = middleMessages
                 .Where(m => m.ToolCalls != null)
                 .SelectMany(m => m.ToolCalls!)
                 .Select(tc => tc.Name)
                 .ToList();

            var summary = $"[Context compacted: {middleMessages.Count} messages summarized. " +
                         $"Tools called: {string.Join(", ", toolCalls.Distinct())}. " +
                         $"Keeping {_keepRecentMessages} most recent messages.]";

            result.Add(new ChatMessage("assistant", summary));
        }

        result.AddRange(recentMessages);
        return result;
    }
}
