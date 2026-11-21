using DtPipe.Coordinator.Contracts;
using TransportR.Client;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The one table from the category TransportR gives a failed transfer to the word an exit reports it by (<see cref="TransferFailures"/>).
/// What a run reports of its edges' failures is read through it, so the coordinator is told a word and never a message.
/// </summary>
internal static class TransferFailureWords
{
    private static readonly IReadOnlyDictionary<TransferFailureCategory, string> Words = new Dictionary<TransferFailureCategory, string>
    {
        [TransferFailureCategory.EndedByHub] = TransferFailures.EndedByHub,
        [TransferFailureCategory.AbandonedByPeer] = TransferFailures.AbandonedByPeer,
        [TransferFailureCategory.AcknowledgementTimedOut] = TransferFailures.AcknowledgementTimedOut,
        [TransferFailureCategory.HandleReleased] = TransferFailures.HandleReleased,
        [TransferFailureCategory.UndecodableOrUnserializableBatch] = TransferFailures.UndecodableOrUnserializableBatch,
        [TransferFailureCategory.ProtocolViolation] = TransferFailures.ProtocolViolation,
        [TransferFailureCategory.Other] = TransferFailures.Other,
        [TransferFailureCategory.EndToEndEncryptionFailed] = TransferFailures.EndToEndEncryptionFailed,
    };

    /// <summary>The word for <paramref name="category"/>, or null when this table does not know it (a value a later TransportR adds).</summary>
    internal static string? WordOf(TransferFailureCategory category) => Words.GetValueOrDefault(category);

    /// <summary>
    /// What an exit reports of its edges' failures: the word of each category, by alias, with no entry for a value the table does not know;
    /// null when no edge has one.
    /// </summary>
    internal static IReadOnlyDictionary<string, string>? WordsOf(IReadOnlyDictionary<string, TransferFailureCategory> failed)
    {
        var words = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (alias, category) in failed)
            if (WordOf(category) is { } word) words[alias] = word;
        return words.Count == 0 ? null : words;
    }
}
