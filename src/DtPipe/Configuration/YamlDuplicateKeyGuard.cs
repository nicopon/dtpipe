using YamlDotNet.Core;
using YamlDotNet.Core.Events;

namespace DtPipe.Configuration;

/// <summary>
/// Refuses a YAML mapping that holds the same key twice. YamlDotNet fills a dictionary by assignment,
/// so the second occurrence replaced the first without a word: a branch with two
/// <c>provider-options:</c> blocks kept only the last, and a job written for a processor ran without
/// it and exited 0. The refusal names the key and both lines, at every level of the job.
/// </summary>
internal static class YamlDuplicateKeyGuard
{
    /// <summary>The YAML merge key: it may legitimately stand beside the keys it completes.</summary>
    private const string MergeKey = "<<";

    /// <exception cref="YamlException">A key appears twice in one mapping; the position is the second one's.</exception>
    public static void Check(string content)
    {
        var parser = new Parser(new StringReader(content));
        var frames = new Stack<Frame>();

        while (parser.MoveNext())
        {
            switch (parser.Current)
            {
                case Scalar scalar:
                    OnNode(frames, scalar);
                    break;
                case MappingStart start:
                    OnNode(frames, start);
                    frames.Push(new Frame(new Dictionary<string, Mark>(StringComparer.Ordinal)));
                    break;
                case SequenceStart start:
                    OnNode(frames, start);
                    frames.Push(new Frame(null));
                    break;
                case AnchorAlias alias:
                    OnNode(frames, alias);
                    break;
                case MappingEnd or SequenceEnd:
                    frames.Pop();
                    break;
            }
        }
    }

    /// <summary>A node starts inside the enclosing frame: in a mapping it is a key, then a value, alternately.</summary>
    private static void OnNode(Stack<Frame> frames, ParsingEvent node)
    {
        if (!frames.TryPeek(out var frame) || frame.Keys is null) return;

        var isKey = frame.ExpectKey;
        frame.ExpectKey = !isKey;
        if (!isKey || node is not Scalar key || key.Value == MergeKey) return;

        if (frame.Keys.TryGetValue(key.Value, out var first))
            throw new YamlException(key.Start, key.End,
                $"The key '{key.Value}' appears twice in the same mapping (first at line {first.Line}); " +
                "the second would silently replace the first. Merge them into one.");

        frame.Keys[key.Value] = key.Start;
    }

    private sealed class Frame(Dictionary<string, Mark>? keys)
    {
        /// <summary>The keys seen so far; null for a sequence, which has none.</summary>
        public Dictionary<string, Mark>? Keys { get; } = keys;

        public bool ExpectKey { get; set; } = true;
    }
}
