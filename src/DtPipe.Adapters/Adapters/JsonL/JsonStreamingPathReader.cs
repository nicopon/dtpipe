using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;

namespace DtPipe.Adapters.JsonL;

/// <summary>
/// Streams JSON array elements from a dot-path property within a large JSON file using
/// a forward-only Utf8JsonReader with bounded memory.
/// Memory usage is O(buffer size + one element) regardless of total file size.
/// </summary>
internal static class JsonStreamingPathReader
{
    private const int InitialBufferSize = 131072; // 128 KB

    /// <summary>
    /// Opens <paramref name="stream"/> and yields each <see cref="JsonElement"/> found in
    /// the array at the given <paramref name="pathParts"/> (dot-path split by the caller).
    /// The returned elements are cloned — callers do not need to keep the stream alive after iteration.
    /// </summary>
    public static async IAsyncEnumerable<JsonElement> StreamArrayAsync(
        Stream stream,
        string[] pathParts,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (pathParts.Length == 0) yield break;

        // A bounded channel decouples the Utf8JsonReader ref-struct loop
        // (which cannot cross async yield points) from the async enumerable consumer.
        var channel = Channel.CreateBounded<JsonElement>(new BoundedChannelOptions(16)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        var producer = Task.Run(() => ProduceAsync(stream, pathParts, channel.Writer, ct), ct);

        await foreach (var element in channel.Reader.ReadAllAsync(ct))
        {
            yield return element;
        }

        await producer; // propagate any producer exception
    }

    private static async Task ProduceAsync(
        Stream stream,
        string[] pathParts,
        ChannelWriter<JsonElement> writer,
        CancellationToken ct)
    {
        try
        {
            byte[] buffer = new byte[InitialBufferSize];
            int dataLen = 0;
            bool streamEnded = false;

            // Append data from stream; grows the buffer when space is low.
            async Task<bool> RefillAsync()
            {
                if (streamEnded) return false;
                if (buffer.Length - dataLen < 4096)
                    Array.Resize(ref buffer, Math.Max(buffer.Length * 2, dataLen + 65536));
                int read = await stream.ReadAsync(buffer.AsMemory(dataLen, buffer.Length - dataLen), ct);
                if (read == 0) { streamEnded = true; return dataLen > 0; }
                dataLen += read;
                return true;
            }

            // Discard consumed bytes by moving remaining data to the front of the buffer.
            void Compact(int consumed)
            {
                if (consumed <= 0) return;
                int remaining = dataLen - consumed;
                if (remaining > 0) Buffer.BlockCopy(buffer, consumed, buffer, 0, remaining);
                dataLen = Math.Max(0, remaining);
            }

            // ── Phase 1: navigate forward to the target array ─────────────────

            int depth = 0;
            string?[] entryPropAtDepth = new string?[64]; // property that "opened" each depth level
            string? pendingProp = null;
            bool foundArray = false;
            JsonReaderState navState = default;

            while (!foundArray)
            {
                if (dataLen == 0 && !await RefillAsync()) break;

                var navReader = new Utf8JsonReader(buffer.AsSpan(0, dataLen), streamEnded, navState);
                bool advanced = false;

                while (navReader.Read())
                {
                    advanced = true;
                    switch (navReader.TokenType)
                    {
                        case JsonTokenType.PropertyName:
                            pendingProp = navReader.GetString();
                            break;

                        case JsonTokenType.StartObject:
                            if (depth < entryPropAtDepth.Length) entryPropAtDepth[depth] = pendingProp;
                            depth++;
                            pendingProp = null;
                            break;

                        case JsonTokenType.StartArray:
                            if (IsPathMatch(pathParts, depth, entryPropAtDepth, pendingProp))
                            {
                                // The '[' was just consumed — save state immediately after it.
                                foundArray = true;
                            }
                            else
                            {
                                if (depth < entryPropAtDepth.Length) entryPropAtDepth[depth] = pendingProp;
                                depth++;
                            }
                            pendingProp = null;
                            break;

                        case JsonTokenType.EndObject:
                        case JsonTokenType.EndArray:
                            if (depth > 0) { depth--; if (depth < entryPropAtDepth.Length) entryPropAtDepth[depth] = null; }
                            pendingProp = null;
                            break;

                        default:
                            pendingProp = null;
                            break;
                    }
                    if (foundArray) break;
                }

                navState = navReader.CurrentState;
                Compact((int)navReader.BytesConsumed);

                if (!advanced && !streamEnded) await RefillAsync();
            }

            if (!foundArray) return;

            // ── Phase 2: enumerate each array element ─────────────────────────
            //
            // Strategy: use a Utf8JsonReader to locate each element's start (via TokenStartIndex)
            // and end (by depth-tracking all inner tokens). Capture the raw bytes and parse them
            // with JsonDocument.Parse — this avoids the JsonDocument.TryParseValue limitation
            // of not respecting array context from JsonReaderState.

            var elemState = navState;

            while (true)
            {
                if (dataLen == 0 && !await RefillAsync()) break;

                bool isFinal = streamEnded;
                var reader = new Utf8JsonReader(buffer.AsSpan(0, dataLen), isFinal, elemState);

                // Advance to the first token of the next element (or EndArray).
                if (!reader.Read())
                {
                    if (streamEnded) break;
                    await RefillAsync();
                    continue;
                }

                if (reader.TokenType == JsonTokenType.EndArray) break;

                // Record where the element starts in the buffer.
                int startIdx = (int)reader.TokenStartIndex;

                // Track nesting to find the element end.
                int nestDepth = reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray ? 1 : 0;
                bool complete = nestDepth == 0; // scalars are complete after the single Read() above

                while (!complete)
                {
                    if (!reader.Read()) { break; }
                    switch (reader.TokenType)
                    {
                        case JsonTokenType.StartObject or JsonTokenType.StartArray:
                            nestDepth++;
                            break;
                        case JsonTokenType.EndObject or JsonTokenType.EndArray:
                            if (--nestDepth == 0) complete = true;
                            break;
                    }
                }

                if (!complete)
                {
                    // Element spans beyond the current buffer — load more data and retry from elemState.
                    if (streamEnded) break;
                    await RefillAsync();
                    // elemState is unchanged: next iteration re-reads from element start.
                    continue;
                }

                int endIdx = (int)reader.BytesConsumed;
                var nextElemState = reader.CurrentState;

                // Parse the captured element bytes as an independent JsonDocument.
                // ToArray() materialises the span so JsonDocument owns its own backing store.
                var elementBytes = buffer.AsSpan(startIdx, endIdx - startIdx).ToArray();
                JsonElement element;
                using (var doc = JsonDocument.Parse(elementBytes))
                {
                    element = doc.RootElement.Clone();
                }

                Compact(endIdx);
                elemState = nextElemState;

                await writer.WriteAsync(element, ct);
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    /// <summary>
    /// Returns true when the current reader position (just before StartArray) matches the target path.
    /// </summary>
    private static bool IsPathMatch(string[] pathParts, int currentDepth, string?[] entryPropAtDepth, string? pendingProp)
    {
        if (pendingProp != pathParts[pathParts.Length - 1]) return false;
        if (currentDepth != pathParts.Length) return false;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            if (i + 1 >= entryPropAtDepth.Length || entryPropAtDepth[i + 1] != pathParts[i]) return false;
        }
        return true;
    }
}
