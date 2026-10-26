using System.Text.Json;
using System.Text.Json.Nodes;

namespace DtPipe.Tests.Helpers;

/// <summary>
/// JSONL events whose <c>payload</c> object has a different shape per kind of event, the first record
/// having the smallest one, and the comparison that tells whether a destination holds them key for key.
/// A key written <c>null</c> counts as absent: a struct column has no way to tell them apart.
/// </summary>
internal static class JsonLShapeData
{
    public static IEnumerable<string> Events(int count) =>
        Enumerable.Range(0, count).Select(i => new JsonObject { ["id"] = i, ["payload"] = Payload(i) }.ToJsonString());

    private static JsonObject Payload(int i)
    {
        var payload = new JsonObject { ["severity"] = i % 3 == 0 ? "info" : "warning" };
        if (i % 7 == 3) payload["error_code"] = $"E{i % 5}";
        if (i % 11 == 5) { payload["from_version"] = "1.0.0"; payload["to_version"] = "1.1.0"; }
        if (i % 13 == 1)
        {
            payload["detail"] = i % 2 == 1
                ? new JsonObject { ["code"] = i, ["cause"] = "battery" }
                : new JsonObject { ["code"] = i };
        }
        if (i % 17 == 4)
            payload["steps"] = new JsonArray(new JsonObject { ["n"] = 1 }, new JsonObject { ["n"] = 2, ["ok"] = true });
        return payload;
    }

    /// <summary>The ids whose payload differs between the source and a destination, with both texts.</summary>
    public static List<string> Differences(IEnumerable<string> sourceLines, IReadOnlyDictionary<long, string> payloadsById)
    {
        var differences = new List<string>();
        foreach (var line in sourceLines)
        {
            var record = JsonNode.Parse(line)!.AsObject();
            var id = record["id"]!.GetValue<long>();
            var expected = Canonical(record["payload"]);
            var actual = payloadsById.TryGetValue(id, out var text) ? Canonical(StripNulls(JsonNode.Parse(text))) : "<row missing>";
            if (expected != actual) differences.Add($"id {id}: expected {expected} but found {actual}");
        }
        return differences;
    }

    public static string Canonical(JsonNode? node) => node switch
    {
        JsonObject o => "{" + string.Join(",", o.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => $"\"{p.Key}\":{Canonical(p.Value)}")) + "}",
        JsonArray a => "[" + string.Join(",", a.Select(Canonical)) + "]",
        null => "null",
        _ => node.ToJsonString()
    };

    public static JsonNode? StripNulls(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject o:
                var kept = new JsonObject();
                foreach (var p in o.Where(p => p.Value is not null)) kept[p.Key] = StripNulls(p.Value?.DeepClone());
                return kept;
            case JsonArray a:
                return new JsonArray(a.Select(e => StripNulls(e?.DeepClone())).ToArray());
            default:
                return node?.DeepClone();
        }
    }

    public static Dictionary<long, string> PayloadsOfJsonl(string path) =>
        File.ReadLines(path).Select(l => JsonNode.Parse(l)!.AsObject())
            .ToDictionary(r => r["id"]!.GetValue<long>(), r => r["payload"]?.ToJsonString() ?? "null");
}
