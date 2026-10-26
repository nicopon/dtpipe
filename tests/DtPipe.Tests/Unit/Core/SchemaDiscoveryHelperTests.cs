using AwesomeAssertions;
using DtPipe.Core.Infrastructure.Discovery;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class SchemaDiscoveryHelperTests
{
    private static Dictionary<string, object?> Obj(params (string Key, object? Value)[] members) =>
        members.ToDictionary(m => m.Key, m => m.Value);

    private static IEnumerable<string> KeysOf(object? value) => ((Dictionary<string, object?>)value!).Keys;

    [Fact]
    public void Merging_Writes_Nothing_Into_The_Records_And_Counts_Every_List_Item()
    {
        var first = Obj(("payload", Obj(("a", 1L))), ("steps", new List<object?> { Obj(("n", 1L)) }));
        var second = Obj(("payload", Obj(("b", 2L))), ("steps", new List<object?> { Obj(("ok", true)), Obj(("z", 0L)) }));

        var schema = new Dictionary<string, object?>();
        SchemaDiscoveryHelper.DeepMergeSchemas(schema, first);
        SchemaDiscoveryHelper.DeepMergeSchemas(schema, second);

        KeysOf(schema["payload"]).Should().BeEquivalentTo("a", "b");
        KeysOf(((List<object?>)schema["steps"]!)[0]).Should().BeEquivalentTo("n", "ok", "z");

        // The records are what a reader replays after discovery: they keep their own keys.
        KeysOf(first["payload"]).Should().BeEquivalentTo("a");
        KeysOf(second["payload"]).Should().BeEquivalentTo("b");
        KeysOf(((List<object?>)first["steps"]!)[0]).Should().BeEquivalentTo("n");
        KeysOf(((List<object?>)second["steps"]!)[0]).Should().BeEquivalentTo("ok");
    }
}
