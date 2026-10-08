using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The JSON the coordinator writes for the records it shares with <c>dtpipe studio</c>, as the server's
/// own options write it: a deployed studio reads these bytes, so a change here is a new contract. The
/// pages read <c>/api/me</c> too, and its JSON is the one they always read.
/// </summary>
public sealed class CoordinatorContractTests
{
    // What Program.cs configures: ASP.NET Core's web defaults and enumerations as text.
    private static readonly JsonSerializerOptions Server = new(new Microsoft.AspNetCore.Http.Json.JsonOptions().SerializerOptions)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private static string Write<T>(T value) => JsonSerializer.Serialize(value, Server);

    [Fact]
    public void Me_Keeps_The_Json_The_Pages_Read()
    {
        Assert.Equal("""{"name":"ann","email":"ann@x.test","roles":["designer","reader"]}""",
            Write(new CoordinatorMe("ann", "ann@x.test", ["designer", "reader"])));
    }

    [Fact]
    public void Metadata_Uses_The_Rfc_Names_And_Omits_An_Absent_Client()
    {
        Assert.Equal(
            """{"resource":"http://c.test","authorization_servers":["http://idp.test/"],"bearer_methods_supported":["header"],"dtpipe_contract":2,"dtpipe_studio_client_id":"studio"}""",
            Write(new ResourceMetadata("http://c.test", ["http://idp.test/"], ["header"], CoordinatorApi.Contract, "studio")));
        Assert.DoesNotContain("dtpipe_studio_client_id", Write(new ResourceMetadata("http://c.test", ["http://idp.test/"], ["header"], 1)));
    }

    [Fact]
    public void Catalog_Recognition_And_Library_Records_Keep_Their_Json()
    {
        var branch = (JsonObject)JsonNode.Parse("""{"input":"duck:x.db"}""")!;
        Assert.Equal(
            """{"nodes":[{"name":"n1","online":true,"role":"data","dtpipeVersion":"1.0.0"}],"bricks":[{"key":"n1/b","node":"n1","id":"b","kind":"source","title":"B","description":"","branch":{"input":"duck:x.db"},"version":"0123456789ab","schema":[{"name":"id","type":"int","isNullable":false}],"schemaError":null,"online":true}]}""",
            Write(new BrickCatalogView([new CatalogNode("n1", true, "data", "1.0.0")],
                [new CatalogBrick("n1/b", "n1", "b", "source", "B", "", branch, "0123456789ab", [new CatalogColumn("id", "int", false)], null, true)])));
        Assert.Equal("""{"job":"a:\n  input: x\n"}""", Write(new RecognizeRequest("a:\n  input: x\n")));
        Assert.Equal("""{"branches":{"a":{"match":"n1/b","source":null,"sink":null}}}""",
            Write(new RecognizeResponse(new Dictionary<string, Recognition> { ["a"] = new("n1/b", null, null) })));
        Assert.Equal("""{"id":"p","commit":"abc","redistribute":true,"deployed":false}""", Write(new LibrarySaved("p", "abc", true, false)));
        Assert.Equal("""{"error":"e","commit":null}""", Write(new LibraryConflict("e", null)));
        Assert.Equal("""{"error":"e","report":"r"}""", Write(new LibraryRefused("e", "r")));
        Assert.Equal("""{"id":"p","title":"T","description":null,"commit":"abc"}""", Write(new LibraryItem("p", "T", null, "abc")));
        Assert.Equal("""{"id":"p","yaml":"a: {}","commit":"abc"}""", Write(new LibraryJob("p", "a: {}", "abc")));
        var save = JsonSerializer.Deserialize<LibrarySave>("""{"yaml":"a: {}","layout":null,"message":"m"}""", Server)!;
        Assert.Equal(("a: {}", "m"), (save.Yaml, save.Message));
    }

    [Theory]
    [InlineData("orders", true)]
    [InlineData("a-1", true)]
    [InlineData("-a", false)]
    [InlineData("Orders", false)]
    [InlineData("orders\n", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void A_Pipeline_Id_Is_Lowercase_Letters_Digits_And_Dashes(string? id, bool valid) =>
        Assert.Equal(valid, CoordinatorApi.IsPipelineId(id));

    [Fact]
    public void Routes_Escape_What_They_Carry()
    {
        Assert.Equal("/api/bricks/n%2F1/a%20b/sample", CoordinatorApi.Sample("n/1", "a b"));
        Assert.Equal("/api/library/orders", CoordinatorApi.Library("orders"));
        Assert.Equal("/api/library", CoordinatorApi.LibraryList);
    }
}
