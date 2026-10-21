using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Text;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Tests.Shared;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The bytes a brick is matched and versioned on. A brick's version is the keyed hash of its canonical form, and a
/// node hashes the job it composes from its declarations: a change to either text, however tidy,
/// gives every brick a new version and stops the library's pipelines matching the bricks they use.
/// </summary>
public sealed class BrickFormTests
{
    private const string Declaration = """
        {"input": "pg:Host=db;Database=shop", "provider-options": {"pg": {"query": "SELECT id,\n  name\nFROM customers", "fetch-size": 500}},
         "limit": 10}
        """;

    [Fact]
    public void A_Declared_Brick_Keeps_Its_Canonical_Form_And_Version()
    {
        var form = BrickRules.Declared(BrickKind.Source, Declaration);

        Assert.Equal(
            """{"input":"pg:Host=db;Database=shop","limit":"10","provider-options":{"pg":{"fetch-size":"500","query":"SELECT id,\n  name\nFROM customers"}}}""",
            form.Canonical);
        Assert.Equal("a778983c7c3d", BrickRules.VersionOf(BrickKind.Source, Declaration, TestBricks.Key));
    }

    [Fact]
    public void A_Bricks_Version_Depends_On_The_Key_And_Reveals_Nothing_Of_The_Plain_Hash()
    {
        var plain = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(BrickRules.Declared(BrickKind.Source, Declaration).Canonical)))[..12];

        Assert.NotEqual("a778983c7c3d", BrickRules.VersionOf(BrickKind.Source, Declaration, TestBricks.Key + "x"));
        Assert.NotEqual(plain, BrickRules.VersionOf(BrickKind.Source, Declaration, TestBricks.Key));
    }

    [Fact]
    public void A_Declared_Brick_Keeps_Its_Yaml()
    {
        var yaml = JobYaml.Serialize((YamlDotNet.RepresentationModel.YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(Declaration)));

        Assert.Equal(
            "input: pg:Host=db;Database=shop\nprovider-options:\n  pg:\n    query: |-\n      SELECT id,\n        name\n      FROM customers\n"
            + "    fetch-size: 500\nlimit: 10\n",
            yaml);
    }
}
