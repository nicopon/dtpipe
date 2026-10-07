using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Text;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The bytes a brick is matched and versioned on. A brick's version hashes its canonical form, and a
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
        Assert.Equal("235ddc4e15bc", Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(form.Canonical)))[..12]);
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
