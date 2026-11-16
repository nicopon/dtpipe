using System.Security.Cryptography;
using System.Text;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Tests.Shared;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The bytes a brick is matched and versioned on. A brick's version is the hash of its canonical form, and a
/// node hashes the job it composes from its declarations: a change to either text, however tidy,
/// gives every brick a new version and stops the library's pipelines matching the bricks they use.
/// </summary>
public sealed class BrickFormTests
{
    private const string Declaration = """
        {"input": "pg:Host=db;Database=shop", "provider-options": {"pg": {"query": "SELECT id,\n  name\nFROM customers", "fetch-size": 500}},
         "limit": 10}
        """;

    // The same brick as YAML: JSON is YAML, so both texts are read by one reader and give one tree.
    private const string YamlDeclaration =
        "input: pg:Host=db;Database=shop\nprovider-options:\n  pg:\n    query: |-\n      SELECT id,\n        name\n      FROM customers\n"
        + "    fetch-size: 500\nlimit: 10\n";

    [Fact]
    public void A_Declared_Brick_Keeps_Its_Canonical_Form_And_Version()
    {
        var form = BrickRules.Declared(BrickKind.Source, JobYaml.ParseBranch(Declaration));

        Assert.Equal(
            """{"input":"pg:Host=db;Database=shop","limit":"10","provider-options":{"pg":{"fetch-size":"500","query":"SELECT id,\n  name\nFROM customers"}}}""",
            form.Canonical);
        Assert.Equal("235ddc4e15bc", BrickRules.VersionOf(BrickKind.Source, JobYaml.ParseBranch(Declaration)));
    }

    [Fact]
    public void A_Brick_Written_As_Yaml_Or_As_Json_Has_One_Version()
    {
        Assert.Equal(BrickRules.Declared(BrickKind.Source, JobYaml.ParseBranch(Declaration)),
                     BrickRules.Declared(BrickKind.Source, JobYaml.ParseBranch(YamlDeclaration)));
        Assert.Equal("235ddc4e15bc", BrickRules.VersionOf(BrickKind.Source, JobYaml.ParseBranch(YamlDeclaration)));
    }

    [Fact]
    public void A_Scalar_Is_Matched_By_The_Text_Dtpipe_Reads()
    {
        // dtpipe reads a job's scalars as their text, quoted or not: "~" and ~ are one value, null is the text null.
        static string Canonical(string branch) => BrickRules.Declared(BrickKind.Source, JobYaml.ParseBranch(branch)).Canonical;

        Assert.Equal(Canonical("input: x\nnote: ~\n"), Canonical("""{"input": "x", "note": "~"}"""));
        Assert.Equal(Canonical("input: x\nnote: 'null'\n"), Canonical("""{"input": "x", "note": null}"""));
        Assert.NotEqual(Canonical("input: x\nnote: ~\n"), Canonical("""{"input": "x", "note": null}"""));
    }

    [Theory]
    [InlineData("[1, 2]")]
    [InlineData("just text")]
    [InlineData("input: a\n---\ninput: b\n")]
    [InlineData("input: a\n? [k]\n: v\n")]
    public void A_Declaration_That_Is_Not_One_Mapping_Of_Scalar_Keys_Is_Refused(string text)
    {
        Assert.Throws<FormatException>(() => JobYaml.ParseBranch(text));
    }

    [Fact]
    public void A_Bricks_Version_Is_The_Plain_Hash_Of_Its_Canonical_Form()
    {
        var branch = JobYaml.ParseBranch(Declaration);
        var plain = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(BrickRules.Declared(BrickKind.Source, branch).Canonical)))[..12];

        Assert.Equal(plain, BrickRules.VersionOf(BrickKind.Source, branch));
    }

    [Fact]
    public void A_Declared_Brick_Keeps_Its_Yaml()
    {
        // The node writes the job a brick composes to as a job file, whether its owner wrote YAML or JSON.
        Assert.Equal(YamlDeclaration, JobYaml.Serialize(JobYaml.ParseBranch(YamlDeclaration)));
        Assert.Equal(YamlDeclaration, JobYaml.Serialize(JobYaml.ParseBranch(Declaration)));
    }
}
