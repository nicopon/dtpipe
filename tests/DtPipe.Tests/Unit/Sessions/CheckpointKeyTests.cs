using DtPipe.Sessions;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

/// <summary>
/// Content addressing is what makes an implicit, working-directory session safe: two pipelines
/// launched in the same place cannot collide, by construction rather than by the user naming
/// them apart.
/// </summary>
public class CheckpointKeyTests
{
    private static string Key(
        string? component = "pg",
        string? conn = "pg:Host=db;Database=app",
        string? query = "SELECT * FROM orders",
        IEnumerable<string>? transformers = null,
        string? definition = "def",
        double rate = 1.0,
        int? seed = 42,
        int limit = 0,
        int batchSize = 32768,
        long maxBytes = 0,
        int segment = 0)
        => CheckpointKey.Compute(component, conn, query, transformers ?? ["mask:email"], definition, rate, seed, limit, batchSize, maxBytes, segment);

    [Fact]
    public void The_Same_Definition_Gives_The_Same_Key()
        => Key().Should().Be(Key(), "an unchanged prefix is reused for free — that is what makes iteration fast");

    [Theory]
    [MemberData(nameof(Variations))]
    public void Any_Change_To_The_Definition_Changes_The_Key(string what, string other)
        => other.Should().NotBe(Key(), $"{what} is part of what produces the rows");

    public static TheoryData<string, string> Variations() => new()
    {
        { "the query",           Key(query: "SELECT * FROM customers") },
        { "the component",       Key(component: "mysql") },
        { "the definition",      Key(definition: "other") },
        { "the connection",      Key(conn: "pg:Host=other;Database=app") },
        { "a transformer",       Key(transformers: ["mask:phone"]) },
        { "transformer order",   Key(transformers: ["b", "a"]) },
        { "the sampling seed",   Key(seed: 43) },
        { "the sampling rate",   Key(rate: 0.5) },
        { "the limit",           Key(limit: 100) },
        { "the segment",         Key(segment: 1) },
    };

    [Fact]
    public void A_Password_Never_Enters_The_Key()
    {
        var withSecret = CheckpointKey.Compute(
            "pg", "pg:Host=db;Username=app;Password=hunter2", "SELECT 1", null, null, 1.0, null, 0, 1, 0, 0);
        var withOther = CheckpointKey.Compute(
            "pg", "pg:Host=db;Username=app;Password=different", "SELECT 1", null, null, 1.0, null, 0, 1, 0, 0);

        withSecret.Should().Be(withOther,
            "the connection is sanitised before hashing — a credential must not sit in a directory name");
    }

    [Fact]
    public void Adjacent_Parts_Cannot_Be_Confused_For_One_Another()
    {
        var a = CheckpointKey.Compute(null, "ab", "c", null, null, 1.0, null, 0, 1, 0, 0);
        var b = CheckpointKey.Compute(null, "a", "bc", null, null, 1.0, null, 0, 1, 0, 0);

        a.Should().NotBe(b, "parts are length-prefixed, so a boundary cannot be moved unnoticed");
    }

    [Fact]
    public void A_Key_Is_Usable_As_A_Directory_Name()
        => Key().Should().HaveLength(CheckpointKey.Length).And.MatchRegex("^[0-9a-f]+$");
}
