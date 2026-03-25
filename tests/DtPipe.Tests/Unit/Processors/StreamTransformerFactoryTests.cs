using DtPipe.Processors;
using DtPipe.Processors.DataFusion;
using DtPipe.Processors.Merge;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

public class SqlTransformerFactoryTests
{
    private readonly SqlTransformerFactory _factory = new();

    [Fact]
    public void IsApplicable_WithSqlFlag_ReturnsTrue()
    {
        var args = new[] { "--from", "src", "--sql", "SELECT * FROM src" };
        Assert.True(_factory.IsApplicable(args));
    }

    [Fact]
    public void IsApplicable_WithoutSqlFlag_ReturnsFalse()
    {
        var args = new[] { "--from", "src", "--merge" };
        Assert.False(_factory.IsApplicable(args));
    }
}

public class MergeTransformerFactoryTests
{
    private readonly MergeTransformerFactory _factory = new();

    [Fact]
    public void IsApplicable_WithMergeFlag_ReturnsTrue()
    {
        var args = new[] { "--from", "a,b", "--merge" };
        Assert.True(_factory.IsApplicable(args));
    }

    [Fact]
    public void IsApplicable_WithoutMergeFlag_ReturnsFalse()
    {
        var args = new[] { "--from", "a,b", "--sql", "SELECT * FROM a" };
        Assert.False(_factory.IsApplicable(args));
    }

    [Fact]
    public void IsApplicable_WithMergeFlagCaseInsensitive_ReturnsTrue()
    {
        var args = new[] { "--from", "a,b", "--MERGE" };
        Assert.True(_factory.IsApplicable(args));
    }
}

public class BranchArgParserTests
{
    [Fact]
    public void ExtractValue_ReturnsValueAfterFlag()
    {
        var args = new[] { "--sql", "SELECT 1" };
        Assert.Equal("SELECT 1", BranchArgParser.ExtractValue(args, "--sql"));
    }

    [Fact]
    public void ExtractValue_MissingFlag_ReturnsNull()
    {
        var args = new[] { "--from", "src" };
        Assert.Null(BranchArgParser.ExtractValue(args, "--sql"));
    }

    [Fact]
    public void ExtractValue_FlagAtEnd_ReturnsNull()
    {
        var args = new[] { "--from", "src", "--sql" };
        Assert.Null(BranchArgParser.ExtractValue(args, "--sql"));
    }

    [Fact]
    public void ExtractAllValues_MultipleOccurrences_ReturnsAll()
    {
        var args = new[] { "--ref", "a", "--ref", "b", "--ref", "c" };
        var values = BranchArgParser.ExtractAllValues(args, "--ref").ToList();
        Assert.Equal(["a", "b", "c"], values);
    }

    [Fact]
    public void ExtractAllValues_NoOccurrence_ReturnsEmpty()
    {
        var args = new[] { "--from", "src", "--sql", "SELECT 1" };
        Assert.Empty(BranchArgParser.ExtractAllValues(args, "--ref"));
    }
}
