using System.Linq;
using DtPipe.Cli.Agent.Tui;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Folding text to a panel's column. A list clips what overruns rather than folding it, so anything
/// the panels are handed unfolded loses its end off the right edge.
/// </summary>
public class TextWrapTests
{
    [Fact]
    public void Short_Lines_Are_Left_Alone()
    {
        Assert.Equal(new[] { "one", "two" }, TextWrap.Fold("one\ntwo", 20));
    }

    [Fact]
    public void A_Long_Line_Is_Folded_On_Word_Boundaries_Within_The_Width()
    {
        var folded = TextWrap.Fold("the model needs to inspect the CSV before it can plan", 20);

        Assert.All(folded, line => Assert.True(line.Length <= 20, $"'{line}' is {line.Length} wide"));
        Assert.Equal("the model needs to inspect the CSV before it can plan",
            string.Join(' ', folded.Select(l => l.Trim())));
    }

    /// <summary>
    /// A tool result is one long JSON line with no spaces in sight. There is nowhere for it to break
    /// politely, so it breaks impolitely rather than disappearing off the edge.
    /// </summary>
    [Fact]
    public void A_Word_Wider_Than_The_Column_Is_Cut()
    {
        var folded = TextWrap.Fold(new string('x', 45), 20);

        Assert.All(folded, line => Assert.True(line.Length <= 20));
        Assert.Equal(45, folded.Sum(l => l.Length));
    }

    /// <summary>An indented body stays indented, or its continuation reads as a new section.</summary>
    [Fact]
    public void A_Continuation_Keeps_The_Indent_Of_Its_Line()
    {
        var folded = TextWrap.Fold("  " + string.Join(' ', Enumerable.Repeat("word", 12)), 20);

        Assert.True(folded.Count > 1);
        Assert.All(folded, line => Assert.StartsWith("  ", line));
    }

    [Fact]
    public void An_Unusable_Width_Folds_Nothing()
    {
        Assert.Equal(new[] { "a long line that cannot be folded" },
            TextWrap.Fold("a long line that cannot be folded", 0));
    }
}
