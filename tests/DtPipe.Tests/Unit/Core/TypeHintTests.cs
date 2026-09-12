using System;
using DtPipe.Core.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// One vocabulary for every type hint.
///
/// Four copies of the resolver existed — <see cref="TypeHelper"/> and one each in the CSV, JSONL
/// and XML readers — and they had drifted: 'str' resolved on CSV and XML but not on JSONL, 'text'
/// only on JSONL, 'date' and 'timestamp' on neither, and the copy here knew eight bare names with
/// no alias at all. Measured on the binary before the merge: '--compute-types "Col:int32"' wrote a
/// String column while '--column-types "Col:int32"' wrote an Int32 one.
/// </summary>
public class TypeHintTests
{
    [Theory]
    [InlineData("string", typeof(string))]
    [InlineData("str", typeof(string))]
    [InlineData("text", typeof(string))]
    [InlineData("int", typeof(int))]
    [InlineData("int32", typeof(int))]
    [InlineData("integer", typeof(int))]
    [InlineData("long", typeof(long))]
    [InlineData("int64", typeof(long))]
    [InlineData("double", typeof(double))]
    [InlineData("float64", typeof(double))]
    [InlineData("number", typeof(double))]
    [InlineData("float", typeof(float))]
    [InlineData("float32", typeof(float))]
    [InlineData("single", typeof(float))]
    [InlineData("decimal", typeof(decimal))]
    [InlineData("numeric", typeof(decimal))]
    [InlineData("money", typeof(decimal))]
    [InlineData("bool", typeof(bool))]
    [InlineData("boolean", typeof(bool))]
    [InlineData("datetime", typeof(DateTime))]
    [InlineData("date", typeof(DateTime))]
    [InlineData("datetimeoffset", typeof(DateTimeOffset))]
    [InlineData("timestamp", typeof(DateTimeOffset))]
    [InlineData("datetimetz", typeof(DateTimeOffset))]
    [InlineData("guid", typeof(Guid))]
    [InlineData("uuid", typeof(Guid))]
    public void Every_Spelling_Resolves(string hint, Type expected)
    {
        Assert.Equal(expected, TypeHelper.ParseTypeHint(hint));
        Assert.Equal(expected, TypeHelper.RequireTypeHint("--column-types", "Col", hint));
    }

    [Theory]
    [InlineData("INT32")]
    [InlineData("  int32  ")]
    public void A_Spelling_Is_Case_And_Space_Insensitive(string hint)
        => Assert.Equal(typeof(int), TypeHelper.ParseTypeHint(hint));

    /// <summary>
    /// Null is an answer, not a failure: '--compute "Col:row.a ? 1 : 2"' splits into three parts
    /// whose middle is not a type, and that is how the two-part form is told from the typed one.
    /// </summary>
    [Fact]
    public void An_Unknown_Spelling_Resolves_To_Null_Rather_Than_Throwing()
        => Assert.Null(TypeHelper.ParseTypeHint("row.a ? 1 "));

    [Fact]
    public void A_Required_Hint_That_Names_No_Type_Is_Refused_And_Lists_The_Spellings()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => TypeHelper.RequireTypeHint("--compute-types", "Qty", "in32"));

        Assert.Contains("--compute-types", ex.Message);
        Assert.Contains("'Qty'", ex.Message);
        Assert.Contains("'in32'", ex.Message);
        Assert.Contains("int32", ex.Message);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void A_Required_Hint_That_Is_Empty_Is_Refused(string? hint)
        => Assert.Throws<InvalidOperationException>(
            () => TypeHelper.RequireTypeHint("--column-types", "Qty", hint));
}
