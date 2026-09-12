using DtPipe.Cli.Incremental;
using DtPipe.Configuration;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A job carrying 'cursor:' and no interpolation tracks and filters nothing: measured at 50 rows
/// on the first run and 100 on the second, over the same unchanged source.
/// </summary>
public class CursorAdvisoryTests
{
    private const string Tracking =
        "main:\n  input: \"csv:orders.csv\"\n  output: \"sqlite:Data Source=out.db\"\n" +
        "  cursor: order_id\n  state: orders.state.json\n";

    [Fact]
    public void A_Cursor_With_Nothing_To_Filter_On_Is_Reported()
    {
        var advisories = CursorAdvisory.Advise(JobFileParser.ParseContent(Tracking), Tracking);

        var only = Assert.Single(advisories);
        Assert.Contains("main", only);
        Assert.Contains("order_id", only);
        Assert.Contains("cursor://", only);
    }

    [Fact]
    public void A_Cursor_Used_In_The_Query_Is_Not_Reported()
    {
        const string filtering =
            "main:\n  input: \"sqlite:Data Source=src.db\"\n  output: \"sqlite:Data Source=out.db\"\n" +
            "  cursor: order_id\n  state: orders.state.json\n  provider-options:\n    sqlite-reader:\n" +
            "      query: \"SELECT * FROM orders WHERE order_id > ${{cursor://orders.state.json|0}}\"\n";

        Assert.Empty(CursorAdvisory.Advise(JobFileParser.ParseContent(filtering), filtering));
    }

    /// <summary>
    /// The filter may live in a file this text cannot read. Reporting then would warn about a
    /// pipeline that works, which costs more than the warning saves.
    /// </summary>
    [Fact]
    public void A_Value_Loaded_From_A_File_Silences_The_Advisory()
    {
        // Parsed from the tracking job so the branches carry a cursor; the text is what the rule
        // reads, and it is the '@' that must silence it — the file's content is out of reach here
        // exactly as it is in production.
        const string fromFile =
            "main:\n  input: \"sqlite:Data Source=src.db\"\n  output: \"sqlite:Data Source=out.db\"\n" +
            "  cursor: order_id\n  state: orders.state.json\n  provider-options:\n    sqlite-reader:\n" +
            "      query: \"@incremental.sql\"\n";

        Assert.Empty(CursorAdvisory.Advise(JobFileParser.ParseContent(Tracking), fromFile));
    }

    [Fact]
    public void A_Job_Without_A_Cursor_Is_Not_Reported()
    {
        const string plain =
            "main:\n  input: \"csv:orders.csv\"\n  output: \"sqlite:Data Source=out.db\"\n";

        Assert.Empty(CursorAdvisory.Advise(JobFileParser.ParseContent(plain), plain));
    }
}
