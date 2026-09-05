using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E5: the full-screen session's input line replaces the post-mission
/// selection prompt, so what the user types is now the whole menu. Parsed purely, and asserted
/// from strings without a terminal.
/// </summary>
public class SessionCommandTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Blank_Input_Asks_For_Nothing(string? line)
        => Assert.Equal(SessionCommandKind.Empty, SessionCommand.Parse(line).Kind);

    [Fact]
    public void Ordinary_Text_Is_The_Next_Turn_S_Prompt()
    {
        var command = SessionCommand.Parse("  anonymize the email column  ");

        Assert.Equal(SessionCommandKind.Prompt, command.Kind);
        Assert.Equal("anonymize the email column", command.Text);
    }

    [Theory]
    [InlineData("/exec", SessionCommandKind.Execute)]
    [InlineData("/execute", SessionCommandKind.Execute)]
    [InlineData("/run", SessionCommandKind.Execute)]
    [InlineData("/mode", SessionCommandKind.Mode)]
    [InlineData("/save", SessionCommandKind.Save)]
    [InlineData("/review", SessionCommandKind.Review)]
    [InlineData("/steps", SessionCommandKind.Review)]
    [InlineData("/quit", SessionCommandKind.Quit)]
    [InlineData("/exit", SessionCommandKind.Quit)]
    [InlineData("/q", SessionCommandKind.Quit)]
    [InlineData("/help", SessionCommandKind.Help)]
    public void The_Commands_Are_Recognised(string line, SessionCommandKind expected)
        => Assert.Equal(expected, SessionCommand.Parse(line).Kind);

    [Fact]
    public void A_Command_Is_Case_Insensitive_And_Keeps_Its_Argument()
    {
        var command = SessionCommand.Parse("/SAVE  plans/invoices.yaml ");

        Assert.Equal(SessionCommandKind.Save, command.Kind);
        Assert.Equal("plans/invoices.yaml", command.Text);
    }

    [Fact]
    public void An_Unknown_Slash_Word_Is_Never_Sent_To_The_Model()
    {
        // A typo must not spend a model call. It is reported, not forwarded.
        var command = SessionCommand.Parse("/exce now");

        Assert.Equal(SessionCommandKind.Unknown, command.Kind);
        Assert.Equal("exce", command.Text);
    }

    [Fact]
    public void A_Command_Without_An_Argument_Carries_An_Empty_One()
        => Assert.Equal(string.Empty, SessionCommand.Parse("/save").Text);
}
