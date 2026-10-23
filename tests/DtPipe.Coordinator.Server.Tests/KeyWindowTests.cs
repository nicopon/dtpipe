using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// No page offers to copy a command that holds a key (root <c>CLAUDE.md</c>). No test runs the pages, so this reads their text: the
/// Access page is the only one that asks for a key; the key its answer carries enters one element, which the Copy button copies and
/// closing the window empties; no line that shows a command or a variable names that element or the answer; and nothing of the page
/// keeps it in storage or in the address. A text check follows names, not values: it holds while the key keeps the one name it has.
/// </summary>
public sealed class KeyWindowTests
{
    private static string Pages([CallerFilePath] string here = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(here)!, "..", "..", "src", "DtPipe.Coordinator.Server", "wwwroot"));

    private static string Access() => File.ReadAllText(Path.Combine(Pages(), "js", "access.js"));

    private static string LineAt(string text, int index)
    {
        var start = text.LastIndexOf('\n', index) + 1;
        var end = text.IndexOf('\n', index);
        return text[start..(end < 0 ? text.Length : end)].Trim();
    }

    [Fact]
    public void Only_The_Access_Page_Asks_For_A_Key()
    {
        var asking = Directory.EnumerateFiles(Pages(), "*.js", SearchOption.AllDirectories)
            .Where(f => Regex.IsMatch(File.ReadAllText(f), @"/keys\b"))
            .Select(Path.GetFileName);

        Assert.Equal(["access.js"], asking);
    }

    [Fact]
    public void The_Key_Enters_One_Element_Which_The_Copy_Button_Copies_And_Closing_Empties()
    {
        var js = Access();

        var read = Assert.Single(Regex.Matches(js, @"\.key\b"));
        Assert.Equal("secret.textContent = made.key;", LineAt(js, read.Index));
        Assert.Equal("clipboard.writeText(secret.textContent)", Assert.Single(Regex.Matches(js, @"clipboard\.\w+\([^)]*\)")).Value);
        Assert.Contains("const close = () => { secret.textContent = \"\"; dialog.remove(); };", js);
    }

    [Fact]
    public void No_Command_The_Page_Shows_Names_The_Key_And_Nothing_Keeps_It()
    {
        var js = Access();

        var commands = js.Split('\n').Where(l => Regex.IsMatch(l, @"--client-secret|--key\b|client_secret|export [A-Za-z_]+=")).ToList();
        Assert.NotEmpty(commands);
        Assert.All(commands, line => Assert.DoesNotMatch(@"\bmade\b|\bsecret\.|textContent", line));
        Assert.DoesNotMatch(@"localStorage|sessionStorage|indexedDB|document\.cookie|history\.(push|replace)State|location\.(hash|search|href)\s*=", js);
    }
}
