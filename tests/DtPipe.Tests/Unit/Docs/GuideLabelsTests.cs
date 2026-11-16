using System.Text.RegularExpressions;
using Xunit;

namespace DtPipe.Tests.Unit.Docs;

/// <summary>A guide that names a button or a tab of the studio or the coordinator names one the page draws.</summary>
public sealed partial class GuideLabelsTests
{
    private static readonly string[] Guides =
    [
        "docs/guides/distributed-first-pipeline.md",
        "docs/guides/agent.md",
    ];

    // Bold words that are a term or a table value, not a label of a page.
    private static readonly HashSet<string> Terms = new(StringComparer.OrdinalIgnoreCase) { "node", "bricks", "launcher", "yes" };

    private static string Root()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "DtPipe.sln"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("The repository root was not found above the test binaries.");
    }

    [GeneratedRegex(@"\*\*([^*]+)\*\*")]
    private static partial Regex Bold();

    [Fact]
    public void Every_Label_A_Guide_Names_Is_Drawn_By_A_Page()
    {
        var root = Root();
        var pages = string.Concat(new[] { "src/DtPipe/Cli/Studio/wwwroot", "src/DtPipe.Coordinator.Server/wwwroot" }
            .SelectMany(d => Directory.EnumerateFiles(Path.Combine(root, d), "*", SearchOption.AllDirectories))
            .Where(f => f.EndsWith(".js", StringComparison.Ordinal) || f.EndsWith(".html", StringComparison.Ordinal))
            .Select(File.ReadAllText));

        var missing = new List<string>();
        foreach (var guide in Guides)
        {
            var text = File.ReadAllText(Path.Combine(root, guide));
            foreach (Match m in Bold().Matches(text))
            {
                var label = m.Groups[1].Value.Trim();
                // The bold lead of a bullet is an emphasis, not a label.
                var leadsBullet = m.Index >= 2 && text.AsSpan(m.Index - 2, 2) is "- " && (m.Index == 2 || text[m.Index - 3] == '\n');
                if (leadsBullet || label.EndsWith('.') || label.EndsWith(':') || label.Contains('`') || Terms.Contains(label)) continue;
                if (!pages.Contains(label, StringComparison.OrdinalIgnoreCase)) missing.Add($"{guide}: **{label}**");
            }
        }

        Assert.True(missing.Count == 0, "A guide names a label no page draws:\n" + string.Join('\n', missing.Distinct()));
    }
}
