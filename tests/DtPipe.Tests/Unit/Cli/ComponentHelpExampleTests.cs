using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Attributes;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// An example in a component's help is the only YAML most callers will copy, so every key it sets
/// must be one the loader reads. fake's set 'fake-locale' and 'fake-seed' — the command-line flag
/// names — which bound nothing and warned about nothing: the example claimed a French locale and a
/// seed while producing English, non-reproducible values.
/// </summary>
public class ComponentHelpExampleTests
{
    public static IEnumerable<object[]> ComponentsWithExamples()
        => new[] { typeof(DtPipe.Adapters.Csv.CsvReaderOptions).Assembly, typeof(DtPipe.Transformers.Arrow.Fake.FakeOptions).Assembly }
            .SelectMany(a => a.GetTypes())
            .Where(t => t.GetCustomAttribute<ComponentHelpAttribute>()?.Examples is { Length: > 0 })
            .Select(t => new object[] { t });

    [Theory]
    [MemberData(nameof(ComponentsWithExamples))]
    public void Every_Option_Key_In_An_Example_Binds(Type optionsType)
    {
        var help = optionsType.GetCustomAttribute<ComponentHelpAttribute>()!;

        foreach (var example in help.Examples!)
            foreach (var key in OptionKeys(example))
                Assert.True(OptionBinder.Binds(optionsType, key),
                    $"{optionsType.Name}'s example sets '{key}', which binds to no option of that type");
    }

    /// <summary>
    /// The keys an example sets inside an option block — <c>options:</c> for a transformer,
    /// <c>provider-options: &lt;name&gt;:</c> for an adapter. Both nest exactly one level below the
    /// block header, which is all the shape this needs to know.
    /// </summary>
    private static IEnumerable<string> OptionKeys(string example)
    {
        var lines = example.Replace("\r", "").Split('\n');
        int blockIndent = -1, keyIndent = -1;

        foreach (var line in lines)
        {
            if (line.Trim().Length == 0) continue;
            var indent = line.Length - line.TrimStart().Length;
            var trimmed = line.Trim().TrimStart('-', ' ');

            if (blockIndent >= 0 && indent <= blockIndent) { blockIndent = -1; keyIndent = -1; }

            if (trimmed is "options:" or "provider-options:") { blockIndent = indent; keyIndent = -1; continue; }
            if (blockIndent < 0) continue;

            var colon = trimmed.IndexOf(':');
            if (colon <= 0) continue;
            var name = trimmed[..colon];

            // A provider-options block names the adapter first; its options sit one level deeper.
            if (colon == trimmed.Length - 1) { keyIndent = -1; blockIndent = indent; continue; }
            if (keyIndent < 0) keyIndent = indent;
            if (indent == keyIndent) yield return name;
        }
    }
}
