using DtPipe.Adapters.Generate;
using DtPipe.Cli.Pipeline;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// What a provider-option key that binds to nothing is told. It is the one mistake that leaves no
/// trace: the warning goes to the engine's stderr, never into a tool result, so a run keeps the
/// default and reports success. Naming the key that would have worked is what makes it learnable
/// in one attempt.
/// </summary>
public class UnknownProviderOptionTests
{
    /// <summary>
    /// The generator's throttle is `--throttle` on the command line and `rows-per-second` in YAML,
    /// and its own documented example used the flag. Nineteen options across the catalogue diverge
    /// the same way, so the flag is the likeliest thing a caller writes.
    /// </summary>
    [Fact]
    public void A_Key_That_Is_The_Command_Line_Flag_Is_Told_So_And_Given_The_Yaml_Key()
    {
        var message = OptionBinder.DescribeUnknownKey(typeof(GenerateReaderOptions), "throttle");

        Assert.Contains("command-line flag", message);
        Assert.Contains("rows-per-second", message);
    }

    [Fact]
    public void A_Near_Miss_Is_Answered_With_The_Key_It_Nearly_Was()
    {
        var message = OptionBinder.DescribeUnknownKey(typeof(GenerateReaderOptions), "row-counts");

        Assert.Contains("Did you mean 'row-count'", message);
    }

    /// <summary>Nothing close enough to guess at: name them all rather than close the door.</summary>
    [Fact]
    public void A_Key_Resembling_Nothing_Is_Answered_With_Every_Valid_Key()
    {
        var message = OptionBinder.DescribeUnknownKey(typeof(GenerateReaderOptions), "zzzz");

        Assert.Contains("Valid keys:", message);
        Assert.Contains("row-count", message);
        Assert.Contains("rows-per-second", message);
        Assert.Contains("arrow-batch-size", message);
    }
}
