using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using CsvHelper;
using DtPipe.Cli;
using DtPipe.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A failure shown to a person names its cause in the page's words: never the type of an exception, a reader's dump of its
/// state, a log line, a link of the chain that only says what was being done, nor a flag the person did not type. One rule
/// over every path that words a failure, held here by the failures that used to leak each part of it.
/// </summary>
public sealed partial class StudioFailureWordingTests
{
    [Theory]
    [InlineData("a source file that is not there", "CSV file not found: no-such.csv")]
    [InlineData("a query the engine refuses", "Referenced column \"nope\" not found in FROM clause!")]
    [InlineData("a reader's conversion, with its state", "The conversion cannot be performed.")]
    [InlineData("a right refused", "Access to the path '/data/out.csv' is denied.")]
    [InlineData("a job that ends where a published one may not", "in a published job a branch reads and writes its arrow:- edges only")]
    public async Task A_failure_shown_to_a_person_states_its_cause_and_nothing_of_how_it_was_found(string failure, string cause)
    {
        var shown = await ShownAsync(failure);

        Assert.Contains(cause, shown, StringComparison.Ordinal);
        Assert.DoesNotMatch(ExceptionType(), shown);
        Assert.DoesNotContain("state:", shown, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("--edges-only", shown, StringComparison.Ordinal);
        Assert.DoesNotMatch(LogLine(), shown);
        Assert.DoesNotContain("->", shown, StringComparison.Ordinal);
    }

    private static async Task<string> ShownAsync(string failure) => failure switch
    {
        // The engine's own failures, run through the path the studio's preview takes.
        "a source file that is not there" => await SampledErrorAsync("a:\n  input: csv:no-such.csv\n  output: \"null:\"\n"),
        "a query the engine refuses" => await SampledErrorAsync(
            "a:\n  input: generate:3\nb:\n  from: a\n  provider-options:\n    sql:\n      query: SELECT nope FROM a\n  output: \"null:\"\n"),
        // What a reader throws, as the run's own Fault.
        "a reader's conversion, with its state" => Faulted(ReaderConversion()),
        // What a branch reports, as the orchestrator flattens it.
        "a right refused" => new RunOutcome(1, null,
            new Dictionary<string, string> { ["b"] = "-> IOException: The output could not be opened.\n-> UnauthorizedAccessException: Access to the path '/data/out.csv' is denied." },
            new Dictionary<string, DtPipe.DryRun.SampleReport>(), new Dictionary<string, string>()).FailureText(forPerson: true)!,
        // What the runner's child writes on its console when the publication's check refuses a job.
        "a job that ends where a published one may not" => ShownFailure.OfConsole(
            "[23:08:05 ERR] Branch 'a' failed with exit code 1.\n  ✖ Branch 'a' failed with exit code 1.\n"
            + "Binding Error: 'a' reads 'generate:3': under --edges-only a branch reads and writes its arrow:- edges only.\n"),
        _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, null),
    };

    private static async Task<string> SampledErrorAsync(string yaml)
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        using var provider = services.BuildServiceProvider();
        var json = await provider.GetRequiredService<JobAnalysis>()
            .SampleAsync(yaml, rows: 5, shownRows: 5, CancellationToken.None, forPerson: true);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.False(root.GetProperty("success").GetBoolean(), json);
        return root.GetProperty("error").GetString() ?? "";
    }

    private static string Faulted(Exception fault) =>
        new RunOutcome(1, fault, new Dictionary<string, string>(), new Dictionary<string, DtPipe.DryRun.SampleReport>(), new Dictionary<string, string>())
            .FailureText(forPerson: true)!;

    // CsvHelper's own conversion failure: its message carries the text, the member and a dump of the reader's state.
    private static Exception ReaderConversion()
    {
        using var csv = new CsvReader(new StringReader("a,b\nx,2\n"), CultureInfo.InvariantCulture);
        csv.Read();
        csv.ReadHeader();
        csv.Read();
        try { csv.GetField<int>("a"); }
        catch (Exception ex) { return new InvalidOperationException("The reader failed.", ex); }
        throw new InvalidOperationException("The conversion was expected to fail.");
    }

    [GeneratedRegex(@"\b\w+(Exception|Error)\b(?!:? [a-z])")]
    private static partial Regex ExceptionType();

    [GeneratedRegex(@"\[\d{2}:\d{2}:\d{2} [A-Z]{3}\]")]
    private static partial Regex LogLine();
}
