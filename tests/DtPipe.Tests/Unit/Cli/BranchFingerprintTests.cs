using AwesomeAssertions;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Services;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The fingerprint is what a checkpoint key hashes beyond the reader: it must tell two definitions
/// apart, ignore the labels, and carry no credential.
/// </summary>
public class BranchFingerprintTests
{
    private static readonly IReadOnlyList<(string, string)> NoUpstream = [];

    private static string Of(JobDefinition job, CliJobContext? context = null)
        => BranchFingerprint.Of(job, context, NoUpstream, namesInputs: false);

    [Fact]
    public void A_Secret_In_The_Source_Never_Enters_The_Fingerprint()
    {
        // 'Zork' is no key Sanitize recognises as sensitive: only Redact, which masks every key
        // off its safe list, keeps this value out.
        var one = Of(new JobDefinition { Input = "pg:Host=db;Zork=hunter2" });
        var other = Of(new JobDefinition { Input = "pg:Host=db;Zork=different" });

        one.Should().Be(other);
    }

    [Fact]
    public void The_Source_Argument_Is_Not_Read_Twice()
    {
        var job = new JobDefinition { Input = "pg:Host=db;Zork=hunter2" };
        string[] args(string secret) => ["-i", $"pg:Host=db;Zork={secret}", "--query", "SELECT 1"];

        Of(job, new CliJobContext(args("hunter2"), [], null, null))
            .Should().Be(Of(job, new CliJobContext(args("different"), [], null, null)));
    }

    [Fact]
    public void A_Transformer_Configuration_Is_Part_Of_The_Definition()
    {
        static JobDefinition Job(string expression) => new()
        {
            Input = "generate:10",
            Transformers = [new TransformerConfig { Type = "filter", Mappings = new() { [expression] = "" } }],
        };

        Of(Job("row.X < 10")).Should().NotBe(Of(Job("row.X < 50")));
        Of(Job("row.X < 10")).Should().Be(Of(Job("row.X < 10")));
    }

    [Fact]
    public void A_Label_Is_Not_Part_Of_The_Definition()
    {
        var job = new JobDefinition { Input = "generate:10" };

        Of(job with { Checkpoint = "one", Output = "csv:a.csv" })
            .Should().Be(Of(job with { Checkpoint = "two", Output = "csv:b.csv" }));
        Of(job, new CliJobContext(["--alias", "src"], [], null, null))
            .Should().Be(Of(job, new CliJobContext(["--alias", "origin"], [], null, null)));
    }

    [Fact]
    public void A_Processor_Names_Its_Inputs_And_A_Plain_Branch_Does_Not()
    {
        var job = new JobDefinition();
        var swapped = new (string, string)[] { ("b", "fingerprint-x"), ("a", "fingerprint-y") };
        var original = new (string, string)[] { ("a", "fingerprint-x"), ("b", "fingerprint-y") };

        BranchFingerprint.Of(job, null, original, namesInputs: true)
            .Should().NotBe(BranchFingerprint.Of(job, null, swapped, namesInputs: true),
                "the query reads a and b by name, so swapping what they point at changes the rows");
        BranchFingerprint.Of(job, null, [("src", "fingerprint-x")], namesInputs: false)
            .Should().Be(BranchFingerprint.Of(job, null, [("origin", "fingerprint-x")], namesInputs: false));
    }

    [Fact]
    public void A_Script_File_Is_Read_Into_The_Definition()
    {
        var path = Path.Combine(Path.GetTempPath(), $"fp_{Guid.NewGuid():N}.js");
        try
        {
            var job = new JobDefinition
            {
                Transformers = [new TransformerConfig { Type = "compute", Mappings = new() { ["x"] = "@" + path } }],
            };
            File.WriteAllText(path, "row.A * 2");
            var before = Of(job);
            File.WriteAllText(path, "row.A * 3");

            Of(job).Should().NotBe(before, "a script that changed is a transformer that changed");
        }
        finally { File.Delete(path); }
    }
}
